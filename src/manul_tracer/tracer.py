"""Main ManulTracer class for OpenAI API call tracing."""

from typing import Optional, Dict, Any
import httpx
import uuid
import logging
from datetime import datetime

from .transport import TracedTransport
from .database.repositories import TraceRepository
from .database.repositories.session_repository import SessionRepository
from .models import Session

# Set up session logger
session_logger = logging.getLogger('manul_tracer.session')
session_logger.setLevel(logging.INFO)

# Add console handler to session logger
import sys
session_console_handler = logging.StreamHandler(sys.stdout)
session_console_handler.setLevel(logging.INFO)
session_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
session_console_handler.setFormatter(session_formatter)
session_logger.addHandler(session_console_handler)


class ManulTracer:
    """Main tracer class for intercepting and storing OpenAI API calls.
    
    Usage:
        tracer = ManulTracer()
        client = openai.OpenAI(api_key=api_key, http_client=tracer.http_client)
    """
    
    def __init__(
        self,
        session_id: Optional[str] = None,
        database_file: Optional[str] = None,
        auto_save: bool = True,
        **httpx_kwargs
    ):
        """Initialize the ManulTracer.
        
        Args:
            session_id: Optional session identifier. If None, generates a UUID
            database_file: Optional database file path. If None, uses in-memory database
            auto_save: Whether to automatically save traces to database (default True)
            **httpx_kwargs: Additional arguments passed to httpx.Client
        """
        self.auto_save = auto_save
        
        # Initialize repositories if auto_save is enabled
        if auto_save:
            try:
                self.repository = TraceRepository(database_file)
                self.session_repository = SessionRepository(database_file)
            except Exception as e:
                session_logger.warning(f"Failed to initialize repository: {e}. Auto-save disabled.")
                self.repository = None
                self.session_repository = None
                self.auto_save = False
        else:
            self.repository = None
            self.session_repository = None
        
        # Generate session_id if not provided
        if session_id is None:
            session_id = str(uuid.uuid4())
            session_logger.info(f"Generated new session ID: {session_id}")
        else:
            session_logger.info(f"Using provided session ID: {session_id}")
        
        self.session_id = session_id
        
        # Message ID mapping for session-aware message identity
        # Format: {f"{role}_{position}": message_id}
        self.message_id_mapping: Dict[str, str] = {}
        
        # Create session object
        self.session = Session(
            session_id=session_id,
            session_type="tracer",
            created_at=None  # Will be set when first request is made
        )
        
        session_logger.info(f"Created ManulTracer session: {session_id} (type: tracer)")
        
        # Separate transport-specific kwargs from client-specific kwargs
        transport_params = [
            'verify', 'cert', 'http1', 'http2', 'limits', 'proxy', 
            'uds', 'local_address', 'retries', 'socket_options', 'trust_env'
        ]
        
        transport_kwargs = {}
        client_kwargs = {}
        
        for key, value in httpx_kwargs.items():
            if key in transport_params:
                transport_kwargs[key] = value
            else:
                client_kwargs[key] = value
        
        base_transport = httpx.HTTPTransport(**transport_kwargs)
        
        self._transport = TracedTransport(
            wrapped_transport=base_transport,
            repository=self.repository,
            session_id=session_id,
            tracer=self
        )
        
        self._http_client = httpx.Client(
            transport=self._transport,
            **client_kwargs
        )
    
    @property
    def http_client(self) -> httpx.Client:
        """Get the traced HTTP client for use with OpenAI.
        
        Returns:
            httpx.Client configured with tracing transport
        """
        return self._http_client
    
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregated statistics from the current session.
        
        Returns:
            Dictionary with statistics like total requests, tokens, etc.
        """
        return self._transport.stats.copy()
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get information about the current session.
        
        Returns:
            Dictionary with session information including ID, timestamps, and stats
        """
        stats = self.get_stats()
        session_dict = self.session.to_dict()
        
        return {
            "session_id": self.session_id,
            "session_type": session_dict["session_type"],
            "created_at": session_dict["created_at"],
            "last_activity_at": session_dict["last_activity_at"],
            "total_requests": stats["total_requests"],
            "total_tokens": stats["total_tokens"],
            "successful_requests": stats["successful_requests"],
            "failed_requests": stats["failed_requests"]
        }
    
    def get_or_assign_message_id(self, role: str, position: int) -> str:
        """Get or assign a stable message ID for a message based on role and position.
        
        Args:
            role: Message role (user, assistant, system, tool)
            position: Position in the conversation (0-based index)
            
        Returns:
            Stable message ID for this role+position combination
        """
        message_key = f"{role}_{position}"
        
        if message_key not in self.message_id_mapping:
            # Generate new ID for this message
            new_id = str(uuid.uuid4())
            self.message_id_mapping[message_key] = new_id
            session_logger.debug(f"Assigned new message ID {new_id} for {message_key}")
        
        return self.message_id_mapping[message_key]
    
    def _initialize_session_if_needed(self):
        """Initialize session timestamps if this is the first request."""
        now = datetime.now()
        if self.session.created_at is None:
            self.session.created_at = now
            session_logger.info(f"Session {self.session_id} started at {now.isoformat()}")
            
            # Save session to database if auto_save is enabled
            if self.auto_save and self.session_repository:
                try:
                    self.session_repository.create_or_update(self.session)
                    session_logger.debug(f"Saved session {self.session_id} to database")
                except Exception as e:
                    session_logger.warning(f"Failed to save session {self.session_id}: {e}")
        
        self.session.last_activity_at = now
        
        # Update session activity in database
        if self.auto_save and self.session_repository:
            try:
                self.session_repository.update_activity(self.session_id)
            except Exception as e:
                session_logger.warning(f"Failed to update session activity: {e}")
        
        session_logger.debug(f"Session {self.session_id} activity updated at {now.isoformat()}")
    
    def _on_trace_completed(self, trace):
        """Called when TracedTransport completes a trace.
        
        Args:
            trace: TraceRecord instance that has been completed
        """
        if not self.auto_save or not self.repository:
            return
            
        try:
            # Save the trace using create_or_update for safety
            self.repository.create_or_update(trace)
            session_logger.debug(f"Saved trace {trace.trace_id} to database")
            
            # Update session statistics
            if self.session_repository:
                tokens = trace.total_tokens or 0
                # Calculate cost if needed (placeholder for now)
                cost = 0.0
                self.session_repository.update_statistics(
                    self.session_id, 
                    requests=1, 
                    tokens=tokens, 
                    cost=cost
                )
                
        except Exception as e:
            # Log error but don't crash tracing
            session_logger.warning(f"Failed to save trace {trace.trace_id}: {e}")
    
    def reset_stats(self):
        """Reset the statistics counters."""
        session_logger.info(f"Resetting stats for session {self.session_id}")
        self._transport.stats = {
            'total_requests': 0,
            'total_prompt_tokens': 0,
            'total_completion_tokens': 0,
            'total_tokens': 0,
            'successful_requests': 0,
            'failed_requests': 0
        }
    
    def close(self):
        """Close the HTTP client and clean up resources."""
        session_logger.info(f"Closing session {self.session_id}")
        if self.session.created_at:
            duration = datetime.now() - self.session.created_at
            session_logger.info(f"Session {self.session_id} duration: {duration}")
            
            # End the session in the database
            if self.auto_save and self.session_repository:
                try:
                    self.session_repository.end_session(self.session_id)
                    session_logger.debug(f"Ended session {self.session_id} in database")
                except Exception as e:
                    session_logger.warning(f"Failed to end session {self.session_id}: {e}")
        
        self._http_client.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

