"""Main ManulTracer class for OpenAI API call tracing."""

from typing import Optional, Dict, Any
import httpx
import uuid
import logging
from datetime import datetime

from .transport import TracedTransport
from .database.repositories import TraceRepository
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
        repository: Optional[TraceRepository] = None,
        session_id: Optional[str] = None,
        **httpx_kwargs
    ):
        """Initialize the ManulTracer.
        
        Args:
            repository: Optional TraceRepository for persisting traces
            session_id: Optional session identifier. If None, generates a UUID
            **httpx_kwargs: Additional arguments passed to httpx.Client
        """
        self.repository = repository
        
        # Generate session_id if not provided
        if session_id is None:
            session_id = str(uuid.uuid4())
            session_logger.info(f"Generated new session ID: {session_id}")
        else:
            session_logger.info(f"Using provided session ID: {session_id}")
        
        self.session_id = session_id
        
        # Create session object
        self.session = Session(
            session_id=session_id,
            session_type="tracer",
            session_created_at=None  # Will be set when first request is made
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
            repository=repository,
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
            "session_created_at": session_dict["session_created_at"],
            "last_activity": session_dict["last_activity"],
            "total_requests": stats["total_requests"],
            "total_tokens": stats["total_tokens"],
            "successful_requests": stats["successful_requests"],
            "failed_requests": stats["failed_requests"]
        }
    
    def _initialize_session_if_needed(self):
        """Initialize session timestamps if this is the first request."""
        now = datetime.now()
        if self.session.session_created_at is None:
            self.session.session_created_at = now
            session_logger.info(f"Session {self.session_id} started at {now.isoformat()}")
        
        self.session.last_activity = now
        session_logger.debug(f"Session {self.session_id} activity updated at {now.isoformat()}")
    
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
        if self.session.session_created_at:
            duration = datetime.now() - self.session.session_created_at
            session_logger.info(f"Session {self.session_id} duration: {duration}")
        self._http_client.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

