"""Transport layer implementation for intercepting HTTP requests."""
import logging
import sys
from datetime import datetime
import httpx
import json
import hashlib
import uuid
from typing import Dict, Any

from .models import TraceRecord, Message

# Set up logger for the transport
logger = logging.getLogger('manul_tracer.transport')
logger.setLevel(logging.INFO)

# Add console handler to ensure logs are output to stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
from .parsers import (
    parse_openai_request,
    calculate_performance_metrics,
    categorize_error,
    is_streaming_request,
    extract_conversation_messages
)


class LogResponse(httpx.Response):
    """Custom Response class that captures content during normal consumption."""
    
    def __init__(self, *args, **kwargs):
        # Extract trace_record and transport from kwargs before calling super
        self.trace_record = kwargs.pop('trace_record', None)
        self.traced_transport = kwargs.pop('traced_transport', None)
        super().__init__(*args, **kwargs)
        self._captured_content = b''
        self._content_captured = False

    def iter_bytes(self, chunk_size: int = 1024):
        """Override iter_bytes to capture chunks as they're consumed"""
        for chunk in super().iter_bytes(chunk_size):
            self._captured_content += chunk
            yield chunk
        # Mark content as fully captured and complete trace
        if not self._content_captured:
            self._content_captured = True
            self._complete_trace()

    def read(self, *args, **kwargs):
        """Override to capture non-streaming reads."""
        content = super().read(*args, **kwargs)
        if content and not self._captured_content:
            self._captured_content = content
            self._content_captured = True
            self._complete_trace()
        return content

    def _complete_trace(self):
        """Complete trace parsing after content is captured."""
        if self.trace_record and self.traced_transport and self._captured_content:
            try:
                # Update trace record from response
                self.trace_record.from_successful_response(
                    self._captured_content,
                    self.headers,
                    self.status_code
                )
                
                # Update statistics
                self.traced_transport.stats['total_prompt_tokens'] += self.trace_record.prompt_tokens
                self.traced_transport.stats['total_completion_tokens'] += self.trace_record.completion_tokens
                self.traced_transport.stats['total_tokens'] += self.trace_record.total_tokens
                
                # Notify ManulTracer of trace completion
                if self.traced_transport.tracer:
                    self.traced_transport.tracer._on_trace_completed(self.trace_record)
                
                # Log the complete trace object
                logger.info("="*80)
                logger.info("ENHANCED TRACE RECORD:")
                logger.info("="*80)
                logger.info(self.trace_record.to_json())
                logger.info("="*80)
                
            except Exception as e:
                logger.error(f"Error completing trace: {e}")

    @property
    def content(self):
        """Property to get response content, using captured content if available."""
        if self._captured_content:
            return self._captured_content
        return super().content

    @property
    def text(self):
        """Property to get response text."""
        content = self.content
        if isinstance(content, bytes):
            return content.decode('utf-8', errors='replace')
        return str(content)

    def json(self, **kwargs):
        """Parse response as JSON."""
        text = self.text
        if not text:
            return None
        return json.loads(text)


class TracedTransport(httpx.BaseTransport):
    """Transport that intercepts and traces all HTTP requests."""
    
    def __init__(self, wrapped_transport=None, repository=None, session_id=None, tracer=None):
        self.wrapped_transport = wrapped_transport or httpx.HTTPTransport()
        self.stats = {
            'total_requests': 0,
            'total_prompt_tokens': 0,
            'total_completion_tokens': 0,
            'total_tokens': 0,
            'successful_requests': 0,
            'failed_requests': 0
        }
        self.repository = repository
        self.session_id = session_id or "default"
        self.tracer = tracer
        
        # Message deduplication cache
        # Format: {message_key: message_id}
        self.message_cache: Dict[str, str] = {}

    def _get_or_assign_message_id(self, role: str, content: str, position: int) -> str:
        """Get or assign a stable message ID using role, content, and position.
        
        Args:
            role: Message role (user, assistant, system, tool)
            content: Message content 
            position: Position in conversation (0-based)
            
        Returns:
            Stable message ID
        """
        # Create message key using role, position, and content hash
        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()[:8]
        message_key = f"{role}_{position}_{content_hash}"
        
        if message_key not in self.message_cache:
            # Generate new UUID for this message
            new_id = str(uuid.uuid4())
            self.message_cache[message_key] = new_id
            logger.debug(f"Assigned new message ID {new_id} for key {message_key}")
        
        return self.message_cache[message_key]

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Intercept and trace HTTP requests."""
        self.stats['total_requests'] += 1
        start_time = datetime.now()
        
        # Initialize session timestamps if this is first request
        if self.tracer:
            self.tracer._initialize_session_if_needed()
            logger.info(f"Processing request #{self.stats['total_requests']} for session {self.session_id}")
        
        # Create initial trace record
        trace = TraceRecord(
            session_id=self.session_id,
            trace_id=f"trace_{self.stats['total_requests']}",
            request_timestamp=start_time,
            provider="openai"
        )
        
        # Parse request
        request_dict = parse_openai_request(request)
        trace.model = request_dict.get('model')
        trace.endpoint = str(request.url)
        trace.stream = is_streaming_request(request_dict)
        
        # Extract messages and assign stable IDs
        messages_data = extract_conversation_messages(request_dict)
        messages_with_ids = []
        
        for position, msg_data in enumerate(messages_data):
            # Get or assign stable message ID
            message_id = self._get_or_assign_message_id(
                role=msg_data['role'],
                content=msg_data['content'] or '',
                position=position
            )
            
            # Add message_id to the message data
            msg_data['message_id'] = message_id
            messages_with_ids.append(msg_data)
        
        trace.full_conversation = [Message(**msg) for msg in messages_with_ids]
        
        # Extract API parameters
        trace.temperature = request_dict.get('temperature')
        trace.max_tokens = request_dict.get('max_tokens')
        trace.top_p = request_dict.get('top_p')
        trace.frequency_penalty = request_dict.get('frequency_penalty')
        trace.presence_penalty = request_dict.get('presence_penalty')
        trace.seed = request_dict.get('seed')
        trace.stop_sequences = request_dict.get('stop', [])
        trace.logit_bias = request_dict.get('logit_bias')
        
        # Check if this is an OpenAI API endpoint that we should trace
        should_trace = 'openai.com' in str(request.url) or 'api.openai.com' in str(request.url)
        
        try:
            # Delegate to wrapped transport
            original_response = self.wrapped_transport.handle_request(request)
            trace.response_timestamp = datetime.now()
            
            if original_response.status_code >= 400:
                # Handle error responses
                self.stats['failed_requests'] += 1
                
                # Read error content immediately since we won't wrap error responses
                try:
                    error_content = original_response.read()
                    error_body = error_content.decode('utf-8') if isinstance(error_content, bytes) else str(error_content)
                except Exception:
                    error_body = "Unable to read error response"
                
                trace.error_message = error_body
                trace.error_category = categorize_error(original_response.status_code, error_body)
                trace.success = False
                
                # Calculate performance metrics and save error trace
                end_time = datetime.now()
                metrics = calculate_performance_metrics(start_time, end_time, 0, 0)
                trace.total_latency_ms = metrics['latency_ms']
                trace.update_completeness()
                
                # Notify ManulTracer of trace completion (error case)
                if self.tracer:
                    self.tracer._on_trace_completed(trace)
                
                return original_response
            
            elif should_trace:
                # Successful response that needs tracing - wrap with LogResponse
                self.stats['successful_requests'] += 1
                
                # Create LogResponse with trace context for deferred completion
                response = LogResponse(
                    status_code=original_response.status_code,
                    headers=original_response.headers,
                    stream=original_response.stream,
                    extensions=original_response.extensions,
                    trace_record=trace,
                    traced_transport=self
                )
                
                # Log initial trace (will be enhanced after content capture)
                logger.info("="*80)
                logger.info("INITIAL TRACE RECORD:")
                logger.info("="*80)
                logger.info(trace.to_json())
                logger.info("="*80)
                
                return response
            
            else:
                # Non-OpenAI response - pass through without tracing
                self.stats['successful_requests'] += 1
                
                # Calculate basic performance metrics for non-traced responses
                end_time = datetime.now()
                metrics = calculate_performance_metrics(start_time, end_time, 0, 0)
                trace.total_latency_ms = metrics['latency_ms']
                trace.update_completeness()
                
                # Notify ManulTracer of trace completion (non-OpenAI case)
                if self.tracer:
                    self.tracer._on_trace_completed(trace)
                
                return original_response
                
        except Exception as e:
            self.stats['failed_requests'] += 1
            trace.error_message = str(e)
            trace.error_category = type(e).__name__
            trace.success = False
            
            # Calculate performance metrics for failed requests
            end_time = datetime.now()
            metrics = calculate_performance_metrics(start_time, end_time, 0, 0)
            trace.total_latency_ms = metrics['latency_ms']
            trace.update_completeness()
            
            # Notify ManulTracer of trace completion (exception case)
            if self.tracer:
                self.tracer._on_trace_completed(trace)
            
            # Log error trace
            logger.error("="*80)
            logger.error("ERROR TRACE RECORD:")
            logger.error("="*80)
            logger.error(trace.to_json())
            logger.error("="*80)
            
            raise
    
    def close(self):
        """Close the wrapped transport."""
        if hasattr(self.wrapped_transport, 'close'):
            self.wrapped_transport.close()