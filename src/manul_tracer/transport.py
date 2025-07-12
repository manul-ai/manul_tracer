"""Transport layer implementation for intercepting HTTP requests."""
import logging
import sys
from datetime import datetime
import httpx
import json

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
    parse_openai_response,
    calculate_performance_metrics,
    populate_assistant_message_tokens,
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
                # Create a mock response with captured content for parsing
                mock_response = type('MockResponse', (), {
                    'content': self._captured_content,
                    'headers': self.headers,
                    'status_code': self.status_code
                })()
                
                # Parse the captured response
                response_data = parse_openai_response(mock_response, self.trace_record.stream)
                
                # Update trace with response data
                self.trace_record.prompt_tokens = response_data.get('prompt_tokens', 0)
                self.trace_record.completion_tokens = response_data.get('completion_tokens', 0)
                self.trace_record.total_tokens = response_data.get('total_tokens', 0)
                self.trace_record.finish_reason = response_data.get('finish_reason')
                self.trace_record.assistant_response = response_data.get('content')
                
                # Detailed token breakdowns
                self.trace_record.prompt_cached_tokens = response_data.get('prompt_cached_tokens')
                self.trace_record.prompt_audio_tokens = response_data.get('prompt_audio_tokens')
                self.trace_record.completion_reasoning_tokens = response_data.get('completion_reasoning_tokens')
                self.trace_record.completion_audio_tokens = response_data.get('completion_audio_tokens')
                self.trace_record.completion_accepted_prediction_tokens = response_data.get('completion_accepted_prediction_tokens')
                self.trace_record.completion_rejected_prediction_tokens = response_data.get('completion_rejected_prediction_tokens')
                
                # Rate limit info
                if response_data.get('rate_limit_requests_remaining'):
                    self.trace_record.rate_limit_remaining = response_data.get('rate_limit_requests_remaining')
                
                # Update statistics
                self.traced_transport.stats['total_prompt_tokens'] += self.trace_record.prompt_tokens
                self.traced_transport.stats['total_completion_tokens'] += self.trace_record.completion_tokens
                self.traced_transport.stats['total_tokens'] += self.trace_record.total_tokens
                
                # Add assistant message if we have response content
                if response_data.get('assistant_content'):
                    assistant_message = Message(
                        role="assistant",
                        content=response_data['assistant_content']
                    )
                    self.trace_record.full_conversation.append(assistant_message)
                
                # Populate token counts for messages
                self.trace_record.full_conversation = populate_assistant_message_tokens(
                    self.trace_record.full_conversation, 
                    self.trace_record.completion_tokens
                )
                
                # Calculate data completeness
                self.trace_record.update_completeness()
                
                # Save to repository if available
                if self.traced_transport.repository:
                    self.traced_transport.repository.create(self.trace_record)
                
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
    
    def __init__(self, wrapped_transport=None, repository=None):
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

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Intercept and trace HTTP requests."""
        self.stats['total_requests'] += 1
        start_time = datetime.now()
        
        # Create initial trace record
        trace = TraceRecord(
            session_id="default",
            trace_id=f"trace_{self.stats['total_requests']}",
            request_timestamp=start_time,
            provider="openai"
        )
        
        # Parse request
        request_dict = parse_openai_request(request)
        trace.model = request_dict.get('model')
        trace.endpoint = str(request.url)
        trace.stream = is_streaming_request(request_dict)
        
        # Extract messages
        messages_data = extract_conversation_messages(request_dict)
        trace.full_conversation = [Message(**msg) for msg in messages_data]
        
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
                
                if self.repository:
                    self.repository.create(trace)
                
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
                
                if self.repository:
                    self.repository.create(trace)
                
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
            
            # Save error trace to repository if available
            if self.repository:
                self.repository.create(trace)
            
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