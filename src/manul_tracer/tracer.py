import httpx
import json
import logging
import time
from datetime import datetime

from .models import TraceRecord
from .utils import (
    parse_openai_request, parse_openai_response, calculate_performance_metrics,
    categorize_error, populate_assistant_message_tokens
)


# Configure logging for trace output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TRACE - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

trace_logger = logging.getLogger('manul_tracer')
trace_logger.setLevel(logging.DEBUG)  # Ensure debug traces are shown


class LogResponse(httpx.Response):
    """Custom Response class that logs streaming content during consumption"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._captured_content = b""
        self._trace_record = None
    
    def set_trace_record(self, trace_record):
        """Set the trace record to update with streaming data"""
        self._trace_record = trace_record
    
    def iter_bytes(self, chunk_size: int = 1024):
        """Override iter_bytes to log chunks as they're consumed"""
        trace_logger.info("  Starting to read streaming response...")
        chunk_count = 0
        total_size = 0
        
        for chunk in super().iter_bytes(chunk_size):
            chunk_count += 1
            total_size += len(chunk)
            self._captured_content += chunk
            trace_logger.info(f"  Received chunk {chunk_count}, total size: {total_size} bytes. Chunk content: {chunk}")
            yield chunk
        
        trace_logger.info(f"  Streaming complete: {chunk_count} chunks, {total_size} total bytes")
        
        # Parse captured content and update trace record
        if self._trace_record and self._captured_content:
            self._parse_and_update_trace()
    
    def _parse_and_update_trace(self):
        """Parse captured streaming content and update trace record"""
        try:
            from .utils import parse_openai_response, calculate_performance_metrics, populate_assistant_message_tokens
            
            # Parse the captured streaming content
            response_data = parse_openai_response(self, self._captured_content)
            
            # Update trace record with response data
            for key, value in response_data.items():
                if hasattr(self._trace_record, key):
                    setattr(self._trace_record, key, value)
            
            # Populate assistant message token counts
            if (hasattr(self._trace_record, 'completion_tokens') and 
                self._trace_record.completion_tokens and 
                hasattr(self._trace_record, 'full_conversation') and 
                self._trace_record.full_conversation):
                self._trace_record.full_conversation = populate_assistant_message_tokens(
                    self._trace_record.full_conversation, self._trace_record.completion_tokens
                )
            
            # Recalculate performance metrics now that we have token counts
            if hasattr(self._trace_record, 'total_tokens') and self._trace_record.total_tokens:
                # Get timing info from trace record
                if (self._trace_record.request_timestamp and 
                    self._trace_record.response_timestamp):
                    
                    start_time = self._trace_record.request_timestamp.timestamp()
                    end_time = self._trace_record.response_timestamp.timestamp()
                    
                    metrics = calculate_performance_metrics(
                        start_time, end_time, 
                        total_tokens=self._trace_record.total_tokens
                    )
                    
                    for key, value in metrics.items():
                        if hasattr(self._trace_record, key):
                            setattr(self._trace_record, key, value)
            
            # Update completeness and log enhanced trace
            self._trace_record.update_completeness()
            trace_logger.debug(f"ENHANCED_STREAMING_TRACE: {self._trace_record.to_json()}")
            
        except Exception as e:
            trace_logger.error(f"Error parsing streaming response: {e}")


class TracedTransport(httpx.BaseTransport):
    """Custom httpx transport that logs all requests and responses"""
    
    def __init__(self, wrapped_transport:httpx.BaseTransport | None = None):
        self.wrapped_transport = wrapped_transport or httpx.HTTPTransport()
        self.stats = {
            'total_calls': 0,
            'successful_calls': 0,
            'total_tokens': 0,
            'total_duration': 0.0,
        }
        
    def _redact_headers(self, headers: httpx.Headers) -> dict:
        """Redact sensitive information from headers"""
        safe_headers = {}
        for key, value in headers.items():
            if key.lower() in ['authorization', 'api-key', 'x-api-key']:
                safe_headers[key] = '[REDACTED]'
            else:
                safe_headers[key] = value
        return safe_headers
    
    def _log_request(self, request: httpx.Request) -> None:
        """Log outgoing request details"""
        safe_headers = self._redact_headers(request.headers)
        
        trace_logger.info(f"→ REQUEST: {request.method} {request.url}")
        trace_logger.info(f"  Headers: {json.dumps(safe_headers, indent=2)}")
        
        if request.content:
            try:
                # Try to parse as JSON for prettier logging
                if request.headers.get('content-type', '').startswith('application/json'):
                    content = json.loads(request.content.decode())
                    trace_logger.info(f"  Body: {json.dumps(content, indent=2)}")
                else:
                    trace_logger.info(f"  Body size: {len(request.content)} bytes")
            except Exception:
                trace_logger.info(f"  Body size: {len(request.content)} bytes")
    
    def _log_response(self, response: httpx.Response, duration: float) -> None:
        """Log incoming response details"""
        trace_logger.info(f"← RESPONSE: {response.status_code} {response.reason_phrase}")
        trace_logger.info(f"  Duration: {duration:.3f}s")
        
        # Log content type and size for non-streaming responses
        content_type = response.headers.get('content-type', '')
        trace_logger.info(f"  Content-Type: {content_type}")
        
        if not self._is_streaming_response(response):
            try:
                content_length = len(response.content)
                trace_logger.info(f"  Content size: {content_length} bytes")
                
                # Try to extract token usage from OpenAI responses
                if content_type.startswith('application/json'):
                    try:
                        data = response.json()
                        if 'usage' in data:
                            usage = data['usage']
                            trace_logger.info(f"  Token usage: {json.dumps(usage, indent=2)}")
                            self.stats['total_tokens'] += usage.get('total_tokens', 0)
                    except Exception:
                        pass
            except Exception:
                trace_logger.info("  Content: [unable to read]")
        else:
            trace_logger.info("  Content: [streaming response]")
    
    def _is_streaming_response(self, response: httpx.Response) -> bool:
        """Check if response is a streaming response"""
        content_type = response.headers.get('content-type', '')
        return 'text/event-stream' in content_type or response.headers.get('transfer-encoding') == 'chunked'
    
    
    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Handle request with comprehensive tracing and logging"""
        self.stats['total_calls'] += 1
        start_time = time.time()
        request_timestamp = datetime.now()
        
        # Create trace record
        trace = TraceRecord(request_timestamp=request_timestamp)
        
        # Parse request data
        request_data = parse_openai_request(request)
        for key, value in request_data.items():
            if hasattr(trace, key):
                setattr(trace, key, value)
        
        # Log the outgoing request (simplified)
        self._log_request(request)
        
        try:
            # Delegate to the wrapped transport
            response = self.wrapped_transport.handle_request(request)
            end_time = time.time()
            response_timestamp = datetime.now()
            
            # Update trace with response timing
            trace.response_timestamp = response_timestamp
            duration = end_time - start_time
            self.stats['total_duration'] += duration
            
            # Parse response data
            response_content = None
            if not self._is_streaming_response(response):
                try:
                    response_content = response.content
                except Exception:
                    pass
            
            response_data = parse_openai_response(response, response_content)
            for key, value in response_data.items():
                if hasattr(trace, key):
                    setattr(trace, key, value)
            
            # Populate assistant message token counts
            if trace.completion_tokens and trace.full_conversation:
                trace.full_conversation = populate_assistant_message_tokens(
                    trace.full_conversation, trace.completion_tokens
                )
            
            # Calculate performance metrics
            metrics = calculate_performance_metrics(
                start_time, end_time, 
                total_tokens=trace.total_tokens
            )
            for key, value in metrics.items():
                if hasattr(trace, key):
                    setattr(trace, key, value)
            
            # Mark as successful
            trace.success = True
            trace.mark_completed()
            
            # Update stats
            if trace.total_tokens:
                self.stats['total_tokens'] += trace.total_tokens
            self.stats['successful_calls'] += 1
            
            # Log comprehensive trace data
            trace_logger.debug(f"COMPREHENSIVE_TRACE: {trace.to_json()}")
            
            # Log the response (simplified)
            self._log_response(response, duration)
            
            # Handle streaming responses with proper LogResponse
            if self._is_streaming_response(response):
                logged_response = LogResponse(
                    status_code=response.status_code,
                    headers=response.headers,
                    stream=response.stream,
                    extensions=response.extensions,
                )
                # Pass trace record so streaming content can update it
                logged_response.set_trace_record(trace)
                return logged_response
            
            return response
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            # Mark trace as failed
            error_category, error_code = categorize_error(e)
            trace.mark_error(error_code, str(e), error_category)
            trace.total_latency_ms = (end_time - start_time) * 1000
            
            # Log comprehensive trace data even for errors
            trace_logger.debug(f"COMPREHENSIVE_TRACE: {trace.to_json()}")
            
            trace_logger.error(f"← ERROR after {duration:.3f}s: {e}")
            raise
    
    def close(self) -> None:
        """Close the wrapped transport"""
        if hasattr(self.wrapped_transport, 'close'):
            self.wrapped_transport.close()


class TracedClient(httpx.Client):
    """httpx.Client with automatic request/response tracing"""
    
    def __init__(self, *args, **kwargs):
        # Extract transport if provided, otherwise None
        transport = kwargs.pop('transport', None)
        
        # Create our traced transport, wrapping the provided transport if any
        traced_transport = TracedTransport(transport)
        
        # Initialize the parent client with our traced transport
        super().__init__(*args, transport=traced_transport, **kwargs)
        
        # Keep reference to our transport for stats
        self._traced_transport = traced_transport
