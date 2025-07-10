import httpx
import json
import logging
import time
from datetime import datetime
from typing import Iterator, Optional, Dict, Any


# Configure logging for trace output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TRACE - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

trace_logger = logging.getLogger('manul_tracer')


class LogResponse(httpx.Response):
    """Custom Response class that logs streaming content during consumption"""
    
    def iter_bytes(self, chunk_size: int = 1024):
        """Override iter_bytes to log chunks as they're consumed"""
        trace_logger.info("  Starting to read streaming response...")
        chunk_count = 0
        total_size = 0
        
        for chunk in super().iter_bytes(chunk_size):
            chunk_count += 1
            total_size += len(chunk)
            if chunk_count % 10 == 0:  # Log every 10th chunk to avoid spam
                trace_logger.info(f"  Received chunk {chunk_count}, total size: {total_size} bytes")
            yield chunk
        
        trace_logger.info(f"  Streaming complete: {chunk_count} chunks, {total_size} total bytes")


class TracedTransport(httpx.BaseTransport):
    """Custom httpx transport that logs all requests and responses"""
    
    def __init__(self, wrapped_transport: Optional[httpx.BaseTransport] = None):
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
        """Handle request with logging and statistics tracking"""
        self.stats['total_calls'] += 1
        start_time = time.time()
        
        # Log the outgoing request
        self._log_request(request)
        
        try:
            # Delegate to the wrapped transport
            response = self.wrapped_transport.handle_request(request)
            duration = time.time() - start_time
            self.stats['total_duration'] += duration
            
            # Log the response
            self._log_response(response, duration)
            
            # Handle streaming responses with proper LogResponse
            if self._is_streaming_response(response):
                # Create LogResponse that will log chunks during consumption
                logged_response = LogResponse(
                    status_code=response.status_code,
                    headers=response.headers,
                    stream=response.stream,
                    extensions=response.extensions,
                )
                self.stats['successful_calls'] += 1
                return logged_response
            
            self.stats['successful_calls'] += 1
            return response
            
        except Exception as e:
            duration = time.time() - start_time
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
    
    def get_stats(self) -> Dict[str, Any]:
        """Get tracing statistics for display in Streamlit"""
        stats = self._traced_transport.stats.copy()
        
        # Calculate average duration
        if stats['total_calls'] > 0:
            stats['average_duration'] = stats['total_duration'] / stats['total_calls']
        else:
            stats['average_duration'] = 0.0
            
        return stats