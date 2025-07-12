"""Main ManulTracer class for OpenAI API call tracing."""

from typing import Optional, Dict, Any
import httpx

from .transport import TracedTransport
from .database.repositories import TraceRepository


class ManulTracer:
    """Main tracer class for intercepting and storing OpenAI API calls.
    
    Usage:
        tracer = ManulTracer()
        client = openai.OpenAI(api_key=api_key, http_client=tracer.http_client)
    """
    
    def __init__(
        self,
        repository: Optional[TraceRepository] = None,
        **httpx_kwargs
    ):
        """Initialize the ManulTracer.
        
        Args:
            repository: Optional TraceRepository for persisting traces
            **httpx_kwargs: Additional arguments passed to httpx.Client
        """
        self.repository = repository
        
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
            repository=repository
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
    
    def reset_stats(self):
        """Reset the statistics counters."""
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
        self._http_client.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

