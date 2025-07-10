"""
Utilities for parsing OpenAI API requests and responses.

This module contains helper functions for extracting data from
OpenAI API structures and calculating metrics.
"""

import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse

from .models import TraceRecord, Message


def parse_openai_request(request) -> Dict[str, Any]:
    """Parse OpenAI API request to extract parameters and content."""
    parsed_data = {}
    
    try:
        # Extract endpoint information
        url = str(request.url)
        parsed_url = urlparse(url)
        parsed_data['endpoint'] = parsed_url.path
        parsed_data['provider'] = 'openai'
        
        # Parse request body if it exists
        if hasattr(request, 'content') and request.content:
            try:
                body = json.loads(request.content.decode('utf-8'))
                
                # Extract API parameters
                api_params = [
                    'model', 'temperature', 'max_tokens', 'top_p', 
                    'frequency_penalty', 'presence_penalty', 'stream',
                    'stop', 'logit_bias'
                ]
                
                for param in api_params:
                    if param in body:
                        if param == 'stop':
                            parsed_data['stop_sequences'] = body[param]
                        else:
                            parsed_data[param] = body[param]
                
                # Extract messages and content
                if 'messages' in body:
                    messages = body['messages']
                    parsed_data['full_conversation'] = []
                    
                    for i, msg in enumerate(messages):
                        message = Message(
                            role=msg.get('role'),
                            content=msg.get('content'),
                            message_order=i,
                            message_timestamp=datetime.now()
                        )
                        parsed_data['full_conversation'].append(message)
                        
                        # Extract specific prompts for easy access
                        if msg.get('role') == 'system':
                            parsed_data['system_prompt'] = msg.get('content')
                        elif msg.get('role') == 'user':
                            parsed_data['user_prompt'] = msg.get('content')
                
                # Calculate request size
                parsed_data['request_size_bytes'] = len(request.content)
                
            except json.JSONDecodeError:
                parsed_data['request_size_bytes'] = len(request.content)
        
        # Extract headers information
        headers = dict(request.headers)
        if 'user-agent' in headers:
            parsed_data['api_version'] = headers.get('user-agent', '').split('/')[-1]
        
    except Exception as e:
        print(f"Error parsing request: {e}")
    
    return parsed_data


def parse_openai_response(response, content_data: bytes = None) -> Dict[str, Any]:
    """Parse OpenAI API response to extract metadata and content."""
    parsed_data = {}
    
    try:
        # Extract response metadata
        parsed_data['response_size_bytes'] = len(content_data) if content_data else 0
        
        # Parse response headers for rate limiting
        headers = dict(response.headers)
        
        # Rate limiting headers (OpenAI specific)
        rate_limit_headers = {
            'x-ratelimit-remaining-requests': 'rate_limit_remaining',
            'x-ratelimit-reset-requests': 'rate_limit_reset',
            'x-ratelimit-remaining-tokens': 'quota_consumed'
        }
        
        for header_name, field_name in rate_limit_headers.items():
            if header_name in headers:
                value = headers[header_name]
                if field_name == 'rate_limit_reset':
                    # Convert to datetime if it's a timestamp
                    try:
                        parsed_data[field_name] = datetime.fromtimestamp(float(value))
                    except (ValueError, TypeError):
                        parsed_data[field_name] = value
                else:
                    try:
                        parsed_data[field_name] = int(value)
                    except (ValueError, TypeError):
                        parsed_data[field_name] = value
        
        # Parse JSON response body
        if content_data:
            try:
                response_json = json.loads(content_data.decode('utf-8'))
                
                # Extract response ID
                if 'id' in response_json:
                    parsed_data['response_id'] = response_json['id']
                
                # Extract model
                if 'model' in response_json:
                    parsed_data['model'] = response_json['model']
                
                # Extract usage information
                if 'usage' in response_json:
                    usage = response_json['usage']
                    parsed_data['prompt_tokens'] = usage.get('prompt_tokens')
                    parsed_data['completion_tokens'] = usage.get('completion_tokens')
                    parsed_data['total_tokens'] = usage.get('total_tokens')
                
                # Extract response content and metadata
                if 'choices' in response_json and response_json['choices']:
                    choice = response_json['choices'][0]  # Take first choice
                    parsed_data['choice_index'] = 0
                    
                    if 'message' in choice:
                        message = choice['message']
                        parsed_data['assistant_response'] = message.get('content')
                    
                    if 'finish_reason' in choice:
                        parsed_data['finish_reason'] = choice['finish_reason']
                
            except json.JSONDecodeError:
                # Response isn't JSON, might be streaming
                pass
    
    except Exception as e:
        print(f"Error parsing response: {e}")
    
    return parsed_data


def calculate_performance_metrics(
    start_time: float, 
    end_time: float, 
    first_token_time: Optional[float] = None,
    total_tokens: Optional[int] = None
) -> Dict[str, Any]:
    """Calculate performance metrics from timing data."""
    metrics = {}
    
    try:
        # Basic latency
        total_latency = (end_time - start_time) * 1000  # Convert to milliseconds
        metrics['total_latency_ms'] = round(total_latency, 2)
        
        # First token latency (for streaming)
        if first_token_time:
            first_token_latency = (first_token_time - start_time) * 1000
            metrics['first_token_latency_ms'] = round(first_token_latency, 2)
        
        # Tokens per second
        if total_tokens and total_latency > 0:
            tokens_per_sec = (total_tokens * 1000) / total_latency  # tokens/second
            metrics['tokens_per_second'] = round(tokens_per_sec, 2)
        
        # For now, assume processing time equals total latency
        # In a more advanced implementation, we could distinguish queue vs processing time
        metrics['processing_time_ms'] = metrics['total_latency_ms']
        
    except Exception as e:
        print(f"Error calculating performance metrics: {e}")
    
    return metrics


def is_streaming_request(request) -> bool:
    """Check if the request is for streaming response."""
    try:
        if hasattr(request, 'content') and request.content:
            body = json.loads(request.content.decode('utf-8'))
            return body.get('stream', False)
    except (json.JSONDecodeError, AttributeError):
        pass
    return False


def extract_conversation_messages(messages_data: List[Dict]) -> List[Message]:
    """Extract and structure conversation messages."""
    messages = []
    
    for i, msg_data in enumerate(messages_data):
        message = Message(
            role=msg_data.get('role'),
            content=msg_data.get('content'),
            message_order=i,
            message_timestamp=datetime.now()
        )
        messages.append(message)
    
    return messages


def categorize_error(exception: Exception) -> Tuple[str, str]:
    """Categorize error type and return (error_category, error_code)."""
    error_str = str(exception).lower()
    
    if 'timeout' in error_str:
        return 'timeout', 'REQUEST_TIMEOUT'
    elif 'rate limit' in error_str or '429' in error_str:
        return 'rate_limit', 'RATE_LIMIT_EXCEEDED'
    elif 'auth' in error_str or '401' in error_str:
        return 'auth', 'AUTHENTICATION_FAILED'
    elif 'permission' in error_str or '403' in error_str:
        return 'auth', 'PERMISSION_DENIED'
    elif 'not found' in error_str or '404' in error_str:
        return 'client', 'ENDPOINT_NOT_FOUND'
    elif 'bad request' in error_str or '400' in error_str:
        return 'client', 'BAD_REQUEST'
    elif '5' in error_str and any(x in error_str for x in ['500', '502', '503', '504']):
        return 'server', 'SERVER_ERROR'
    elif 'connection' in error_str:
        return 'network', 'CONNECTION_ERROR'
    else:
        return 'unknown', 'UNKNOWN_ERROR'