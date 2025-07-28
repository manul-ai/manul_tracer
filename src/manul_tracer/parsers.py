"""Parsing utilities for OpenAI API requests and responses."""

from typing import Any
from datetime import datetime
import json
import httpx
import hashlib
import base64


def parse_openai_request(request: httpx.Request) -> dict[str, Any]:
    """Parse OpenAI API request to extract parameters and content."""
    try:
        if request.content:
            body = json.loads(request.content.decode('utf-8'))
            return body
        elif hasattr(request, '_content') and request._content:
            body = json.loads(request._content.decode('utf-8'))
            return body
        else:
            if hasattr(request.stream, 'read'):
                content = request.stream.read()
                request.stream = httpx.ByteStream(content)
                body = json.loads(content.decode('utf-8'))
                return body
    except Exception:
        pass
    
    return {}


def parse_openai_response(response: httpx.Response, is_streaming: bool = False) -> dict[str, Any]:
    """Parse OpenAI API response to extract usage data and content."""
    result = {
        'prompt_tokens': 0,
        'completion_tokens': 0,
        'total_tokens': 0,
        'finish_reason': None,
        'content': None,
        'assistant_content': None,
        'response_format': 'text'
    }
    
    try:
        if hasattr(response, 'get_captured_content'):
            content = response.get_captured_content()
        else:
            content = response.content
            
        if not content:
            return result
            
        if is_streaming:
            text = content.decode('utf-8') if isinstance(content, bytes) else str(content)
            lines = text.strip().split('\n')
            
            chunks = []
            usage_data = None
            
            for line in lines:
                if line.startswith('data: '):
                    data_str = line[6:]
                    if data_str.strip() == '[DONE]':
                        continue
                        
                    try:
                        chunk_data = json.loads(data_str)
                        
                        if 'choices' in chunk_data and chunk_data['choices']:
                            choice = chunk_data['choices'][0]
                            
                            if 'finish_reason' in choice and choice['finish_reason']:
                                result['finish_reason'] = choice['finish_reason']
                            
                            if 'delta' in choice and 'content' in choice['delta']:
                                chunks.append(choice['delta']['content'])
                        
                        if 'usage' in chunk_data:
                            usage_data = chunk_data['usage']
                            
                    except json.JSONDecodeError:
                        continue
            
            if chunks:
                result['assistant_content'] = ''.join(chunks)
                result['content'] = result['assistant_content']
            
            if usage_data:
                result['prompt_tokens'] = usage_data.get('prompt_tokens', 0)
                result['completion_tokens'] = usage_data.get('completion_tokens', 0)
                result['total_tokens'] = usage_data.get('total_tokens', 0)
                
                if 'prompt_tokens_details' in usage_data:
                    details = usage_data['prompt_tokens_details']
                    result['prompt_cached_tokens'] = details.get('cached_tokens')
                    result['prompt_audio_tokens'] = details.get('audio_tokens')
                
                if 'completion_tokens_details' in usage_data:
                    details = usage_data['completion_tokens_details']
                    result['completion_reasoning_tokens'] = details.get('reasoning_tokens')
                    result['completion_audio_tokens'] = details.get('audio_tokens')
                    result['completion_accepted_prediction_tokens'] = details.get('accepted_prediction_tokens')
                    result['completion_rejected_prediction_tokens'] = details.get('rejected_prediction_tokens')
                    
        else:
            response_json = json.loads(content) if isinstance(content, bytes) else content
            
            if 'usage' in response_json:
                usage = response_json['usage']
                result['prompt_tokens'] = usage.get('prompt_tokens', 0)
                result['completion_tokens'] = usage.get('completion_tokens', 0)
                result['total_tokens'] = usage.get('total_tokens', 0)
                
                if 'prompt_tokens_details' in usage:
                    details = usage['prompt_tokens_details']
                    result['prompt_cached_tokens'] = details.get('cached_tokens')
                    result['prompt_audio_tokens'] = details.get('audio_tokens')
                
                if 'completion_tokens_details' in usage:
                    details = usage['completion_tokens_details']
                    result['completion_reasoning_tokens'] = details.get('reasoning_tokens')
                    result['completion_audio_tokens'] = details.get('audio_tokens')
                    result['completion_accepted_prediction_tokens'] = details.get('accepted_prediction_tokens')
                    result['completion_rejected_prediction_tokens'] = details.get('rejected_prediction_tokens')
            
            if 'choices' in response_json and response_json['choices']:
                choice = response_json['choices'][0]
                result['finish_reason'] = choice.get('finish_reason')
                
                if 'message' in choice:
                    result['assistant_content'] = choice['message'].get('content')
                    result['content'] = result['assistant_content']
                elif 'text' in choice:
                    result['assistant_content'] = choice['text']
                    result['content'] = result['assistant_content']
        
        headers = dict(response.headers)
        if 'x-ratelimit-limit-requests' in headers:
            result['rate_limit_requests_limit'] = int(headers['x-ratelimit-limit-requests'])
        if 'x-ratelimit-remaining-requests' in headers:
            result['rate_limit_requests_remaining'] = int(headers['x-ratelimit-remaining-requests'])
        if 'x-ratelimit-limit-tokens' in headers:
            result['rate_limit_tokens_limit'] = int(headers['x-ratelimit-limit-tokens'])
        if 'x-ratelimit-remaining-tokens' in headers:
            result['rate_limit_tokens_remaining'] = int(headers['x-ratelimit-remaining-tokens'])
            
    except Exception:
        pass
    
    return result


def calculate_performance_metrics(
    start_time: datetime,
    end_time: datetime,
    prompt_tokens: int,
    completion_tokens: int
) -> dict[str, Any]:
    """Calculate performance metrics for the API call."""
    duration = (end_time - start_time).total_seconds()
    latency_ms = duration * 1000
    
    metrics = {
        'latency_ms': latency_ms,
        'time_to_first_token_ms': None,
        'tokens_per_second': None
    }
    
    if completion_tokens > 0 and duration > 0:
        metrics['tokens_per_second'] = completion_tokens / duration
        if completion_tokens > 1:
            metrics['time_to_first_token_ms'] = latency_ms / completion_tokens
    
    return metrics


def is_streaming_request(request_body: dict[str, Any]) -> bool:
    """Check if the request is for streaming response."""
    return request_body.get('stream', False) is True


def extract_conversation_messages(request_body: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract and structure conversation messages from request."""
    messages = []
    
    if 'messages' in request_body:
        for msg in request_body['messages']:
            # Only extract fields that Message dataclass accepts
            message = {
                'role': msg.get('role'),
                'content': msg.get('content')
            }
            messages.append(message)
    
    return messages


def populate_assistant_message_tokens(
    messages: list[Any],
    completion_tokens: int
) -> list[Any]:
    """Populate token counts for assistant messages."""
    assistant_messages = [m for m in messages if m.role == "assistant"]
    
    if assistant_messages and completion_tokens > 0:
        assistant_messages[-1].token_count = completion_tokens
    
    return messages


def categorize_error(status_code: int, error_message: str) -> str:
    """Categorize error type based on status code and message."""
    if status_code == 429:
        return "RateLimitError"
    elif status_code == 401:
        return "AuthenticationError"
    elif status_code == 404:
        return "NotFoundError"
    elif status_code >= 500:
        return "ServerError"
    elif status_code >= 400:
        if "context_length_exceeded" in error_message.lower():
            return "ContextLengthExceededError"
        elif "invalid_request" in error_message.lower():
            return "InvalidRequestError"
        else:
            return "ClientError"
    else:
        return "UnknownError"

