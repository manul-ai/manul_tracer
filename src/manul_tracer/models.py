"""
Data models for comprehensive LLM tracing with database storage.

This module contains the core data structures for capturing detailed
information about LLM API calls, conversations, and performance metrics,
optimized for persistence in a database using the repository pattern.
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any

from .parsers import parse_openai_response, populate_assistant_message_tokens


@dataclass
class Message:
    """Individual message within a conversation trace."""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: str | None = None
    role: str | None = None
    content: str | list | None = None  # Allow both string and list for Vision API
    has_images: bool = False
    message_order: int | None = None
    message_timestamp: datetime | None = None
    
    token_count: int | None = None

    def to_dict(self, skip_none: bool = False) -> dict[str, Any]:
        """Convert to dictionary with proper datetime serialization.
        
        Args:
            skip_none: If True, exclude key-value pairs where value is None
        """
        data = asdict(self)
        if data['message_timestamp'] and isinstance(data['message_timestamp'], datetime):
            data['message_timestamp'] = data['message_timestamp'].isoformat()
        
        # Properly serialize content if it's a list (Vision API format)
        if data['content'] and isinstance(data['content'], list):
            data['content'] = json.dumps(data['content'])
        
        # Skip None values if requested
        if skip_none:
            data = {k: v for k, v in data.items() if v is not None}
        
        return data
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'Message':
        """Create Message from dictionary.
        
        Args:
            data: Dictionary containing message data
            
        Returns:
            Message instance with proper datetime deserialization
        """
        # Convert timestamp string back to datetime if present and it's a string
        if data.get('message_timestamp') and isinstance(data['message_timestamp'], str):
            data['message_timestamp'] = datetime.fromisoformat(data['message_timestamp'])
        
        # Parse content if it's a JSON string representing a list
        if data.get('content') and isinstance(data['content'], str):
            try:
                # Try to parse as JSON if it looks like a list
                if data['content'].startswith('[') and data['content'].endswith(']'):
                    data['content'] = json.loads(data['content'])
            except json.JSONDecodeError:
                # If parsing fails, keep as string
                pass
        
        return cls(**data)


@dataclass
class Session:
    """Session tracking for grouping related traces."""
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str | None = None
    session_name: str | None = None
    session_type: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    ended_at: datetime | None = None
    last_activity_at: datetime | None = None

    def to_dict(self, skip_none: bool = False) -> dict[str, Any]:
        """Convert to dictionary with proper datetime serialization.
        
        Args:
            skip_none: If True, exclude key-value pairs where value is None
        """
        data = asdict(self)
        for timestamp_field in ['created_at', 'started_at', 'ended_at', 'last_activity_at']:
            if data[timestamp_field] and isinstance(data[timestamp_field], datetime):
                data[timestamp_field] = data[timestamp_field].isoformat()
        
        # Skip None values if requested
        if skip_none:
            data = {k: v for k, v in data.items() if v is not None}
        
        return data
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'Session':
        """Create Session from dictionary.
        
        Args:
            data: Dictionary containing session data
            
        Returns:
            Session instance with proper datetime deserialization
        """
        # Convert timestamp strings back to datetime if present and they're strings
        timestamp_fields = ['created_at', 'started_at', 'ended_at', 'last_activity_at']
        for field_name in timestamp_fields:
            if data.get(field_name) and isinstance(data[field_name], str):
                data[field_name] = datetime.fromisoformat(data[field_name])
        
        return cls(**data)


@dataclass
class Image:
    """Image metadata for images sent in API requests."""
    image_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    image_hash: str | None = None
    size_mb: float | None = None
    format: str | None = None
    width: int | None = None
    height: int | None = None
    created_at: datetime | None = None
    
    def to_dict(self, skip_none: bool = False) -> dict[str, Any]:
        """Convert to dictionary with proper datetime serialization.
        
        Args:
            skip_none: If True, exclude key-value pairs where value is None
        """
        data = asdict(self)
        if data['created_at'] and isinstance(data['created_at'], datetime):
            data['created_at'] = data['created_at'].isoformat()
        
        # Skip None values if requested
        if skip_none:
            data = {k: v for k, v in data.items() if v is not None}
        
        return data
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'Image':
        """Create Image from dictionary.
        
        Args:
            data: Dictionary containing image data
            
        Returns:
            Image instance with proper datetime deserialization
        """
        # Convert timestamp string back to datetime if present and it's a string
        if data.get('created_at') and isinstance(data['created_at'], str):
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        
        return cls(**data)


@dataclass
class TraceRecord:
    """Comprehensive trace record for LLM API calls."""
    
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session_id: str | None = None
    user_id: str | None = None
    model_id: str | None = None
    endpoint: str | None = None
    api_version: str | None = None
    request_timestamp: datetime | None = None
    response_timestamp: datetime | None = None
    
    temperature: float | None = None
    max_tokens: int | None = None
    top_p: float | None = None
    frequency_penalty: float | None = None
    presence_penalty: float | None = None
    stream: bool | None = None
    stop_sequences: list[str] | None = None
    logit_bias: dict[str, float] | None = None
    seed: int | None = None
    
    full_conversation: list[Message] | None = field(default_factory=list)
    images: list[Image] | None = field(default_factory=list)
    
    finish_reason: str | None = None
    choice_index: int | None = None
    response_id: str | None = None
    
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    
    prompt_cached_tokens: int | None = None
    prompt_audio_tokens: int | None = None
    
    completion_reasoning_tokens: int | None = None
    completion_audio_tokens: int | None = None
    completion_accepted_prediction_tokens: int | None = None
    completion_rejected_prediction_tokens: int | None = None
    
    total_latency_ms: float | None = None
    tokens_per_second: float | None = None
    processing_time_ms: float | None = None
    
    success: bool = True
    error_code: str | None = None
    error_message: str | None = None
    retry_count: int = 0
    error_category: str | None = None
    
    rate_limit_remaining: int | None = None
    rate_limit_reset: datetime | None = None
    quota_consumed: float | None = None
    
    data_completeness_score: float | None = None
    missing_fields: list[str] | None = field(default_factory=list)
    trace_status: str = "pending"
    
    request_size_bytes: int | None = None
    response_size_bytes: int | None = None
    
    trace_created_at: datetime | None = field(default_factory=datetime.now)
    trace_updated_at: datetime | None = None
    trace_completed_at: datetime | None = None
    
    def calculate_completeness_score(self) -> float:
        """Calculate what percentage of fields are populated."""
        all_fields = list(self.__dataclass_fields__.keys())
        populated_fields = 0
        
        for field_name in all_fields:
            value = getattr(self, field_name)
            if value is not None and value != [] and value != "":
                populated_fields += 1
                
        return populated_fields / len(all_fields) if all_fields else 0.0
    
    def get_missing_fields(self) -> list[str]:
        """Get list of fields that are None or empty."""
        missing = []
        for field_name in self.__dataclass_fields__.keys():
            value = getattr(self, field_name)
            if value is None or value == [] or value == "":
                missing.append(field_name)
        return missing
    
    def update_completeness(self):
        """Update completeness tracking fields."""
        self.data_completeness_score = self.calculate_completeness_score()
        self.missing_fields = self.get_missing_fields()
        self.trace_updated_at = datetime.now()
    
    def mark_completed(self):
        """Mark trace as completed and update timestamps."""
        self.trace_status = "complete"
        self.trace_completed_at = datetime.now()
        self.update_completeness()
    
    def mark_error(self, error_code: str, error_message: str, error_category: str | None = None):
        """Mark trace as failed with error details."""
        self.success = False
        self.error_code = error_code
        self.error_message = error_message
        self.error_category = error_category
        self.trace_status = "error"
        self.trace_completed_at = datetime.now()
        self.update_completeness()

    def from_successful_response(self, captured_content: bytes, headers: dict, status_code: int) -> None:

        mock_response = type('MockResponse', (), {
            'content': captured_content,
            'headers': headers,
            'status_code': status_code
        })()
        
        # Parse the captured response
        response_data = parse_openai_response(mock_response, self.stream)
        
        # Update trace with response data
        self.prompt_tokens = response_data.get('prompt_tokens', 0)
        self.completion_tokens = response_data.get('completion_tokens', 0)
        self.total_tokens = response_data.get('total_tokens', 0)
        self.finish_reason = response_data.get('finish_reason')
        
        # Detailed token breakdowns
        self.prompt_cached_tokens = response_data.get('prompt_cached_tokens')
        self.prompt_audio_tokens = response_data.get('prompt_audio_tokens')
        self.completion_reasoning_tokens = response_data.get('completion_reasoning_tokens')
        self.completion_audio_tokens = response_data.get('completion_audio_tokens')
        self.completion_accepted_prediction_tokens = response_data.get('completion_accepted_prediction_tokens')
        self.completion_rejected_prediction_tokens = response_data.get('completion_rejected_prediction_tokens')
        
        if response_data.get('rate_limit_requests_remaining'):
            self.rate_limit_remaining = response_data.get('rate_limit_requests_remaining')
        
        if response_data.get('assistant_content'):
            assistant_message = Message(
                role="assistant",
                content=response_data['assistant_content']
            )
            self.full_conversation.append(assistant_message)
        
        self.full_conversation = populate_assistant_message_tokens(
            self.full_conversation,
            self.completion_tokens
        )
        
        # Calculate data completeness
        self.update_completeness()
    
    def to_dict(self, skip_none: bool = False) -> dict[str, Any]:
        """Convert to dictionary with proper serialization.
        
        Args:
            skip_none: If True, exclude key-value pairs where value is None
        """
        data = asdict(self)
        
        # Handle datetime serialization
        datetime_fields = [
            'request_timestamp', 'response_timestamp', 'rate_limit_reset',
            'trace_created_at', 'trace_updated_at', 'trace_completed_at'
        ]
        
        for field_name in datetime_fields:
            if data[field_name] and isinstance(data[field_name], datetime):
                data[field_name] = data[field_name].isoformat()
        
        # Convert Message objects to dicts with proper datetime handling
        if data['full_conversation']:
            data['full_conversation'] = [msg.to_dict(skip_none=skip_none) for msg in self.full_conversation]
        
        # Convert Image objects to dicts
        if data['images']:
            data['images'] = [img.to_dict(skip_none=skip_none) for img in self.images]
        
        if skip_none:
            data = {k: v for k, v in data.items() if v is not None}
        
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'TraceRecord':
        """Create TraceRecord from dictionary."""
        datetime_fields = [
            'request_timestamp', 'response_timestamp', 'rate_limit_reset',
            'trace_created_at', 'trace_updated_at', 'trace_completed_at'
        ]
        
        for field_name in datetime_fields:
            if data.get(field_name) and isinstance(data[field_name], str):
                data[field_name] = datetime.fromisoformat(data[field_name])
        
        # Convert message dicts back to Message objects
        if data.get('full_conversation'):
            messages = []
            for msg_data in data['full_conversation']:
                messages.append(Message.from_dict(msg_data))
            data['full_conversation'] = messages
        
        # Convert image dicts back to Image objects
        if data.get('images'):
            images = []
            for img_data in data['images']:
                images.append(Image.from_dict(img_data))
            data['images'] = images
        
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'TraceRecord':
        """Create TraceRecord from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)