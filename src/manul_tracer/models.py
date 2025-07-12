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
from typing import Optional, Dict, Any, List


@dataclass
class Message:
    """Individual message within a conversation trace."""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    trace_id: Optional[str] = None
    role: Optional[str] = None  # system, user, assistant, tool
    content: Optional[str] = None
    message_order: Optional[int] = None
    message_timestamp: Optional[datetime] = None
    
    # Token count - only populated for assistant messages (from completion_tokens)
    token_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper datetime serialization."""
        data = asdict(self)
        if data['message_timestamp'] and isinstance(data['message_timestamp'], datetime):
            data['message_timestamp'] = data['message_timestamp'].isoformat()
        return data


@dataclass
class Session:
    """Session tracking for grouping related traces."""
    session_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: Optional[str] = None
    session_name: Optional[str] = None
    session_type: Optional[str] = None  # chat, completion, etc.
    session_created_at: Optional[datetime] = None
    session_ended_at: Optional[datetime] = None
    last_activity: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper datetime serialization."""
        data = asdict(self)
        for timestamp_field in ['session_created_at', 'session_ended_at', 'last_activity']:
            if data[timestamp_field] and isinstance(data[timestamp_field], datetime):
                data[timestamp_field] = data[timestamp_field].isoformat()
        return data


@dataclass
class TraceRecord:
    """Comprehensive trace record for LLM API calls."""
    
    # Core Identifiers
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    
    # Request Metadata
    model: Optional[str] = None
    provider: str = "openai"
    endpoint: Optional[str] = None
    api_version: Optional[str] = None
    request_timestamp: Optional[datetime] = None
    response_timestamp: Optional[datetime] = None
    
    # API Parameters
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    top_p: Optional[float] = None
    frequency_penalty: Optional[float] = None
    presence_penalty: Optional[float] = None
    stream: Optional[bool] = None
    stop_sequences: Optional[List[str]] = None
    logit_bias: Optional[Dict[str, float]] = None
    
    # Content Data
    system_prompt: Optional[str] = None
    user_prompt: Optional[str] = None
    assistant_response: Optional[str] = None
    full_conversation: Optional[List[Message]] = field(default_factory=list)
    
    # Response Metadata
    finish_reason: Optional[str] = None
    choice_index: Optional[int] = None
    response_id: Optional[str] = None
    
    # Usage - Aggregate Counts
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    total_tokens: Optional[int] = None
    
    # Detailed Prompt Token Breakdown
    prompt_cached_tokens: Optional[int] = None
    prompt_audio_tokens: Optional[int] = None
    
    # Detailed Completion Token Breakdown
    completion_reasoning_tokens: Optional[int] = None
    completion_audio_tokens: Optional[int] = None
    completion_accepted_prediction_tokens: Optional[int] = None
    completion_rejected_prediction_tokens: Optional[int] = None
    
    # Performance Metrics
    total_latency_ms: Optional[float] = None
    tokens_per_second: Optional[float] = None
    processing_time_ms: Optional[float] = None
    
    # Error Handling
    success: bool = True
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    error_category: Optional[str] = None
    
    # Rate Limiting & Quotas
    rate_limit_remaining: Optional[int] = None
    rate_limit_reset: Optional[datetime] = None
    quota_consumed: Optional[float] = None
    
    # Data Completeness Tracking
    data_completeness_score: Optional[float] = None
    missing_fields: Optional[List[str]] = field(default_factory=list)
    trace_status: str = "pending"  # complete, partial, error, timeout
    
    # Technical Details
    request_size_bytes: Optional[int] = None
    response_size_bytes: Optional[int] = None
    
    # Timestamps for Lifecycle
    trace_created_at: Optional[datetime] = field(default_factory=datetime.now)
    trace_updated_at: Optional[datetime] = None
    trace_completed_at: Optional[datetime] = None
    
    def calculate_completeness_score(self) -> float:
        """Calculate what percentage of fields are populated."""
        all_fields = list(self.__dataclass_fields__.keys())
        populated_fields = 0
        
        for field_name in all_fields:
            value = getattr(self, field_name)
            if value is not None and value != [] and value != "":
                populated_fields += 1
                
        return populated_fields / len(all_fields) if all_fields else 0.0
    
    def get_missing_fields(self) -> List[str]:
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
    
    def mark_error(self, error_code: str, error_message: str, error_category: str = None):
        """Mark trace as failed with error details."""
        self.success = False
        self.error_code = error_code
        self.error_message = error_message
        self.error_category = error_category
        self.trace_status = "error"
        self.trace_completed_at = datetime.now()
        self.update_completeness()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with proper serialization."""
        data = asdict(self)
        
        # Handle datetime serialization
        datetime_fields = [
            'request_timestamp', 'response_timestamp', 'rate_limit_reset',
            'trace_created_at', 'trace_updated_at', 'trace_completed_at'
        ]
        
        for field_name in datetime_fields:
            if data[field_name] and isinstance(data[field_name], datetime):
                data[field_name] = data[field_name].isoformat()
        
        # Handle nested Message objects - they're already dicts from asdict(), 
        # but need datetime conversion
        if data['full_conversation']:
            converted_messages = []
            for msg in data['full_conversation']:
                if isinstance(msg, dict):
                    # Handle datetime in message dict
                    if msg.get('message_timestamp') and isinstance(msg['message_timestamp'], datetime):
                        msg['message_timestamp'] = msg['message_timestamp'].isoformat()
                    converted_messages.append(msg)
                else:
                    # Fallback: if it's still a Message object, convert it
                    converted_messages.append(msg.to_dict() if hasattr(msg, 'to_dict') else msg)
            data['full_conversation'] = converted_messages
        
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TraceRecord':
        """Create TraceRecord from dictionary."""
        # Handle datetime deserialization
        datetime_fields = [
            'request_timestamp', 'response_timestamp', 'rate_limit_reset',
            'trace_created_at', 'trace_updated_at', 'trace_completed_at'
        ]
        
        for field_name in datetime_fields:
            if data.get(field_name):
                data[field_name] = datetime.fromisoformat(data[field_name])
        
        # Handle nested Message objects
        if data.get('full_conversation'):
            messages = []
            for msg_data in data['full_conversation']:
                if isinstance(msg_data, dict):
                    if msg_data.get('message_timestamp'):
                        msg_data['message_timestamp'] = datetime.fromisoformat(msg_data['message_timestamp'])
                    messages.append(Message(**msg_data))
                else:
                    messages.append(msg_data)
            data['full_conversation'] = messages
        
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'TraceRecord':
        """Create TraceRecord from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)