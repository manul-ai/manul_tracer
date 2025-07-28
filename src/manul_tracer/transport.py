import logging
import sys
from datetime import datetime
import httpx
import json
import hashlib
import uuid

from .models import TraceRecord, Message

logger = logging.getLogger('manul_tracer.transport')
logger.setLevel(logging.INFO)

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
from .image_utils import (
    extract_images_from_request,
    update_messages_with_image_references
)


class LogResponse(httpx.Response):
    
    def __init__(self, *args, **kwargs):
        self.trace_record = kwargs.pop('trace_record', None)
        self.traced_transport = kwargs.pop('traced_transport', None)
        super().__init__(*args, **kwargs)
        self._captured_content = b''
        self._content_captured = False

    def iter_bytes(self, chunk_size: int = 1024):
        for chunk in super().iter_bytes(chunk_size):
            self._captured_content += chunk
            yield chunk
        if not self._content_captured:
            self._content_captured = True
            self._complete_trace()

    def read(self, *args, **kwargs):
        content = super().read(*args, **kwargs)
        if content and not self._captured_content:
            self._captured_content = content
            self._content_captured = True
            self._complete_trace()
        return content

    def _complete_trace(self):
        logger.info(f"LogResponse._complete_trace called")
        logger.info(f"  trace_record={self.trace_record is not None}")
        logger.info(f"  traced_transport={self.traced_transport is not None}")
        logger.info(f"  _captured_content={self._captured_content is not None}")
        
        if self.trace_record and self.traced_transport and self._captured_content:
            try:
                logger.info(f"  Updating trace record from response")
                self.trace_record.from_successful_response(
                    self._captured_content,
                    self.headers,
                    self.status_code
                )
                
                self.traced_transport.stats['total_prompt_tokens'] += self.trace_record.prompt_tokens or 0
                self.traced_transport.stats['total_completion_tokens'] += self.trace_record.completion_tokens or 0
                self.traced_transport.stats['total_tokens'] += self.trace_record.total_tokens or 0
                
                logger.info(f"  Checking if tracer exists: {self.traced_transport.tracer is not None}")
                if self.traced_transport.tracer:
                    logger.info(f"  Calling tracer._on_trace_completed")
                    self.traced_transport.tracer._on_trace_completed(self.trace_record)
                else:
                    logger.warning(f"  No tracer available to notify")
                
                logger.info("="*80)
                logger.info("ENHANCED TRACE RECORD:")
                logger.info("="*80)
                logger.info(self.trace_record.to_json())
                logger.info("="*80)
                
            except Exception as e:
                logger.error(f"Error completing trace: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")

    @property
    def content(self):
        if self._captured_content:
            return self._captured_content
        return super().content

    @property
    def text(self):
        content = self.content
        if isinstance(content, bytes):
            return content.decode('utf-8', errors='replace')
        return str(content)

    def json(self, **kwargs):
        text = self.text
        if not text:
            return None
        return json.loads(text)


class TracedTransport(httpx.BaseTransport):
    
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

        self.message_cache: dict[str, str] = {}

    def _get_or_assign_message_id(self, role: str, content: str, position: int) -> str:
        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()[:8]
        message_key = f"{role}_{position}_{content_hash}"
        
        if message_key not in self.message_cache:
            new_id = str(uuid.uuid4())
            self.message_cache[message_key] = new_id
            logger.debug(f"Assigned new message ID {new_id} for key {message_key}")
        
        return self.message_cache[message_key]

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.stats['total_requests'] += 1
        start_time = datetime.now()
        
        if self.tracer:
            self.tracer._initialize_session_if_needed()
            logger.info(f"Processing request #{self.stats['total_requests']} for session {self.session_id}")
        
        trace = TraceRecord(
            session_id=self.session_id,
            user_id=self.tracer.user_id if self.tracer else None,
            request_timestamp=start_time
        )
        
        request_dict = parse_openai_request(request)
        model_name = request_dict.get('model')
        
        # Get or create model_id from model name
        if model_name and self.repository:
            try:
                trace.model_id = self.repository.create_or_get_model(model_name, "openai")
            except Exception as e:
                logger.warning(f"Failed to get model_id for {model_name}: {e}")
                trace.model_id = None
        
        trace.endpoint = str(request.url)
        trace.stream = is_streaming_request(request_dict)
        
        # Extract images from request  
        images = extract_images_from_request(request_dict)
        trace.images = images
        
        messages_data = extract_conversation_messages(request_dict)
        
        # Update messages with image references if images were found
        if images:
            messages_data = update_messages_with_image_references(messages_data, images)
        
        messages_with_ids = []
        
        for position, msg_data in enumerate(messages_data):
            content_str = str(msg_data.get('content', ''))
            message_id = self._get_or_assign_message_id(
                role=msg_data['role'],
                content=content_str,
                position=position
            )
            
            msg_data['message_id'] = message_id
            
            # Check if this message contains images
            has_images = False
            if isinstance(msg_data.get('content'), list):
                has_images = any(
                    isinstance(item, dict) and item.get('type') == 'image_url'
                    for item in msg_data['content']
                )
            msg_data['has_images'] = has_images
            
            messages_with_ids.append(msg_data)
        
        trace.full_conversation = [Message(**msg) for msg in messages_with_ids]
        
        trace.temperature = request_dict.get('temperature')
        trace.max_tokens = request_dict.get('max_tokens')
        trace.top_p = request_dict.get('top_p')
        trace.frequency_penalty = request_dict.get('frequency_penalty')
        trace.presence_penalty = request_dict.get('presence_penalty')
        trace.seed = request_dict.get('seed')
        trace.stop_sequences = request_dict.get('stop', [])
        trace.logit_bias = request_dict.get('logit_bias')
        
        try:
            original_response = self.wrapped_transport.handle_request(request)
            trace.response_timestamp = datetime.now()
            
            if original_response.status_code >= 400:
                self.stats['failed_requests'] += 1
                
                try:
                    error_content = original_response.read()
                    error_body = error_content.decode('utf-8') if isinstance(error_content, bytes) else str(error_content)
                except Exception:
                    error_body = "Unable to read error response"
                
                trace.error_message = error_body
                trace.error_category = categorize_error(original_response.status_code, error_body)
                trace.success = False
                
                end_time = datetime.now()
                metrics = calculate_performance_metrics(start_time, end_time, 0, 0)
                trace.total_latency_ms = metrics['latency_ms']
                trace.update_completeness()
                
                if self.tracer:
                    self.tracer._on_trace_completed(trace)
                
                return original_response
            
            else:
                self.stats['successful_requests'] += 1
                
                response = LogResponse(
                    status_code=original_response.status_code,
                    headers=original_response.headers,
                    stream=original_response.stream,
                    extensions=original_response.extensions,
                    trace_record=trace,
                    traced_transport=self
                )
                
                logger.info("="*80)
                logger.info("INITIAL TRACE RECORD:")
                logger.info("="*80)
                logger.info(trace.to_json())
                logger.info("="*80)
                
                return response
                
        except Exception as e:
            self.stats['failed_requests'] += 1
            trace.error_message = str(e)
            trace.error_category = type(e).__name__
            trace.success = False
            
            end_time = datetime.now()
            metrics = calculate_performance_metrics(start_time, end_time, 0, 0)
            trace.total_latency_ms = metrics['latency_ms']
            trace.update_completeness()
            
            if self.tracer:
                self.tracer._on_trace_completed(trace)
            
            logger.error("="*80)
            logger.error("ERROR TRACE RECORD:")
            logger.error("="*80)
            logger.error(trace.to_json())
            logger.error("="*80)
            
            raise
    
    def close(self):
        if hasattr(self.wrapped_transport, 'close'):
            self.wrapped_transport.close()