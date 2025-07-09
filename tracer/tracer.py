import openai
import json
from datetime import datetime

class OpenAITracer:
    def __init__(self):
        self.calls = []
        self.initialized = False
        self._original_methods = {}


    def initialize(self):
        if self.initialized:
            return  # Already initialized

        # Store original methods
        self._original_methods['chat_create'] = openai.ChatCompletion.create
        self._original_methods['completion_create'] = openai.Completion.create

        # Patch with traced versions
        openai.ChatCompletion.create = self._trace_chat_completion
        openai.Completion.create = self._trace_completion

        self.initialized = True
        print("OpenAI tracer initialized")

    def _trace_chat_completion(self, *args, **kwargs):
        return self._trace_call('chat_completion', self._original_methods['chat_create'], *args,
                                **kwargs)

    def _trace_completion(self, *args, **kwargs):
        return self._trace_call('completion', self._original_methods['completion_create'], *args,
                                **kwargs)

    def _trace_call(self, call_type, original_func, *args, **kwargs):
        start_time = datetime.now()
        call_data = {
            'type': call_type,
            'timestamp': start_time.isoformat(),
            'model': kwargs.get('model', 'unknown'),
            'messages': kwargs.get('messages', kwargs.get('prompt', 'N/A')),
        }
        try:
            response = original_func(*args, **kwargs)
            call_data['success'] = True
            call_data['response_id'] = getattr(response, 'id', None)
            call_data['tokens_used'] = getattr(response.get('usage', {}), 'total_tokens', 0)
        except Exception as e:
            call_data['success'] = False
            call_data['error'] = str(e)
            raise
        finally:
            call_data['duration'] = (datetime.now() - start_time).total_seconds()
            self.calls.append(call_data)
            print(call_data)
        return response

    def get_stats(self):
        return {
            'total_calls': len(self.calls),
            'successful_calls': sum(1 for c in self.calls if c['success']),
            'total_tokens': sum(c.get('tokens_used', 0) for c in self.calls),
            'average_duration': sum(c['duration'] for c in self.calls) / len(
                self.calls) if self.calls else 0
        }

