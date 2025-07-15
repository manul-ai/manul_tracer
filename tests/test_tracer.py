import pytest
from manul_tracer import ManulTracer


def test_tracer_import():
    """Test that we can import ManulTracer"""
    assert ManulTracer is not None


def test_tracer_initialization():
    """Test that ManulTracer can be initialized"""
    tracer = ManulTracer()
    assert tracer is not None
    assert hasattr(tracer, 'get_stats')
    assert hasattr(tracer, 'session_id')
    assert hasattr(tracer, 'session')


def test_tracer_session():
    """Test session handling"""
    # Test auto-generated session_id
    tracer1 = ManulTracer()
    assert tracer1.session_id is not None
    assert isinstance(tracer1.session_id, str)
    
    # Test custom session_id
    custom_id = "test_session_123"
    tracer2 = ManulTracer(session_id=custom_id)
    assert tracer2.session_id == custom_id


def test_tracer_stats():
    """Test that get_stats returns expected structure"""
    tracer = ManulTracer()
    stats = tracer.get_stats()
    
    expected_keys = ['total_requests', 'total_prompt_tokens', 'total_completion_tokens', 'total_tokens', 'successful_requests', 'failed_requests']
    for key in expected_keys:
        assert key in stats
        assert isinstance(stats[key], int)


def test_session_info():
    """Test that get_session_info returns expected structure"""
    tracer = ManulTracer()
    session_info = tracer.get_session_info()
    
    expected_keys = ['session_id', 'session_type', 'created_at', 'last_activity_at', 'total_requests', 'total_tokens', 'successful_requests', 'failed_requests']
    for key in expected_keys:
        assert key in session_info