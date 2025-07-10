import pytest
from manul_tracer import TracedClient


def test_tracer_import():
    """Test that we can import TracedClient"""
    assert TracedClient is not None


def test_tracer_initialization():
    """Test that TracedClient can be initialized"""
    client = TracedClient()
    assert client is not None
    assert hasattr(client, 'get_stats')


def test_tracer_stats():
    """Test that get_stats returns expected structure"""
    client = TracedClient()
    stats = client.get_stats()
    
    expected_keys = ['total_calls', 'successful_calls', 'total_tokens', 'total_duration', 'average_duration']
    for key in expected_keys:
        assert key in stats
        assert isinstance(stats[key], (int, float))