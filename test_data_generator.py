#!/usr/bin/env python3
"""Generate test data for Manul Tracer visualization testing."""

import os
import sys
from datetime import datetime, timedelta
import uuid

# Add the source directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from manul_tracer.models import TraceRecord, Message, Session
from manul_tracer.database.repositories.trace_repository import TraceRepository
from manul_tracer.database.repositories.session_repository import SessionRepository

def generate_test_data(db_path: str = "test_traces.db"):
    """Generate sample trace data for testing."""
    print(f"Creating test database: {db_path}")
    
    # Initialize repositories
    trace_repo = TraceRepository(db_path)
    session_repo = SessionRepository(db_path)
    
    # Create test sessions
    sessions = []
    for i in range(3):
        session = Session(
            session_id=f"test-session-{i+1}",
            session_type="test",
            created_at=datetime.now() - timedelta(days=i),
            last_activity_at=datetime.now() - timedelta(hours=i)
        )
        sessions.append(session)
        session_repo.create_or_update(session)
        print(f"Created session: {session.session_id}")
    
    # Generate test traces
    models = ["gpt-4", "gpt-3.5-turbo", "gpt-4o"]
    
    for session in sessions:
        for i in range(5):  # 5 traces per session
            # Create messages
            messages = [
                Message(
                    message_id=str(uuid.uuid4()),
                    role="user",
                    content=f"Test user message {i+1} for session {session.session_id}",
                    message_order=0,
                    message_timestamp=datetime.now() - timedelta(minutes=i*10)
                ),
                Message(
                    message_id=str(uuid.uuid4()),
                    role="assistant", 
                    content=f"Test assistant response {i+1} for session {session.session_id}",
                    message_order=1,
                    token_count=50 + i*10,
                    message_timestamp=datetime.now() - timedelta(minutes=i*10-1)
                )
            ]
            
            # Create trace
            trace = TraceRecord(
                trace_id=f"test-trace-{session.session_id}-{i+1}",
                session_id=session.session_id,
                model=models[i % len(models)],
                request_timestamp=datetime.now() - timedelta(minutes=i*10),
                response_timestamp=datetime.now() - timedelta(minutes=i*10-2),
                temperature=0.7,
                max_tokens=1000,
                prompt_tokens=20 + i*5,
                completion_tokens=50 + i*10,
                total_tokens=70 + i*15,
                total_latency_ms=1000 + i*200,
                tokens_per_second=15.5 + i*2,
                success=True if i % 4 != 0 else False,  # Make some failures
                error_message="Test error message" if i % 4 == 0 else None,
                error_category="TestError" if i % 4 == 0 else None,
                full_conversation=messages
            )
            
            trace_repo.create_or_update(trace)
            print(f"Created trace: {trace.trace_id}")
    
    print(f"\nTest data generation complete!")
    print(f"Database created at: {os.path.abspath(db_path)}")
    print(f"\nTo start the visualization app:")
    print(f"manul-tracer --database {os.path.abspath(db_path)} --port 8501")

if __name__ == "__main__":
    generate_test_data()