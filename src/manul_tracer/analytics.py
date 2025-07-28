"""Analytics service for combining and analyzing trace data."""

from typing import Any
from datetime import datetime, timedelta

from .database.repositories.trace_repository import TraceRepository
from .database.repositories.session_repository import SessionRepository


class AnalyticsService:
    """Service for complex analytics across trace and session data."""
    
    def __init__(self, database_file: str | None = None):
        self.session_repo = SessionRepository(database_file)
        self.trace_repo = TraceRepository(database_file)
    
    def get_dashboard_overview(self) -> dict[str, Any]:
        """Get data for dashboard overview page."""
        stats = self.trace_repo.get_statistics()
        latency_stats = self.trace_repo.get_latency_statistics()
        recent_traces = self.trace_repo.get_recent_traces(limit=5)
        
        # Get session count
        sessions = self.session_repo.list_all()
        active_sessions = [s for s in sessions if s.ended_at is None]
        
        return {
            'overview_stats': stats,
            'latency_stats': latency_stats,
            'recent_traces': [
                {
                    'trace_id': t.trace_id,
                    'model_id': t.model_id,
                    'total_tokens': t.total_tokens or 0,
                    'latency_ms': t.total_latency_ms or 0,
                    'success': t.success,
                    'timestamp': t.request_timestamp.isoformat() if t.request_timestamp else None
                }
                for t in recent_traces
            ],
            'session_counts': {
                'total_sessions': len(sessions),
                'active_sessions': len(active_sessions)
            }
        }
    
    def get_performance_data(self) -> dict[str, Any]:
        """Get data for performance analysis page."""
        # Get token usage by model
        token_usage = self.trace_repo.get_token_usage_by_model()
        
        # Get daily trends
        daily_trends = self.trace_repo.get_daily_usage_trends(days=7)
        
        # Get latency stats
        latency_stats = self.trace_repo.get_latency_statistics()
        
        return {
            'token_usage_by_model': token_usage,
            'daily_trends': daily_trends,
            'latency_distribution': latency_stats
        }
    
    def get_session_analytics(self) -> dict[str, Any]:
        """Get session-level analytics."""
        sessions = self.session_repo.list_all()
        
        # Get all users for lookup
        users = {u['user_id']: u for u in self.trace_repo.get_all_users()}
        
        # Calculate session durations
        session_data = []
        for session in sessions:
            duration = None
            if session.created_at and session.ended_at:
                duration = (session.ended_at - session.created_at).total_seconds()
            elif session.created_at and session.last_activity_at:
                duration = (session.last_activity_at - session.created_at).total_seconds()
            
            # Get traces for this session
            traces = self.trace_repo.get_by_session(session.session_id)
            
            # Get user info
            user_info = users.get(session.user_id) if session.user_id else None
            
            session_data.append({
                'session_id': session.session_id,
                'user_id': session.user_id,
                'username': user_info['username'] if user_info else None,
                'session_type': session.session_type,
                'created_at': session.created_at.isoformat() if session.created_at else None,
                'ended_at': session.ended_at.isoformat() if session.ended_at else None,
                'duration_seconds': duration,
                'trace_count': len(traces),
                'total_tokens': sum(t.total_tokens or 0 for t in traces),
                'success_rate': (
                    sum(1 for t in traces if t.success) / len(traces) * 100.0
                    if traces else 0.0
                )
            })
        
        return {
            'sessions': session_data,
            'summary': {
                'total_sessions': len(sessions),
                'avg_duration': sum(s['duration_seconds'] or 0 for s in session_data) / len(session_data) if session_data else 0,
                'avg_traces_per_session': sum(s['trace_count'] for s in session_data) / len(session_data) if session_data else 0
            }
        }
    
    def search_traces(self, 
                     session_id: str | None = None, 
                     model: str | None = None,
                     success: bool | None = None,
                     hours_back: int | None = None) -> list[dict[str, Any]]:
        """Search traces with filters."""
        filters = {}
        if session_id:
            filters['session_id'] = session_id
        if model:
            filters['model_id'] = model
        if success is not None:
            filters['success'] = success
        
        traces = self.trace_repo.list_all(filters)
        
        # Filter by time if specified
        if hours_back:
            cutoff = datetime.now() - timedelta(hours=hours_back)
            traces = [t for t in traces if t.request_timestamp and t.request_timestamp >= cutoff]
        
        # Get all users for lookup
        users = {u['user_id']: u for u in self.trace_repo.get_all_users()}
        
        # Convert to simple dict format for UI
        return [
            {
                'trace_id': t.trace_id,
                'session_id': t.session_id,
                'user_id': t.user_id,
                'username': users.get(t.user_id, {}).get('username') if t.user_id else None,
                'model_id': t.model_id,
                'total_tokens': t.total_tokens or 0,
                'latency_ms': t.total_latency_ms or 0,
                'success': t.success,
                'error_message': t.error_message,
                'timestamp': t.request_timestamp.isoformat() if t.request_timestamp else None,
                'conversation_length': len(t.full_conversation) if t.full_conversation else 0
            }
            for t in traces
        ]
    
    def get_error_analysis(self) -> dict[str, Any]:
        """Analyze error patterns."""
        failed_traces = self.trace_repo.list_all(filters={'success': False})
        
        # Group by error category
        error_categories = {}
        for trace in failed_traces:
            category = trace.error_category or 'Unknown'
            if category not in error_categories:
                error_categories[category] = []
            error_categories[category].append({
                'trace_id': trace.trace_id,
                'error_message': trace.error_message,
                'model_id': trace.model_id,
                'timestamp': trace.request_timestamp.isoformat() if trace.request_timestamp else None
            })
        
        return {
            'error_summary': {
                'total_failed_traces': len(failed_traces),
                'error_categories': {cat: len(traces) for cat, traces in error_categories.items()}
            },
            'error_details': error_categories
        }