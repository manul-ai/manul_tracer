"""Repository for managing session records in the database."""
from typing import Any

from .base import BaseRepository
from ...models import Session


class SessionRepository(BaseRepository):
    """Repository for session record database operations."""
    TABLE_NAME: str = "sessions"

    def __init__(self, database_filename: str | None = None):
        super().__init__(database_filename)

    def _ensure_table_exists(self) -> bool:
        """Ensure the sessions table exists in the database."""
        
        # First ensure users table exists (dependency)
        sql_create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR PRIMARY KEY,
            username VARCHAR,
            email VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active_at TIMESTAMP
        );
        """
        
        sql_create_sessions_table = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            -- Core Identifiers
            session_id VARCHAR PRIMARY KEY,
            user_id VARCHAR,
            session_type VARCHAR DEFAULT 'tracer',
            
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            
            -- Session Metadata
            session_name VARCHAR,
            session_description TEXT,
            
            -- Timestamps
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            last_activity_at TIMESTAMP,
            
            -- Session Statistics
            total_requests INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            total_cost DOUBLE DEFAULT 0.0,
            
            -- Session State
            is_active BOOLEAN DEFAULT TRUE,
            session_data JSON
        );
        """
        
        # Create indexes for better performance
        sql_create_indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON {self.TABLE_NAME}(user_id);",
            f"CREATE INDEX IF NOT EXISTS idx_sessions_type ON {self.TABLE_NAME}(session_type);",
            f"CREATE INDEX IF NOT EXISTS idx_sessions_created_at ON {self.TABLE_NAME}(created_at);",
            f"CREATE INDEX IF NOT EXISTS idx_sessions_is_active ON {self.TABLE_NAME}(is_active);",
        ]
        
        # Execute table creation statements in correct order
        self.connection.execute(sql_create_users_table)
        self.connection.execute(sql_create_sessions_table)
        
        # Create indexes
        for index_sql in sql_create_indexes:
            self.connection.execute(index_sql)
            
        return True

    def create(self, session: Session) -> Session:
        """Create a new session record in the database.
        
        Args:
            session: Session instance to persist (must have session_id)
            
        Returns:
            Session with any database-generated fields populated
        """
        record = session.to_dict(skip_none=True)

        sql_insert_session = f"""
        INSERT INTO {self.TABLE_NAME} ({', '.join(record.keys())})
        VALUES ({', '.join(['?' for _ in record])})
        """
        self.connection.execute(sql_insert_session, tuple(record.values()))
        
        return session

    def read(self, session_id: str) -> Session | None:
        """Read a session record by ID."""
        sql_select_session = f"""
        SELECT * FROM {self.TABLE_NAME} WHERE session_id = ?;
        """
        df = self.connection.execute(sql_select_session, (session_id,)).fetchdf()
        if not df.empty:
            session_dict = df.to_dict('records')[0]
            # Keep only fields that exist in Session model
            valid_fields = {
                'session_id', 'user_id', 'session_name', 'session_type',
                'created_at', 'started_at', 'ended_at', 'last_activity_at'
            }
            filtered_dict = {k: v for k, v in session_dict.items() if k in valid_fields}
            return Session.from_dict(filtered_dict)
        return None

    def update(self, session_id: str, session: Session) -> Session:
        """Update an existing session record.
        
        Args:
            session_id: Unique identifier of the session
            session: Updated Session instance
            
        Returns:
            Updated Session
        """
        record = session.to_dict(skip_none=True)
        
        set_clause = ", ".join([f"{key} = ?" for key in record.keys() if key != 'session_id'])
        sql_update_session = f"""
        UPDATE {self.TABLE_NAME} SET
        {set_clause}
        WHERE session_id = ?;
        """
        self.connection.execute(sql_update_session, tuple(list(record.values()) + [session_id]))
        
        return session

    def delete(self, session_id: str) -> bool:
        """Delete a session record."""
        sql_delete_session = f"""
        DELETE FROM {self.TABLE_NAME} WHERE session_id = ?;
        """
        result = self.connection.execute(sql_delete_session, (session_id,))
        return result.rowcount > 0

    def create_or_update(self, session: Session) -> Session:
        """Create or update a session record based on session_id existence."""
        if self.read(session.session_id) is None:
            return self.create(session)
        else:
            return self.update(session.session_id, session)

    def list_all(self, filters: dict[str, Any] | None = None) -> list[Session]:
        """List session records with optional filters."""
        sql_select = f"""
        SELECT * FROM {self.TABLE_NAME}
        """
        if filters:
            conditions = " AND ".join([f"{key} = ?" for key in filters.keys()])
            sql_select += f" WHERE {conditions}"
            params = tuple(filters.values())
        else:
            params = ()

        df = self.connection.execute(sql_select, params).fetchdf()
        sessions = []
        
        if not df.empty:
            for session_dict in df.to_dict('records'):
                # Keep only fields that exist in Session model
                valid_fields = {
                    'session_id', 'user_id', 'session_name', 'session_type',
                    'created_at', 'started_at', 'ended_at', 'last_activity_at'
                }
                filtered_dict = {k: v for k, v in session_dict.items() if k in valid_fields}
                sessions.append(Session.from_dict(filtered_dict))

        return sessions

    def get_by_user(self, user_id: str) -> list[Session]:
        """Get all sessions for a specific user.
        
        Args:
            user_id: User identifier
            
        Returns:
            List of Session instances for the user
        """
        return self.list_all(filters={'user_id': user_id})

    def get_active_sessions(self) -> list[Session]:
        """Get all active sessions.
        
        Returns:
            List of active Session instances
        """
        return self.list_all(filters={'is_active': True})

    def end_session(self, session_id: str) -> bool:
        """End a session by setting ended_at timestamp and is_active to False.
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if session was ended successfully
        """
        sql_end_session = f"""
        UPDATE {self.TABLE_NAME} SET
        ended_at = CURRENT_TIMESTAMP,
        is_active = FALSE
        WHERE session_id = ?;
        """
        result = self.connection.execute(sql_end_session, (session_id,))
        return result.rowcount > 0

    def update_activity(self, session_id: str) -> bool:
        """Update the last activity timestamp for a session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            True if activity was updated successfully
        """
        sql_update_activity = f"""
        UPDATE {self.TABLE_NAME} SET
        last_activity_at = CURRENT_TIMESTAMP
        WHERE session_id = ?;
        """
        result = self.connection.execute(sql_update_activity, (session_id,))
        return result.rowcount > 0

    def update_statistics(self, session_id: str, requests: int = 0, tokens: int = 0, cost: float = 0.0) -> bool:
        """Update session statistics.
        
        Args:
            session_id: Session identifier
            requests: Number of requests to add
            tokens: Number of tokens to add
            cost: Cost to add
            
        Returns:
            True if statistics were updated successfully
        """
        sql_update_stats = f"""
        UPDATE {self.TABLE_NAME} SET
        total_requests = total_requests + ?,
        total_tokens = total_tokens + ?,
        total_cost = total_cost + ?,
        last_activity_at = CURRENT_TIMESTAMP
        WHERE session_id = ?;
        """
        result = self.connection.execute(sql_update_stats, (requests, tokens, cost, session_id))
        return result.rowcount > 0