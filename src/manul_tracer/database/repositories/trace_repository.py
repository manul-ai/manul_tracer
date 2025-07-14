"""Repository for managing trace records in the database."""
import uuid

from typing import Any

from .base import BaseRepository
from ...models import TraceRecord, Message, Session


class TraceRepository(BaseRepository):
    """Repository for trace record database operations."""
    TABLE_NAME: str = "traces"

    def __init__(self, database_filename: str | None = None):
        super().__init__(database_filename)

    def _ensure_table_exists(self) -> bool:
        """Ensure the traces and messages tables exist in the database."""
        
        sql_create_traces_table = f"""
        CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
            -- Core Identifiers
            trace_id VARCHAR PRIMARY KEY,
            session_id VARCHAR,
            user_id VARCHAR,
            
            -- Request Metadata
            model VARCHAR,
            provider VARCHAR DEFAULT 'openai',
            endpoint VARCHAR,
            api_version VARCHAR,
            request_timestamp TIMESTAMP,
            response_timestamp TIMESTAMP,
            
            -- API Parameters
            temperature DOUBLE,
            max_tokens INTEGER,
            top_p DOUBLE,
            frequency_penalty DOUBLE,
            presence_penalty DOUBLE,
            stream BOOLEAN,
            stop_sequences JSON,
            logit_bias JSON,
            seed INTEGER,
            
            -- Content Data
            system_prompt TEXT,
            user_prompt TEXT,
            assistant_response TEXT,
            
            -- Response Metadata
            finish_reason VARCHAR,
            choice_index INTEGER,
            response_id VARCHAR,
            
            -- Usage - Aggregate Counts
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            total_tokens INTEGER,
            
            -- Detailed Prompt Token Breakdown
            prompt_cached_tokens INTEGER,
            prompt_audio_tokens INTEGER,
            
            -- Detailed Completion Token Breakdown
            completion_reasoning_tokens INTEGER,
            completion_audio_tokens INTEGER,
            completion_accepted_prediction_tokens INTEGER,
            completion_rejected_prediction_tokens INTEGER,
            
            -- Performance Metrics
            total_latency_ms DOUBLE,
            tokens_per_second DOUBLE,
            processing_time_ms DOUBLE,
            
            -- Error Handling
            success BOOLEAN DEFAULT TRUE,
            error_code VARCHAR,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            error_category VARCHAR,
            
            -- Rate Limiting & Quotas
            rate_limit_remaining INTEGER,
            rate_limit_reset TIMESTAMP,
            quota_consumed DOUBLE,
            
            -- Data Completeness Tracking
            data_completeness_score DOUBLE,
            missing_fields JSON,
            trace_status VARCHAR DEFAULT 'pending',
            
            -- Technical Details
            request_size_bytes INTEGER,
            response_size_bytes INTEGER,
            
            -- Timestamps for Lifecycle
            trace_created_at TIMESTAMP,
            trace_updated_at TIMESTAMP,
            trace_completed_at TIMESTAMP
        );
        """
        
        # Messages table for conversation tracking
        sql_create_messages_table = """
        CREATE TABLE IF NOT EXISTS messages (
            message_id VARCHAR PRIMARY KEY,
            trace_id VARCHAR,
            role VARCHAR,
            content TEXT,
            message_order INTEGER,
            message_timestamp TIMESTAMP,
            token_count INTEGER,
            
            FOREIGN KEY (trace_id) REFERENCES traces(trace_id)
        );
        """
        
        # Create indexes for better performance
        sql_create_indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_traces_session_id ON {self.TABLE_NAME}(session_id);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_user_id ON {self.TABLE_NAME}(user_id);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_model ON {self.TABLE_NAME}(model);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_timestamp ON {self.TABLE_NAME}(request_timestamp);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_success ON {self.TABLE_NAME}(success);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_status ON {self.TABLE_NAME}(trace_status);",
            "CREATE INDEX IF NOT EXISTS idx_messages_trace_id ON messages(trace_id);",
            "CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);"
        ]
        
        # Execute table creation statements
        self.connection.execute(sql_create_traces_table)
        self.connection.execute(sql_create_messages_table)
        
        # Create indexes
        for index_sql in sql_create_indexes:
            self.connection.execute(index_sql)
            
        return True

    def generate_trace_id(self) -> str:
        """Generate a unique trace ID."""
        return str(uuid.uuid4())
    
    def _create_message(self, trace_id: str, message: Message) -> None:
        """Create a message in the database.
        
        Args:
            trace_id: The trace ID this message belongs to
            message: Message instance to persist
        """
        message.message_id = message.message_id or str(uuid.uuid4())
        message_record = message.to_dict(skip_none=True)
        message_record['message_id'] = message_record.get('message_id') or str(uuid.uuid4())
        message_record['trace_id'] = trace_id

        sql_insert_message = f"""
        INSERT INTO messages ({', '.join(message_record.keys())})
        VALUES ({', '.join(['?' for _ in message_record])})
        """
        self.connection.execute(sql_insert_message, tuple(message_record.values()))

    def create(self, trace: TraceRecord) -> TraceRecord:
        """Create a new trace record in the database.
        
        Args:
            trace: TraceRecord instance to persist
            
        Returns:
            TraceRecord with any database-generated fields populated
        """
        trace.trace_id = trace.trace_id or self.generate_trace_id()
        record = trace.to_dict(skip_none=True)

        sql_insert_trace = f"""
        INSERT INTO {self.TABLE_NAME} ({', '.join(record.keys())})
        VALUES 
        ({', '.join(['?' for _ in record])})
        """
        self.connection.execute(sql_insert_trace, tuple(record.values()))

        if trace.full_conversation:
            for message in trace.full_conversation:
                self._create_message(record['trace_id'], message)
        return trace

    def read(self, trace_id: str) -> TraceRecord | None:
        """Read a trace record by ID."""
        sql_select_trace = f"""
        SELECT * FROM {self.TABLE_NAME} WHERE trace_id = ?;
        """
        result = self.connection.execute(sql_select_trace, (trace_id,)).fetchone()
        if result:
            trace_record = TraceRecord.from_dict(dict(result))

            sql_select_messages = """
            SELECT * FROM messages WHERE trace_id = ? ORDER BY message_order;
            """
            messages = self.connection.execute(sql_select_messages, (trace_id,)).fetchall()
            trace_record.full_conversation = [Message.from_dict(dict(msg)) for msg in messages]
            return trace_record

    def check_messages_table_exists(self, message_id: str) -> bool:
        """Check if a message exists in the messages table."""
        sql_check_message = """
        SELECT COUNT(*) FROM messages WHERE message_id = ?;
        """
        result = self.connection.execute(sql_check_message, (message_id,)).fetchone()
        return result[0] > 0
    
    def update(self, trace_id: str, trace: TraceRecord) -> TraceRecord:
        """Update an existing trace record.
        
        Args:
            trace_id: Unique identifier of the trace
            trace: Updated TraceRecord instance
            
        Returns:
            Updated TraceRecord
        """
        record = trace.to_dict(skip_none=True)
        set_clause = ", ".join([f"{key} = ?" for key in record.keys() if key != 'trace_id'])
        sql_update_trace = f"""
        UPDATE {self.TABLE_NAME} SET
        {set_clause}
        WHERE trace_id = ?;
        """
        self.connection.execute(sql_update_trace, tuple(list(record.values()) + [trace_id]))

        if trace.full_conversation:
            for message in trace.full_conversation:
                if self.check_messages_table_exists(message.message_id):
                    message_record = message.to_dict(skip_none=True)
                    message_record['trace_id'] = trace_id
                    message_set_clause = ", ".join([f"{key} = ?" for key in message_record.keys() if key != 'message_id'])
                    sql_update_message = f"""
                    UPDATE messages SET
                    {message_set_clause}
                    WHERE message_id = ?;
                    """
                    self.connection.execute(sql_update_message, tuple(list(message_record.values()) + [message.message_id]))
                else:
                    self._create_message(trace_id, message)
        return trace
    
    def delete(self, trace_id: str) -> bool:
        """Delete a trace record."""
        sql_delete_trace = f"""
        DELETE FROM {self.TABLE_NAME} WHERE trace_id = ?;
        """
        result = self.connection.execute(sql_delete_trace, (trace_id,))

        if result.rowcount > 0:
            sql_delete_messages = """
            DELETE FROM messages WHERE trace_id = ?;
            """
            self.connection.execute(sql_delete_messages, (trace_id,))
            return True
        return False

    def create_or_update(self, trace: TraceRecord) -> TraceRecord:
        if not trace.trace_id:
            trace.trace_id = self.generate_trace_id()
            return self.create(trace)
        elif self.read(trace.trace_id) is None:
            return self.create(trace)
        else:
            return self.update(trace.trace_id, trace)


    def list_all(self, filters: dict[str, Any] | None = None) -> list[TraceRecord]:
        """List trace records with optional filters.
        
        Args:
            filters: Optional dictionary of filter criteria
                - session_id: Filter by session
                - user_id: Filter by user
                - model: Filter by model name
                - start_time: Filter by minimum timestamp
                - end_time: Filter by maximum timestamp
                - success: Filter by success status
                
        Returns:
            List of TraceRecord instances matching filters
        """
        # TODO: Implement DuckDB query logic with filters
        raise NotImplementedError("Database integration pending")
    
    def get_by_session(self, session_id: str) -> list[TraceRecord]:
        """Get all traces for a specific session.
        
        Args:
            session_id: Session identifier
            
        Returns:
            List of TraceRecord instances for the session
        """
        return self.list_all(filters={'session_id': session_id})
    
    def get_by_user(self, user_id: str) -> list[TraceRecord]:
        """Get all traces for a specific user.
        
        Args:
            user_id: User identifier
            
        Returns:
            List of TraceRecord instances for the user
        """
        return self.list_all(filters={'user_id': user_id})
    
    def get_statistics(self, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        """Get aggregated statistics for traces.
        
        Args:
            filters: Optional filters to apply before aggregation
            
        Returns:
            Dictionary with statistics like total_tokens, average_latency, etc.
        """
        # TODO: Implement aggregation queries in DuckDB
        raise NotImplementedError("Database integration pending")