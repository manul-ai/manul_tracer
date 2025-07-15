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
        
        # Messages table for conversation tracking (now independent of traces)
        sql_create_messages_table = """
        CREATE TABLE IF NOT EXISTS messages (
            message_id VARCHAR PRIMARY KEY,
            session_id VARCHAR,
            role VARCHAR,
            content TEXT,
            message_order INTEGER,
            message_timestamp TIMESTAMP,
            token_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Junction table for many-to-many relationship between traces and messages
        sql_create_trace_messages_table = """
        CREATE TABLE IF NOT EXISTS trace_messages (
            trace_id VARCHAR,
            message_id VARCHAR,
            message_order INTEGER,
            
            PRIMARY KEY (trace_id, message_id),
            FOREIGN KEY (trace_id) REFERENCES traces(trace_id),
            FOREIGN KEY (message_id) REFERENCES messages(message_id)
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
            "CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);",
            "CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);",
            "CREATE INDEX IF NOT EXISTS idx_trace_messages_trace_id ON trace_messages(trace_id);",
            "CREATE INDEX IF NOT EXISTS idx_trace_messages_message_id ON trace_messages(message_id);"
        ]
        
        # Execute table creation statements
        self.connection.execute(sql_create_traces_table)
        self.connection.execute(sql_create_messages_table)
        self.connection.execute(sql_create_trace_messages_table)
        
        # Create indexes
        for index_sql in sql_create_indexes:
            self.connection.execute(index_sql)
            
        return True

    def generate_trace_id(self) -> str:
        """Generate a unique trace ID."""
        return str(uuid.uuid4())
    
    def _create_or_get_message(self, session_id: str, message: Message) -> str:
        """Create or get a message in the database.
        
        Args:
            session_id: The session ID this message belongs to
            message: Message instance to persist (should have stable message_id from tracer)
            
        Returns:
            message_id: The ID of the created/existing message
        """
        # Generate message_id if not provided (fallback)
        if not message.message_id:
            message.message_id = str(uuid.uuid4())
        
        # Check if message already exists in this session
        existing_message = self.connection.execute(
            "SELECT message_id FROM messages WHERE message_id = ? AND session_id = ?",
            (message.message_id, session_id)
        ).fetchone()
        
        if existing_message:
            # Message exists, return the existing ID
            return existing_message[0]
        
        # Create new message with stable ID
        message_record = message.to_dict(skip_none=True)
        message_record['session_id'] = session_id
        message_record['message_id'] = message.message_id
        
        sql_insert_message = f"""
        INSERT INTO messages ({', '.join(message_record.keys())})
        VALUES ({', '.join(['?' for _ in message_record])})
        """
        self.connection.execute(sql_insert_message, tuple(message_record.values()))
        return message.message_id

    def _link_trace_to_message(self, trace_id: str, message_id: str, message_order: int) -> None:
        """Link a trace to a message via the junction table.
        
        Args:
            trace_id: The trace ID
            message_id: The message ID
            message_order: The order of the message in the trace
        """
        sql_insert_junction = """
        INSERT OR IGNORE INTO trace_messages (trace_id, message_id, message_order)
        VALUES (?, ?, ?)
        """
        self.connection.execute(sql_insert_junction, (trace_id, message_id, message_order))

    def create(self, trace: TraceRecord) -> TraceRecord:
        """Create a new trace record in the database.
        
        Args:
            trace: TraceRecord instance to persist
            
        Returns:
            TraceRecord with any database-generated fields populated
        """
        trace.trace_id = trace.trace_id or self.generate_trace_id()
        record = trace.to_dict(skip_none=True)
        
        # Save full_conversation before removing it from record
        full_conversation = record.pop('full_conversation', None)

        sql_insert_trace = f"""
        INSERT INTO {self.TABLE_NAME} ({', '.join(record.keys())})
        VALUES 
        ({', '.join(['?' for _ in record])})
        """
        self.connection.execute(sql_insert_trace, tuple(record.values()))

        # Create or get messages and link them to this trace
        if full_conversation and trace.session_id:
            for i, message in enumerate(trace.full_conversation):
                message_id = self._create_or_get_message(trace.session_id, message)
                self._link_trace_to_message(trace.trace_id, message_id, i)
        
        return trace

    def read(self, trace_id: str) -> TraceRecord | None:
        """Read a trace record by ID."""
        sql_select_trace = f"""
        SELECT * FROM {self.TABLE_NAME} WHERE trace_id = ?;
        """
        df = self.connection.execute(sql_select_trace, (trace_id,)).fetchdf()
        if not df.empty:
            trace_dict = df.to_dict('records')[0]  # Get first (and only) row as dict
            trace_record = TraceRecord.from_dict(trace_dict)

            # Get messages for this trace via junction table
            sql_select_messages = """
            SELECT m.*, tm.message_order as trace_message_order
            FROM messages m
            JOIN trace_messages tm ON m.message_id = tm.message_id
            WHERE tm.trace_id = ? 
            ORDER BY tm.message_order;
            """
            messages_df = self.connection.execute(sql_select_messages, (trace_id,)).fetchdf()
            
            # Convert messages DataFrame to list of Message objects
            messages = []
            if not messages_df.empty:
                for msg_dict in messages_df.to_dict('records'):
                    # Remove the junction table field before creating Message
                    msg_dict.pop('trace_message_order', None)
                    messages.append(Message.from_dict(msg_dict))
            
            trace_record.full_conversation = messages
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
        
        # Save full_conversation before removing it from record
        full_conversation = record.pop('full_conversation', None)
        
        set_clause = ", ".join([f"{key} = ?" for key in record.keys() if key != 'trace_id'])
        sql_update_trace = f"""
        UPDATE {self.TABLE_NAME} SET
        {set_clause}
        WHERE trace_id = ?;
        """
        self.connection.execute(sql_update_trace, tuple(list(record.values()) + [trace_id]))

        # Update messages and junction table relationships
        if trace.full_conversation and trace.session_id:
            # Remove existing trace-message relationships
            self.connection.execute("DELETE FROM trace_messages WHERE trace_id = ?", (trace_id,))
            
            # Create or get messages and link them to this trace
            for i, message in enumerate(trace.full_conversation):
                message_id = self._create_or_get_message(trace.session_id, message)
                self._link_trace_to_message(trace_id, message_id, i)
        
        return trace
    
    def delete(self, trace_id: str) -> bool:
        """Delete a trace record."""
        # First delete junction table entries
        self.connection.execute("DELETE FROM trace_messages WHERE trace_id = ?", (trace_id,))
        
        # Then delete the trace record
        sql_delete_trace = f"""
        DELETE FROM {self.TABLE_NAME} WHERE trace_id = ?;
        """
        result = self.connection.execute(sql_delete_trace, (trace_id,))

        # Note: Messages are NOT deleted as they belong to the session and may be referenced by other traces
        return result.rowcount > 0

    def create_or_update(self, trace: TraceRecord) -> TraceRecord:
        if not trace.trace_id:
            trace.trace_id = self.generate_trace_id()
            return self.create(trace)
        elif self.read(trace.trace_id) is None:
            return self.create(trace)
        else:
            return self.update(trace.trace_id, trace)


    def list_all(self, filters: dict[str, Any] | None = None) -> list[TraceRecord]:
        """List trace records with optional filters."""
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
        traces = []
        
        if not df.empty:
            for trace_dict in df.to_dict('records'):
                traces.append(TraceRecord.from_dict(trace_dict))

        # Load messages for each trace via junction table
        for trace in traces:
            sql_select_messages = """
            SELECT m.*, tm.message_order as trace_message_order
            FROM messages m
            JOIN trace_messages tm ON m.message_id = tm.message_id
            WHERE tm.trace_id = ? 
            ORDER BY tm.message_order;
            """
            messages_df = self.connection.execute(sql_select_messages, (trace.trace_id,)).fetchdf()
            
            messages = []
            if not messages_df.empty:
                for msg_dict in messages_df.to_dict('records'):
                    # Remove the junction table field before creating Message
                    msg_dict.pop('trace_message_order', None)
                    messages.append(Message.from_dict(msg_dict))
            
            trace.full_conversation = messages

        return traces
    
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