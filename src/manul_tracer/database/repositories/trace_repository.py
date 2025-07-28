"""Repository for managing trace records in the database."""
import uuid
import logging
from typing import Any

from .base import BaseRepository
from ...models import TraceRecord, Message, Session, Image

logger = logging.getLogger('manul_tracer.repository')
logger.setLevel(logging.DEBUG)


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
            model_id VARCHAR,
            
            FOREIGN KEY (session_id) REFERENCES sessions(session_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id),
            FOREIGN KEY (model_id) REFERENCES models(model_id),
            
            -- Request Metadata
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
        
        # Users table for user management
        sql_create_users_table = """
        CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR PRIMARY KEY,
            username VARCHAR UNIQUE,
            email VARCHAR UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_active_at TIMESTAMP
        );
        """
        
        # Models table for model management
        sql_create_models_table = """
        CREATE TABLE IF NOT EXISTS models (
            model_id VARCHAR PRIMARY KEY,
            model_name VARCHAR NOT NULL,
            provider VARCHAR NOT NULL DEFAULT 'openai',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(model_name, provider)
        );
        """
        
        # Messages table for conversation tracking (now independent of traces)
        sql_create_messages_table = """
        CREATE TABLE IF NOT EXISTS messages (
            message_id VARCHAR PRIMARY KEY,
            session_id VARCHAR,
            role VARCHAR,
            content TEXT,
            has_images BOOLEAN DEFAULT FALSE,
            message_order INTEGER,
            message_timestamp TIMESTAMP,
            token_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (session_id) REFERENCES sessions(session_id)
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
        
        # Images table for storing image metadata
        sql_create_images_table = """
        CREATE TABLE IF NOT EXISTS images (
            image_id VARCHAR PRIMARY KEY,
            image_hash VARCHAR UNIQUE,
            size_mb DOUBLE,
            format VARCHAR,
            width INTEGER,
            height INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # Junction table for many-to-many relationship between messages and images
        sql_create_message_images_table = """
        CREATE TABLE IF NOT EXISTS message_images (
            message_id VARCHAR,
            image_id VARCHAR,
            image_order INTEGER,
            
            PRIMARY KEY (message_id, image_id),
            FOREIGN KEY (message_id) REFERENCES messages(message_id),
            FOREIGN KEY (image_id) REFERENCES images(image_id)
        );
        """
        
        # Create indexes for better performance
        sql_create_indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_traces_session_id ON {self.TABLE_NAME}(session_id);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_user_id ON {self.TABLE_NAME}(user_id);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_model_id ON {self.TABLE_NAME}(model_id);",
            "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);",
            "CREATE INDEX IF NOT EXISTS idx_models_name_provider ON models(model_name, provider);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_timestamp ON {self.TABLE_NAME}(request_timestamp);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_success ON {self.TABLE_NAME}(success);",
            f"CREATE INDEX IF NOT EXISTS idx_traces_status ON {self.TABLE_NAME}(trace_status);",
            "CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);",
            "CREATE INDEX IF NOT EXISTS idx_messages_role ON messages(role);",
            "CREATE INDEX IF NOT EXISTS idx_trace_messages_trace_id ON trace_messages(trace_id);",
            "CREATE INDEX IF NOT EXISTS idx_trace_messages_message_id ON trace_messages(message_id);",
            "CREATE INDEX IF NOT EXISTS idx_images_hash ON images(image_hash);",
            "CREATE INDEX IF NOT EXISTS idx_message_images_message_id ON message_images(message_id);",
            "CREATE INDEX IF NOT EXISTS idx_message_images_image_id ON message_images(image_id);"
        ]
        
        # Execute table creation statements
        self.connection.execute(sql_create_users_table)
        self.connection.execute(sql_create_models_table)
        self.connection.execute(sql_create_traces_table)
        self.connection.execute(sql_create_messages_table)
        self.connection.execute(sql_create_trace_messages_table)
        self.connection.execute(sql_create_images_table)
        self.connection.execute(sql_create_message_images_table)
        
        # Create indexes
        for index_sql in sql_create_indexes:
            self.connection.execute(index_sql)
            
        return True
    def create_or_get_user(self, user_id: str, username: str | None = None, email: str | None = None) -> str:
        """Create or get a user in the database.
        
        Args:
            user_id: The user ID
            username: Optional username
            email: Optional email
            
        Returns:
            user_id: The ID of the created/existing user
        """
        # Check if user already exists
        existing_user = self.connection.execute(
            "SELECT user_id FROM users WHERE user_id = ?",
            (user_id,)
        ).fetchone()
        
        if existing_user:
            # Update last_active_at
            self.connection.execute(
                "UPDATE users SET last_active_at = CURRENT_TIMESTAMP WHERE user_id = ?",
                (user_id,)
            )
            return existing_user[0]
        
        # Create new user
        sql_insert_user = """
        INSERT INTO users (user_id, username, email, last_active_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        """
        self.connection.execute(sql_insert_user, (user_id, username, email))
        return user_id

    def create_or_get_model(self, model_name: str, provider: str = "openai") -> str:
        """Create or get a model in the database.
        
        Args:
            model_name: The model name (e.g., 'gpt-4', 'gpt-3.5-turbo')
            provider: The provider (e.g., 'openai', 'anthropic')
            
        Returns:
            model_id: The ID of the created/existing model
        """
        # Check if model already exists
        existing_model = self.connection.execute(
            "SELECT model_id FROM models WHERE model_name = ? AND provider = ?",
            (model_name, provider)
        ).fetchone()
        
        if existing_model:
            return existing_model[0]
        
        # Create new model with UUID as model_id
        model_id = str(uuid.uuid4())
        sql_insert_model = """
        INSERT INTO models (model_id, model_name, provider)
        VALUES (?, ?, ?)
        """
        self.connection.execute(sql_insert_model, (model_id, model_name, provider))
        return model_id

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

    def _create_or_get_image(self, image: Image) -> str:
        """Create or get an image in the database using hash-based deduplication.
        
        Args:
            image: Image instance to persist
            
        Returns:
            image_id: The ID of the created/existing image
        """
        if not image.image_hash:
            return image.image_id

        # Check if image with this hash already exists
        existing_image = self.connection.execute(
            "SELECT image_id FROM images WHERE image_hash = ?",
            (image.image_hash,)
        ).fetchone()
        
        if existing_image:
            # Image with same hash exists, return the existing ID
            return existing_image[0]
        
        # Create new image record
        image_record = image.to_dict(skip_none=True)
        
        sql_insert_image = f"""
        INSERT INTO images ({', '.join(image_record.keys())})
        VALUES ({', '.join(['?' for _ in image_record])})
        """
        self.connection.execute(sql_insert_image, tuple(image_record.values()))
        return image.image_id

    def _link_message_to_image(self, message_id: str, image_id: str, image_order: int) -> None:
        """Link a message to an image via the junction table.
        
        Args:
            message_id: The message ID
            image_id: The image ID
            image_order: The order of the image in the message
        """
        sql_insert_junction = """
        INSERT OR IGNORE INTO message_images (message_id, image_id, image_order)
        VALUES (?, ?, ?)
        """
        self.connection.execute(sql_insert_junction, (message_id, image_id, image_order))

    def _process_message_images(self, message_id: str, images: list[Image]) -> None:
        """Process and link images to a message.
        
        Args:
            message_id: The message ID to link images to
            images: List of Image objects to process
        """
        if not images:
            return
            
        for i, image in enumerate(images):
            image_id = self._create_or_get_image(image)
            self._link_message_to_image(message_id, image_id, i)
        
        # Update the message to set has_images = TRUE
        self.connection.execute(
            "UPDATE messages SET has_images = TRUE WHERE message_id = ?",
            (message_id,)
        )

    def create(self, trace: TraceRecord) -> TraceRecord:
        """Create a new trace record in the database.
        
        Args:
            trace: TraceRecord instance to persist
            
        Returns:
            TraceRecord with any database-generated fields populated
        """
        logger.info(f"TraceRepository.create called for trace_id={trace.trace_id}")
        
        trace.trace_id = trace.trace_id or self.generate_trace_id()
        logger.debug(f"  Final trace_id={trace.trace_id}")
        
        record = trace.to_dict(skip_none=True)
        logger.debug(f"  Record keys: {list(record.keys())}")
        
        # Save full_conversation and images before removing them from record
        # These are handled via separate normalized tables
        full_conversation = record.pop('full_conversation', None)
        images_data = record.pop('images', None)
        
        # Convert image dictionaries back to Image objects if needed
        images = None
        if images_data:
            images = []
            for img_data in images_data:
                if isinstance(img_data, dict):
                    images.append(Image.from_dict(img_data))
                elif isinstance(img_data, Image):
                    images.append(img_data)
                    
        logger.debug(f"  Has full_conversation: {full_conversation is not None}")
        logger.debug(f"  Number of messages: {len(trace.full_conversation) if trace.full_conversation else 0}")
        logger.debug(f"  Has images: {images is not None}")
        logger.debug(f"  Number of images: {len(images) if images else 0}")

        sql_insert_trace = f"""
        INSERT INTO {self.TABLE_NAME} ({', '.join(record.keys())})
        VALUES 
        ({', '.join(['?' for _ in record])})
        """
        
        try:
            logger.debug(f"  Executing INSERT for trace")
            self.connection.execute(sql_insert_trace, tuple(record.values()))
            logger.info(f"  Successfully inserted trace {trace.trace_id}")
        except Exception as e:
            logger.error(f"  ERROR inserting trace: {e}")
            raise

        # Create or get messages and link them to this trace
        if full_conversation and trace.session_id:
            logger.debug(f"  Processing {len(trace.full_conversation)} messages")
            for i, message in enumerate(trace.full_conversation):
                try:
                    message_id = self._create_or_get_message(trace.session_id, message)
                    self._link_trace_to_message(trace.trace_id, message_id, i)
                    
                    # Process images for this message if images exist in the trace
                    if images and message.has_images:
                        self._process_message_images(message_id, images)
                    
                    logger.debug(f"    Linked message {i}: {message_id}")
                except Exception as e:
                    logger.error(f"    ERROR processing message {i}: {e}")
                    raise
        
        logger.info(f"  Completed creating trace {trace.trace_id}")
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
                    msg_dict.pop('session_id', None)  # Remove session_id as it's not part of Message model
                    msg_dict.pop('created_at', None)  # Remove created_at as it's not part of Message model
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
        
        # Save full_conversation and images before removing them from record
        # These are handled via separate normalized tables
        full_conversation = record.pop('full_conversation', None)
        images_data = record.pop('images', None)
        
        # Convert image dictionaries back to Image objects if needed
        images = None
        if images_data:
            images = []
            for img_data in images_data:
                if isinstance(img_data, dict):
                    images.append(Image.from_dict(img_data))
                elif isinstance(img_data, Image):
                    images.append(img_data)
        
        # Filter out trace_id from both keys and values for SET clause
        update_data = {k: v for k, v in record.items() if k != 'trace_id'}
        set_clause = ", ".join([f"{key} = ?" for key in update_data.keys()])
        sql_update_trace = f"""
        UPDATE {self.TABLE_NAME} SET
        {set_clause}
        WHERE trace_id = ?;
        """
        self.connection.execute(sql_update_trace, tuple(list(update_data.values()) + [trace_id]))

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
        logger.info(f"TraceRepository.create_or_update called")
        logger.debug(f"  trace_id={trace.trace_id}, session_id={trace.session_id}")
        
        if not trace.trace_id:
            logger.debug(f"  No trace_id, generating new one")
            trace.trace_id = self.generate_trace_id()
            return self.create(trace)
        elif self.read(trace.trace_id) is None:
            logger.debug(f"  Trace {trace.trace_id} not found, creating new")
            return self.create(trace)
        else:
            logger.debug(f"  Trace {trace.trace_id} exists, updating")
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
                    msg_dict.pop('session_id', None)  # Remove session_id as it's not part of Message model
                    msg_dict.pop('created_at', None)  # Remove created_at as it's not part of Message model
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
        where_clause = ""
        params = ()
        
        if filters:
            conditions = " AND ".join([f"{key} = ?" for key in filters.keys()])
            where_clause = f" WHERE {conditions}"
            params = tuple(filters.values())
        
        sql = f"""
        SELECT 
            COUNT(*) as total_traces,
            COUNT(CASE WHEN success = true THEN 1 END) as successful_traces,
            COUNT(CASE WHEN success = false THEN 1 END) as failed_traces,
            SUM(COALESCE(total_tokens, 0)) as total_tokens,
            SUM(COALESCE(prompt_tokens, 0)) as total_prompt_tokens,
            SUM(COALESCE(completion_tokens, 0)) as total_completion_tokens,
            AVG(COALESCE(total_latency_ms, 0)) as avg_latency_ms,
            AVG(COALESCE(tokens_per_second, 0)) as avg_tokens_per_second,
            COUNT(DISTINCT session_id) as unique_sessions,
            COUNT(DISTINCT model_id) as unique_models
        FROM {self.TABLE_NAME}{where_clause}
        """
        
        result = self.connection.execute(sql, params).fetchone()
        
        return {
            'total_traces': result[0] or 0,
            'successful_traces': result[1] or 0,
            'failed_traces': result[2] or 0,
            'total_tokens': result[3] or 0,
            'total_prompt_tokens': result[4] or 0,
            'total_completion_tokens': result[5] or 0,
            'avg_latency_ms': result[6] or 0.0,
            'avg_tokens_per_second': result[7] or 0.0,
            'unique_sessions': result[8] or 0,
            'unique_models': result[9] or 0,
            'success_rate': (result[1] / result[0] * 100.0) if result[0] > 0 else 0.0
        }
    
    def get_token_usage_by_model(self) -> list[dict[str, Any]]:
        """Get token usage statistics grouped by model."""
        sql = f"""
        SELECT 
            m.model_name as model,
            m.provider,
            COUNT(*) as trace_count,
            SUM(COALESCE(t.total_tokens, 0)) as total_tokens,
            SUM(COALESCE(t.prompt_tokens, 0)) as prompt_tokens,
            SUM(COALESCE(t.completion_tokens, 0)) as completion_tokens,
            AVG(COALESCE(t.total_tokens, 0)) as avg_tokens_per_trace
        FROM {self.TABLE_NAME} t
        JOIN models m ON t.model_id = m.model_id
        WHERE t.model_id IS NOT NULL
        GROUP BY m.model_name, m.provider
        ORDER BY total_tokens DESC
        """
        
        df = self.connection.execute(sql).fetchdf()
        return df.to_dict('records') if not df.empty else []
    
    def get_latency_statistics(self) -> dict[str, Any]:
        """Get latency statistics."""
        sql = f"""
        SELECT 
            AVG(COALESCE(total_latency_ms, 0)) as avg_latency,
            MIN(COALESCE(total_latency_ms, 0)) as min_latency,
            MAX(COALESCE(total_latency_ms, 0)) as max_latency,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_latency_ms) as median_latency,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_latency_ms) as p95_latency,
            COUNT(CASE WHEN total_latency_ms > 5000 THEN 1 END) as slow_requests
        FROM {self.TABLE_NAME}
        WHERE total_latency_ms IS NOT NULL AND total_latency_ms > 0
        """
        
        result = self.connection.execute(sql).fetchone()
        if not result or result[0] is None:
            return {
                'avg_latency': 0.0,
                'min_latency': 0.0,
                'max_latency': 0.0,
                'median_latency': 0.0,
                'p95_latency': 0.0,
                'slow_requests': 0
            }
        
        return {
            'avg_latency': float(result[0] or 0),
            'min_latency': float(result[1] or 0),
            'max_latency': float(result[2] or 0),
            'median_latency': float(result[3] or 0),
            'p95_latency': float(result[4] or 0),
            'slow_requests': int(result[5] or 0)
        }
    
    def get_success_rate_by_timeframe(self, hours: int = 24) -> dict[str, Any]:
        """Get success rate for recent timeframe."""
        sql = f"""
        SELECT 
            COUNT(*) as total_requests,
            COUNT(CASE WHEN success = true THEN 1 END) as successful_requests,
            COUNT(CASE WHEN success = false THEN 1 END) as failed_requests
        FROM {self.TABLE_NAME}
        WHERE request_timestamp >= NOW() - INTERVAL '{hours} hours'
        """
        
        result = self.connection.execute(sql).fetchone()
        total = result[0] or 0
        successful = result[1] or 0
        failed = result[2] or 0
        
        return {
            'total_requests': total,
            'successful_requests': successful,
            'failed_requests': failed,
            'success_rate': (successful / total * 100.0) if total > 0 else 0.0,
            'timeframe_hours': hours
        }
    
    def get_traces_by_date_range(self, start_date: str, end_date: str) -> list[TraceRecord]:
        """Get traces within a date range."""
        sql = f"""
        SELECT * FROM {self.TABLE_NAME}
        WHERE request_timestamp >= ? AND request_timestamp <= ?
        ORDER BY request_timestamp DESC
        """
        
        df = self.connection.execute(sql, (start_date, end_date)).fetchdf()
        traces = []
        
        if not df.empty:
            for trace_dict in df.to_dict('records'):
                traces.append(TraceRecord.from_dict(trace_dict))
        
        return traces
    
    def get_daily_usage_trends(self, days: int = 7) -> list[dict[str, Any]]:
        """Get daily usage trends for the past N days."""
        sql = f"""
        SELECT 
            DATE(request_timestamp) as date,
            COUNT(*) as total_requests,
            SUM(COALESCE(total_tokens, 0)) as total_tokens,
            AVG(COALESCE(total_latency_ms, 0)) as avg_latency
        FROM {self.TABLE_NAME}
        WHERE request_timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY DATE(request_timestamp)
        ORDER BY date DESC
        """
        
        df = self.connection.execute(sql).fetchdf()
        return df.to_dict('records') if not df.empty else []
    
    def get_recent_traces(self, limit: int = 10) -> list[TraceRecord]:
        """Get most recent traces."""
        sql = f"""
        SELECT * FROM {self.TABLE_NAME}
        ORDER BY request_timestamp DESC
        LIMIT ?
        """
        
        df = self.connection.execute(sql, (limit,)).fetchdf()
        traces = []
        
        if not df.empty:
            for trace_dict in df.to_dict('records'):
                trace = TraceRecord.from_dict(trace_dict)
                # Load messages for this trace
                sql_messages = """
                SELECT m.*, tm.message_order as trace_message_order
                FROM messages m
                JOIN trace_messages tm ON m.message_id = tm.message_id
                WHERE tm.trace_id = ? 
                ORDER BY tm.message_order
                """
                messages_df = self.connection.execute(sql_messages, (trace.trace_id,)).fetchdf()
                
                if not messages_df.empty:
                    messages = []
                    for msg_dict in messages_df.to_dict('records'):
                        msg_dict.pop('trace_message_order', None)
                        msg_dict.pop('session_id', None)  # Remove session_id as it's not part of Message model
                        msg_dict.pop('created_at', None)  # Remove created_at as it's not part of Message model
                        messages.append(Message.from_dict(msg_dict))
                    trace.full_conversation = messages
                
                traces.append(trace)
        
        return traces
    
    def get_all_users(self) -> list[dict[str, Any]]:
        """Get all users from the database.
        
        Returns:
            List of user dictionaries with user_id, username, email
        """
        sql = """
        SELECT user_id, username, email, created_at, last_active_at
        FROM users
        ORDER BY username
        """
        
        df = self.connection.execute(sql).fetchdf()
        return df.to_dict('records') if not df.empty else []
    
    def get_user_by_id(self, user_id: str) -> dict[str, Any] | None:
        """Get user information by user_id.
        
        Args:
            user_id: The user ID to look up
            
        Returns:
            User dictionary or None if not found
        """
        sql = """
        SELECT user_id, username, email, created_at, last_active_at
        FROM users
        WHERE user_id = ?
        """
        
        df = self.connection.execute(sql, (user_id,)).fetchdf()
        if not df.empty:
            return df.to_dict('records')[0]
        return None