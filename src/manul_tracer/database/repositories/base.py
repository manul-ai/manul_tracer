"""Base repository interface for database operations."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import duckdb


class BaseRepository(ABC):
    """Abstract base class for repository pattern."""
    TABLE_NAME: str = "base_table"

    def __init__(self, database_filename: str | Path | None = None):
        """Initialize repository with database connection."""
        self.connection = duckdb.connect(
            database=database_filename if database_filename else ":memory:")
        self._ensure_table_exists()

    @abstractmethod
    def _ensure_table_exists(self) -> bool:
        """Create the table if it does not exist (CREATE TABLE self.TABLE_NAME IF NOT EXISTS)."""
        pass

    @abstractmethod
    def create(self, entity: Any) -> Any:
        """Create a new entity in the database."""
        pass

    @abstractmethod
    def read(self, entity_id: str) -> Any | None:
        """Read an entity by ID from the database."""
        pass

    @abstractmethod
    def update(self, entity_id: str, entity: Any) -> Any:
        """Update an existing entity in the database."""
        pass

    @abstractmethod
    def delete(self, entity_id: str) -> bool:
        """Delete an entity from the database."""
        pass

    @abstractmethod
    def list_all(self, filters: dict[str, Any] | None = None) -> list[Any]:
        """List entities with optional filters."""
        pass