"""Base repository interface for database operations."""

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict


class BaseRepository(ABC):
    """Abstract base class for repository pattern."""
    
    def __init__(self, connection=None):
        """Initialize repository with database connection."""
        self.connection = connection
    
    @abstractmethod
    def create(self, entity: Any) -> Any:
        """Create a new entity in the database."""
        pass
    
    @abstractmethod
    def read(self, entity_id: str) -> Optional[Any]:
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
    def list(self, filters: Optional[Dict[str, Any]] = None) -> List[Any]:
        """List entities with optional filters."""
        pass