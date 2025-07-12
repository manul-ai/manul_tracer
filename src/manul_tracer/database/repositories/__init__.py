"""Repository pattern implementations for database operations."""

from .base import BaseRepository
from .trace_repository import TraceRepository

__all__ = ['BaseRepository', 'TraceRepository']