"""Persistence adapters for raw article storage."""

from .postgres import PostgresRawStorage, StorageError

__all__ = ["PostgresRawStorage", "StorageError"]
