"""
Base database models for the blockchain decoder.

This module provides the base classes for all database models:
- SQLAlchemy base class
- Common model mixins
- Shared model utilities
- Type definitions
- Migration support

TODO: Implement Base and related model utilities
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData

# Use naming convention for constraints to work well with migrations
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

metadata = MetaData(naming_convention=convention)
Base = declarative_base(metadata=metadata)