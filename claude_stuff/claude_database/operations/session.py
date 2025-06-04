"""
Database session management for the blockchain decoder.

This module handles database connection and session management:
- Connection pooling
- Session lifecycle
- Transaction boundaries
- Context managers
- Connection health checks

TODO: Implement the ConnectionManager and Session classes
"""

from contextlib import contextmanager
from sqlalchemy.orm import Session

from ...env import env
from ..models.base import Base

class ConnectionManager:
    def __init__(self, database_url=None):
        """
        Initialize database connection manager.
        
        Args:
            database_url: Optional database URL (defaults to env.get_db_url())
        """
        if database_url:
            # Create a new engine if URL is provided
            from sqlalchemy import create_engine
            self.engine = create_engine(database_url)
            self._initialize_tables()
        else:
            # Use the engine from environment
            self.engine = env.get_db_engine()
    
    def _initialize_tables(self):
        """Initialize database tables."""
        Base.metadata.create_all(self.engine)
    
    @contextmanager
    def get_session(self):
        """Get a database session."""
        session = Session(self.engine)
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()