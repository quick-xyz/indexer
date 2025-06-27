# indexer/database/connection.py

from typing import Generator, Optional
from contextlib import contextmanager
import logging

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.pool import QueuePool

from ..core.logging_config import IndexerLogger, log_with_context
from ..types.config import DatabaseConfig


class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        if not config:
            raise ValueError("DatabaseConfig is required")
        
        self.config = config
        self.logger = IndexerLogger.get_logger('database.manager')
        
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None
        self._scoped_session: Optional[scoped_session] = None
        
        log_with_context(self.logger, logging.INFO, "DatabaseManager initialized",
                        db_url_host=self._extract_host_from_url(config.url))
    
    def _extract_host_from_url(self, url: str) -> str:
        try:
            if '@' in url and '/' in url:
                after_at = url.split('@')[1]
                host_part = after_at.split('/')[0]
                return host_part
            return "unknown"
        except Exception:
            return "unknown"
    
    def initialize(self) -> None:
        if self._engine is not None:
            self.logger.warning("Database already initialized")
            return
        
        try:
            self.logger.info("Initializing database engine")
            
            self._engine = create_engine(
                self.config.url,
                poolclass=QueuePool,
                pool_size=getattr(self.config, 'pool_size', 5),
                max_overflow=getattr(self.config, 'max_overflow', 10),
                pool_timeout=30,
                pool_recycle=3600,  # Recycle connections after 1 hour
                echo=False,  # Set to True for SQL debugging
            )
            
            self._session_factory = sessionmaker(
                bind=self._engine,
                expire_on_commit=False,  # Keep objects accessible after commit
            )
            
            self._scoped_session = scoped_session(self._session_factory)
            
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            log_with_context(self.logger, logging.INFO, "Database initialized successfully",
                            pool_size=getattr(self.config, 'pool_size', 5),
                            max_overflow=getattr(self.config, 'max_overflow', 10))
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to initialize database",
                            error=str(e),
                            exception_type=type(e).__name__)
            raise
    
    def shutdown(self) -> None:
        self.logger.info("Shutting down database connections")
        
        try:
            if self._scoped_session:
                self._scoped_session.remove()
                self._scoped_session = None
            
            if self._engine:
                self._engine.dispose()
                self._engine = None
            
            self._session_factory = None
            
            self.logger.info("Database shutdown completed")
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error during database shutdown",
                            error=str(e),
                            exception_type=type(e).__name__)
    
    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._session_factory
    
    def get_scoped_session(self) -> scoped_session:
        if self._scoped_session is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._scoped_session
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        session = self._session_factory()
        try:
            log_with_context(self.logger, logging.DEBUG, "Database session created")
            yield session
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Database session error, rolling back",
                            error=str(e),
                            exception_type=type(e).__name__)
            session.rollback()
            raise
        finally:
            session.close()
            log_with_context(self.logger, logging.DEBUG, "Database session closed")
    
    @contextmanager
    def get_transaction(self) -> Generator[Session, None, None]:
        with self.get_session() as session:
            try:
                yield session
                session.commit()
                log_with_context(self.logger, logging.DEBUG, "Database transaction committed")
            except Exception as e:
                session.rollback()
                log_with_context(self.logger, logging.ERROR, "Database transaction rolled back",
                                error=str(e),
                                exception_type=type(e).__name__)
                raise
    
    def health_check(self) -> bool:
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Database health check failed",
                            error=str(e),
                            exception_type=type(e).__name__)
            return False

class InfrastructureDatabaseManager(DatabaseManager):
    """Database manager for infrastructure database (indexer_shared)"""
    pass

class ModelDatabaseManager(DatabaseManager):
    """Database manager for model-specific database (e.g. blub_test)"""
    pass