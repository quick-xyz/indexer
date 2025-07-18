# api/dependencies.py

from fastapi import Depends, HTTPException
from typing import Generator
import logging

from indexer.database.repository_manager import RepositoryManager
from indexer.database.connection import DatabaseManager
from indexer.core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

# Global variables - these get set during app startup
_repository_manager: RepositoryManager = None
_logger = None

def set_dependencies(repo_manager: RepositoryManager):
    """Called during app startup to set global dependencies"""
    global _repository_manager, _logger
    _repository_manager = repo_manager
    _logger = IndexerLogger.get_logger('api.dependencies')

def get_repository_manager() -> RepositoryManager:
    """Dependency to get repository manager"""
    if _repository_manager is None:
        raise HTTPException(status_code=500, detail="Repository manager not initialized")
    return _repository_manager

def get_database_session():
    """Dependency to get database session with proper cleanup"""
    repo_manager = get_repository_manager()
    
    with repo_manager.db_manager.get_session() as session:
        try:
            yield session
        except Exception as e:
            if _logger:
                log_with_context(_logger, ERROR, "Database session error",
                                error=str(e), exception_type=type(e).__name__)
            raise HTTPException(status_code=500, detail="Database error")

def get_logger():
    """Dependency to get logger"""
    if _logger is None:
        return IndexerLogger.get_logger('api.default')
    return _logger