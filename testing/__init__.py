# testing/__init__.py
"""
Testing module for the blockchain indexer.

Provides a clean testing environment using the indexer's DI container
and configuration system.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, Any, TypeVar

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.core.indexer_config import IndexerConfig
from indexer.core.container import IndexerContainer
from indexer.core.logging import IndexerLogger, log_with_context

T = TypeVar('T')


class TestingEnvironment:
    """
    Minimal testing environment that wraps the indexer's DI container.
    
    This provides a clean interface for tests while using the actual
    indexer infrastructure.
    """
    
    def __init__(self, model_name: Optional[str] = None, log_level: str = "WARNING"):
        """
        Initialize testing environment.
        
        Args:
            model_name: Model to use (defaults to INDEXER_MODEL env var)
            log_level: Logging level for tests
        """
        self.model_name = model_name or os.getenv("INDEXER_MODEL", "blub_test")
        self.log_level = log_level
        
        # Configure minimal logging
        self._setup_logging()
        
        # Initialize the indexer container
        self._initialize_container()
    
    def _setup_logging(self):
        """Configure logging for testing."""
        IndexerLogger.configure(
            log_dir=PROJECT_ROOT / "logs" / "tests",
            log_level=self.log_level,
            console_enabled=True,
            file_enabled=False,
            structured_format=False  # Simple format for tests
        )
        
        self.logger = IndexerLogger.get_logger('testing')
        
    def _initialize_container(self):
        """Initialize the DI container using create_indexer()."""
        try:
            self.logger.info(f"Initializing testing environment for model: {self.model_name}")
            
            # Use the indexer's entry point
            self.container = create_indexer(model_name=self.model_name)
            self.config = self.container._config
            
            self.logger.info(f"Testing environment ready: {self.config.model_name} v{self.config.model_version}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize testing environment: {e}")
            raise RuntimeError(f"Testing initialization failed: {e}") from e
    
    def get_service(self, service_type: type[T]) -> T:
        """
        Get a service from the DI container.
        
        Args:
            service_type: The service class to retrieve
            
        Returns:
            The service instance
        """
        try:
            return self.container.get(service_type)
        except Exception as e:
            self.logger.error(f"Failed to get service {service_type.__name__}: {e}")
            raise
    
    def get_config(self) -> IndexerConfig:
        """Get the indexer configuration."""
        return self.config
    
    def get_container(self) -> IndexerContainer:
        """Get the DI container directly."""
        return self.container
    
    def cleanup(self):
        """Clean up resources."""
        # The container handles its own cleanup
        pass


# Singleton instance
_test_env: Optional[TestingEnvironment] = None


def get_testing_environment(
    model_name: Optional[str] = None, 
    log_level: str = "WARNING"
) -> TestingEnvironment:
    """
    Get or create the testing environment.
    
    Args:
        model_name: Model to use (defaults to env var)
        log_level: Logging level
        
    Returns:
        TestingEnvironment instance
    """
    global _test_env
    
    # Create new environment if needed
    if _test_env is None or (model_name and model_name != _test_env.model_name):
        if _test_env:
            _test_env.cleanup()
        _test_env = TestingEnvironment(model_name, log_level)
    
    return _test_env


def reset_testing_environment():
    """Reset the testing environment."""
    global _test_env
    if _test_env:
        _test_env.cleanup()
        _test_env = None


__all__ = ['TestingEnvironment', 'get_testing_environment', 'reset_testing_environment']