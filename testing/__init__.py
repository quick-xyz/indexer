# testing/__init__.py
"""
Testing package for the blockchain indexer.

This package integrates with the indexer's configuration system,
dependency injection container, and logging infrastructure.
"""

import sys
import logging
import os
from pathlib import Path

# Add project root to Python path for imports
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import indexer components
from indexer import create_indexer
from indexer.core.config import IndexerConfig
from indexer.core.container import IndexerContainer
from indexer.core.logging_config import IndexerLogger, log_with_context


class TestingEnvironment:
    """
    Testing environment that uses the indexer's database-driven configuration system
    """
    
    def __init__(self, model_name: str = None, log_level: str = "DEBUG"):
        self.model_name = model_name or os.getenv("INDEXER_MODEL_NAME", "blub_test")
        self.log_level = log_level
        self.container: IndexerContainer = None
        self.config: IndexerConfig = None
        self.logger = None
        
        self._setup_logging()
        self._initialize_indexer()
    
    def _setup_logging(self):
        """Configure logging using indexer's logging system"""
        # Configure logging with debug level for testing
        IndexerLogger.configure(
            log_dir=PROJECT_ROOT / "logs",
            log_level=self.log_level,
            console_enabled=True,
            file_enabled=True,
            structured_format=True
        )
        
        self.logger = IndexerLogger.get_logger('testing')
        self.logger.info("Testing environment initialized")
    
    def _initialize_indexer(self):
        """Initialize indexer using the database-driven configuration system"""
        try:
            # Use the new database-driven configuration
            self.container = create_indexer(model_name=self.model_name)
            self.config = self.container._config
            
            log_with_context(
                self.logger, 
                logging.INFO,
                "Indexer container initialized for testing",
                model_name=self.model_name,
                indexer_name=self.config.model_name,
                indexer_version=self.config.model_version
            )
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR, 
                "Failed to initialize indexer for testing",
                error=str(e),
                exception_type=type(e).__name__,
                model_name=self.model_name
            )
            raise
    
    def get_service(self, service_type):
        """Get service from the dependency injection container"""
        if not self.container:
            raise RuntimeError("Indexer container not initialized")
        return self.container.get(service_type)
    
    def get_config(self) -> IndexerConfig:
        """Get the indexer configuration"""
        return self.config
    
    def get_logger(self, name: str = None):
        """Get a logger using indexer's logging system"""
        logger_name = f"testing.{name}" if name else "testing"
        return IndexerLogger.get_logger(logger_name)


# Global testing environment instance
_testing_env = None

def get_testing_environment(model_name: str = None, log_level: str = "DEBUG") -> TestingEnvironment:
    """Get or create the global testing environment"""
    global _testing_env
    if _testing_env is None:
        _testing_env = TestingEnvironment(model_name, log_level)
    return _testing_env

def reset_testing_environment():
    """Reset the global testing environment"""
    global _testing_env
    _testing_env = None