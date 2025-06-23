# indexer/__init__.py
import logging
import os
from pathlib import Path

from .core.container import IndexerContainer
from .core.config import IndexerConfig
from .core.logging_config import IndexerLogger, log_with_context
from .clients.quicknode_rpc import QuickNodeRpcClient
from .storage.gcs_handler import GCSHandler
from .decode.block_decoder import BlockDecoder
from .transform.manager import TransformManager
from .database.connection import DatabaseManager
from .database.repository import RepositoryManager

def create_indexer(config_path: str = None, config_dict: dict = None, 
                  env_vars: dict = None, **overrides) -> IndexerContainer:
    """
    Create a new indexer instance with dependency injection.
    
    Args:
        config_path: Path to configuration file
        config_dict: Configuration dictionary  
        env_vars: Environment variables override
        **overrides: Configuration overrides
        
    Returns:
        Configured IndexerContainer instance
    """
    
    # Configure logging as early as possible
    env = env_vars or os.environ
    _configure_logging_early(env)
    
    # Get logger for this module
    logger = IndexerLogger.get_logger('core.init')
    logger.info("Creating indexer instance")
    
    if config_path:
        log_with_context(logger, logging.INFO, "Loading configuration from file", config_path=config_path)
        config = IndexerConfig.from_file(config_path, env_vars, **overrides)
    elif config_dict:
        logger.info("Loading configuration from dictionary")
        config = IndexerConfig.from_dict(config_dict, env_vars, **overrides)
    else:
        logger.error("No configuration provided")
        raise ValueError("Must provide either config_path or config_dict")
    
    log_with_context(logger, logging.INFO, "Configuration loaded successfully", 
                    indexer_name=config.name,
                    indexer_version=config.version,
                    contract_count=len(config.contracts) if config.contracts else 0)
    
    container = IndexerContainer(config)
    _register_services(container)
    
    logger.info("Indexer instance created successfully")
    return container

def _configure_logging_early(env: dict) -> None:
    """Configure logging based on environment variables"""
    
    # Determine log directory
    log_dir_env = env.get("INDEXER_LOG_DIR")
    if log_dir_env:
        log_dir = Path(log_dir_env)
    else:
        log_dir = Path.cwd() / "logs"
    
    # Get logging configuration from environment
    log_level = env.get("LOG_LEVEL", "INFO").upper()
    console_enabled = env.get("INDEXER_LOG_CONSOLE", "true").lower() == "true"
    file_enabled = env.get("INDEXER_LOG_FILE", "true").lower() == "true"
    structured_format = env.get("INDEXER_LOG_STRUCTURED", "true").lower() == "true"
    
    # Configure the logging system
    IndexerLogger.configure(
        log_dir=log_dir,
        log_level=log_level,
        console_enabled=console_enabled,
        file_enabled=file_enabled,
        structured_format=structured_format
    )

def _register_services(container: IndexerContainer):
    """Register all services in the container"""
    logger = IndexerLogger.get_logger('core.services')
    logger.info("Registering services in container")
    
    # Import here to avoid circular dependencies
    from .contracts.registry import ContractRegistry
    from .contracts.manager import ContractManager
    from .decode.block_decoder import BlockDecoder
    from .decode.transaction_decoder import TransactionDecoder
    from .decode.log_decoder import LogDecoder
    from .transform.registry import TransformRegistry
    from .transform.manager import TransformManager
    
    # Client services (need factory functions for config parameters)
    logger.debug("Registering client services")
    container.register_factory(QuickNodeRpcClient, _create_rpc_client)
    container.register_factory(GCSHandler, _create_gcs_handler)
    
    # Contract services (dependency injection handles relationships)
    logger.debug("Registering contract services")
    container.register_singleton(ContractRegistry, ContractRegistry)
    container.register_singleton(ContractManager, ContractManager)
    
    # Decoder services (auto-wired dependencies)
    logger.debug("Registering decoder services")
    container.register_singleton(LogDecoder, LogDecoder)
    container.register_singleton(TransactionDecoder, TransactionDecoder)
    container.register_singleton(BlockDecoder, BlockDecoder)
    
    # Transform services (auto-wired dependencies)
    logger.debug("Registering transform services")
    container.register_singleton(TransformRegistry, TransformRegistry)
    container.register_singleton(TransformManager, TransformManager)

    # Database services
    logger.debug("Registering database services")
    container.register_factory(DatabaseManager, _create_database_manager)
    container.register_singleton(RepositoryManager, RepositoryManager)

    logger.info("Service registration completed")

def _create_database_manager(container: IndexerContainer) -> DatabaseManager:
    """Create database manager from config"""
    logger = IndexerLogger.get_logger('core.factory.database')
    config = container._config
    
    logger.info("Creating database manager")
    db_manager = DatabaseManager(config.database)
    db_manager.initialize()
    
    return db_manager

def _create_rpc_client(container: IndexerContainer) -> QuickNodeRpcClient:
    """Create RPC client from config"""
    logger = IndexerLogger.get_logger('core.factory.rpc')
    config = container._config
    
    log_with_context(logger, logging.INFO, "Creating RPC client", endpoint_url=config.rpc.endpoint_url)
    
    try:
        client = QuickNodeRpcClient(endpoint_url=config.rpc.endpoint_url)
        logger.info("RPC client created successfully")
        return client
    except Exception as e:
        log_with_context(logger, logging.ERROR, "Failed to create RPC client", 
                        error=str(e),
                        exception_type=type(e).__name__)
        raise

def _create_gcs_handler(container: IndexerContainer) -> GCSHandler:
    """Create GCS handler from config"""
    logger = IndexerLogger.get_logger('core.factory.gcs')
    config = container._config
    
    log_with_context(logger, logging.INFO, "Creating GCS handler", 
                    project_id=config.gcs.project_id,
                    bucket_name=config.gcs.bucket_name,
                    has_credentials=bool(config.gcs.credentials_path))
    
    try:
        handler = GCSHandler(
            storage_config=config.storage,
            gcs_project=config.gcs.project_id,
            bucket_name=config.gcs.bucket_name,
            credentials_path=config.gcs.credentials_path
        )
        logger.info("GCS handler created successfully")
        return handler
    except Exception as e:
        log_with_context(logger, logging.ERROR, "Failed to create GCS handler",
                        error=str(e),
                        exception_type=type(e).__name__)
        raise


# Optional: Convenience functions for common usage patterns
def get_rpc_client(container: IndexerContainer) -> QuickNodeRpcClient:
    """Get RPC client from container"""
    return container.get(QuickNodeRpcClient)

def get_storage_handler(container: IndexerContainer) -> GCSHandler:
    """Get storage handler from container"""
    return container.get(GCSHandler)

def get_block_decoder(container: IndexerContainer) -> BlockDecoder:
    """Get block decoder from container"""
    return container.get(BlockDecoder)

def get_transformation_manager(container: IndexerContainer) -> TransformManager:
    """Get transformation manager from container"""
    return container.get(TransformManager)