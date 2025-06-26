# indexer/__init__.py

import logging
import os
from pathlib import Path

from .core.container import IndexerContainer
from .core.config import IndexerConfig
from .core.config_service import ConfigService
from .core.logging_config import IndexerLogger, log_with_context
from .clients.quicknode_rpc import QuickNodeRpcClient
from .storage.gcs_handler import GCSHandler
from .database.connection import DatabaseManager
from .database.repository import RepositoryManager
from .decode.block_decoder import BlockDecoder
from .transform.manager import TransformManager

from .core.secrets_service import SecretsService
from .types import DatabaseConfig


def create_indexer(model_name: str = None, env_vars: dict = None, **overrides) -> IndexerContainer:
    env = env_vars or os.environ
    _configure_logging_early(env)
    
    logger = IndexerLogger.get_logger('core.init')
    logger.info("Creating indexer instance with database-driven configuration")
    
    if not model_name:
        model_name = env.get("INDEXER_MODEL_NAME")
        if not model_name:
            logger.error("No model name provided and INDEXER_MODEL_NAME not set")
            raise ValueError("Must provide model_name or set INDEXER_MODEL_NAME environment variable")
    
    log_with_context(logger, logging.INFO, "Loading configuration for model", model_name=model_name)
    
    infrastructure_db_manager = _create_infrastructure_db_manager(env)
    config_service = ConfigService(infrastructure_db_manager)
    
    config = IndexerConfig.from_model(model_name, config_service, env, **overrides)
    
    log_with_context(logger, logging.INFO, "Configuration loaded successfully", 
                    model_name=config.model_name,
                    model_version=config.model_version,
                    contract_count=len(config.contracts),
                    model_database=config.model_db_name)
    
    container = IndexerContainer(config)
    container.register_singleton(ConfigService, lambda c: config_service)
    _register_services(container)
    
    logger.info("Indexer instance created successfully")
    return container


def _create_infrastructure_db_manager(env: dict) -> DatabaseManager:
    """Create database manager for the infrastructure database (where configuration is stored)"""
    logger = IndexerLogger.get_logger('core.init.infrastructure_db')
    
    project_id = env.get("INDEXER_GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("INDEXER_GCP_PROJECT_ID environment variable required")
    
    secrets_service = SecretsService(project_id)
    db_credentials = secrets_service.get_database_credentials()
    
    db_user = db_credentials.get('user') or env.get("INDEXER_DB_USER")
    db_password = db_credentials.get('password') or env.get("INDEXER_DB_PASSWORD")
    db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
    db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
    db_name = env.get("INDEXER_DB_NAME", "indexer_shared")
    
    db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    infrastructure_db_config = DatabaseConfig(url=db_url)
    
    log_with_context(logger, logging.DEBUG, "Database configuration created",
                    db_host=db_host, db_port=db_port, db_name=db_name)
    
    return DatabaseManager(infrastructure_db_config)


def _configure_logging_early(env: dict) -> None:
    log_dir_env = env.get("INDEXER_LOG_DIR")
    if log_dir_env:
        log_dir = Path(log_dir_env)
    else:
        log_dir = Path.cwd() / "logs"
    
    log_level = env.get("LOG_LEVEL", "INFO").upper()
    console_enabled = env.get("INDEXER_LOG_CONSOLE", "true").lower() == "true"
    file_enabled = env.get("INDEXER_LOG_FILE", "true").lower() == "true"
    structured_format = env.get("INDEXER_LOG_STRUCTURED", "true").lower() == "true"
    
    IndexerLogger.configure(
        log_dir=log_dir,
        log_level=log_level,
        console_enabled=console_enabled,
        file_enabled=file_enabled,
        structured_format=structured_format
    )


def _register_services(container: IndexerContainer):
    logger = IndexerLogger.get_logger('core.services')
    logger.info("Registering services in container")
    
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
    
    # Decoder services (auto-wired via dependency injection)
    logger.debug("Registering decoder services")
    container.register_singleton(LogDecoder, LogDecoder)
    container.register_singleton(TransactionDecoder, TransactionDecoder)
    container.register_singleton(BlockDecoder, BlockDecoder)
    
    # Transform services
    logger.debug("Registering transform services")
    container.register_singleton(TransformRegistry, TransformRegistry)
    container.register_singleton(TransformManager, TransformManager)
    
    # Database services (model-specific database)
    logger.debug("Registering database services")
    container.register_factory(DatabaseManager, _create_model_database_manager)
    container.register_singleton(RepositoryManager, RepositoryManager)
    
    logger.info("Service registration completed")


def _create_rpc_client(container: IndexerContainer) -> QuickNodeRpcClient:
    """Factory function to create RPC client with configuration"""
    config = container._config
    return QuickNodeRpcClient(
        endpoint_url=config.rpc.endpoint_url,
        timeout=config.rpc.timeout,
        max_retries=config.rpc.max_retries
    )


def _create_gcs_handler(container: IndexerContainer) -> GCSHandler:
    """Factory function to create GCS handler with configuration"""
    config = container._config
    return GCSHandler(
        storage_config=config.storage,
        gcs_project=config.gcs.project_id,
        bucket_name=config.gcs.bucket_name,
        credentials_path=config.gcs.credentials_path
    )


def _create_model_database_manager(container: IndexerContainer) -> DatabaseManager:
    logger = IndexerLogger.get_logger('core.factory.database')
    config = container._config
    
    logger.info("Creating database manager")
    db_manager = DatabaseManager(config.database)
    db_manager.initialize()
    
    return db_manager

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