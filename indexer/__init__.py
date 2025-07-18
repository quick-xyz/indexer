# indexer/__init__.py

import logging
import os
from pathlib import Path

from .core.container import IndexerContainer
from .core.indexer_config import IndexerConfig
from .core.config_service import ConfigService
from .core.logging import IndexerLogger, log_with_context
from .clients.quicknode_rpc import QuickNodeRpcClient
from .storage.gcs_handler import GCSHandler
from .database.connection import DatabaseManager, SharedDatabaseManager, ModelDatabaseManager
from .database.repository_manager import RepositoryManager
from .decode.block_decoder import BlockDecoder
from .transform.manager import TransformManager
from .database.writers.domain_event_writer import DomainEventWriter
from .pipeline.indexing_pipeline import IndexingPipeline  
from .pipeline.batch_pipeline import BatchPipeline
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

    shared_db_manager = _create_shared_db_manager(env)
    config_service = ConfigService(shared_db_manager)
    
    config = IndexerConfig.from_model(model_name, config_service, env, **overrides)
    
    log_with_context(logger, logging.INFO, "Configuration loaded successfully", 
                    model_name=config.model_name,
                    model_version=config.model_version,
                    contract_count=len(config.contracts),
                    model_database=config.model_db_name)
    
    container = IndexerContainer(config)
    
    # Create singleton SecretsService before registering other services
    secrets_service = _create_secrets_service_singleton(env)
    
    _register_services(container, shared_db_manager, secrets_service)
    
    log_with_context(logger, logging.INFO, "Indexer created successfully")
    
    return container


def _configure_logging_early(env: dict):
    log_dir_env = env.get("INDEXER_LOG_DIR")
    if log_dir_env:
        log_dir = Path(log_dir_env)
    else:
        log_dir = Path.cwd() / "logs"
    
    log_level = env.get("INDEXER_LOG_LEVEL", "INFO")
    console_enabled = env.get("INDEXER_LOG_CONSOLE", "true").lower() == "true"
    file_enabled = env.get("INDEXER_LOG_FILE", "true").lower() == "true"
    structured_format = env.get("INDEXER_LOG_STRUCTURED", "false").lower() == "true"
    
    IndexerLogger.configure(
        log_dir=log_dir,
        log_level=log_level,
        console_enabled=console_enabled,
        file_enabled=file_enabled,
        structured_format=structured_format
    )


def _register_services(container: IndexerContainer, shared_db_manager: SharedDatabaseManager, secrets_service: SecretsService):
    logger = IndexerLogger.get_logger('core.services')
    logger.info("Registering services in container")
    
    from .contracts.registry import ContractRegistry
    from .contracts.manager import ContractManager
    from .contracts.abi_loader import ABILoader
    from .decode.block_decoder import BlockDecoder
    from .decode.transaction_decoder import TransactionDecoder
    from .decode.log_decoder import LogDecoder
    from .transform.registry import TransformRegistry
    from .transform.manager import TransformManager
    
    # Core services - register SecretsService as singleton instance
    logger.debug("Registering core services")
    container.register_instance(SecretsService, secrets_service)

    # Client services (need factory functions for config parameters)
    logger.debug("Registering client services")
    container.register_factory(QuickNodeRpcClient, _create_rpc_client)

    logger.debug("Registering storage services")
    container.register_factory(GCSHandler, _create_gcs_handler)

    # Contract services (dependency injection handles relationships)
    logger.debug("Registering contract services")
    container.register_singleton(ABILoader, ABILoader)
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
    container.register_instance(SharedDatabaseManager, shared_db_manager)
    container.register_factory(ModelDatabaseManager, _create_model_database_manager)
    container.register_singleton(RepositoryManager, RepositoryManager)

    # Database writers
    logger.debug("Registering database writers")
    container.register_singleton(DomainEventWriter, DomainEventWriter)

    # Pipeline services  
    logger.debug("Registering pipeline services")
    container.register_singleton(IndexingPipeline, IndexingPipeline)
    container.register_singleton(BatchPipeline, BatchPipeline)

    # Migration services
    logger.debug("Registering migration services")
    from .database.migration_manager import MigrationManager
    container.register_singleton(MigrationManager, MigrationManager)

    logger.info("Service registration completed")


def _create_rpc_client(container: IndexerContainer) -> QuickNodeRpcClient:
    """Factory function to create RPC client with configuration"""
    config = container._config
    return QuickNodeRpcClient(
        endpoint_url=config.rpc.endpoint_url
    )


def _create_gcs_handler(container: IndexerContainer) -> GCSHandler:
    """Factory function to create single GCS handler with backward compatibility"""
    config = container._config
    
    # Create GCS handler without specific source configuration
    # Sources will be passed to methods as needed
    return GCSHandler(
        storage_config=config.storage,
        gcs_project=config.gcs.project_id,
        bucket_name=config.gcs.bucket_name,
        credentials_path=config.gcs.credentials_path,
    )

def _create_model_database_manager(container: IndexerContainer) -> ModelDatabaseManager:
    logger = IndexerLogger.get_logger('core.factory.database')
    config = container._config
    
    logger.info("Creating database manager")

    env = os.environ
    project_id = env.get("INDEXER_GCP_PROJECT_ID")

    if project_id:
        # Use the singleton SecretsService from the container
        secrets_service = container.get(SecretsService)
        db_credentials = secrets_service.get_database_credentials()
        
        db_user = db_credentials.get('user') or env.get("INDEXER_DB_USER")
        db_password = db_credentials.get('password') or env.get("INDEXER_DB_PASSWORD")
        db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
        db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
    else:
        db_user = env.get("INDEXER_DB_USER")
        db_password = env.get("INDEXER_DB_PASSWORD")
        db_host = env.get("INDEXER_DB_HOST", "127.0.0.1")
        db_port = env.get("INDEXER_DB_PORT", "5432")

    if not db_user or not db_password:
        raise ValueError("Database credentials not found")

    # Use the model-specific database name
    model_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{config.model_db_name}"
    model_db_config = DatabaseConfig(url=model_db_url)

    db_manager = ModelDatabaseManager(model_db_config)
    db_manager.initialize()
    
    return db_manager

def _create_secrets_service_singleton(env: dict) -> SecretsService:
    """Create a singleton SecretsService instance before container initialization"""
    logger = IndexerLogger.get_logger('core.factory.secrets')
    
    project_id = env.get("INDEXER_GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("INDEXER_GCP_PROJECT_ID environment variable required for SecretsService")
    
    log_with_context(logger, logging.INFO, "Creating singleton SecretsService", project_id=project_id)
    
    return SecretsService(project_id)

def _create_shared_db_manager(env: dict) -> SharedDatabaseManager:
    """Create infrastructure database manager for config service"""
    logger = IndexerLogger.get_logger('core.factory.infrastructure_db')
    
    project_id = env.get("INDEXER_GCP_PROJECT_ID")
    
    if project_id:
        # Use temporary SecretsService instance for infrastructure setup only
        # The main singleton will be created later
        temp_secrets_service = SecretsService(project_id)
        db_credentials = temp_secrets_service.get_database_credentials()
        
        db_user = db_credentials.get('user') or env.get("INDEXER_DB_USER") 
        db_password = db_credentials.get('password') or env.get("INDEXER_DB_PASSWORD")
        db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
        db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
    else:
        db_user = env.get("INDEXER_DB_USER")
        db_password = env.get("INDEXER_DB_PASSWORD")  
        db_host = env.get("INDEXER_DB_HOST", "127.0.0.1")
        db_port = env.get("INDEXER_DB_PORT", "5432")

    if not db_user or not db_password:
        raise ValueError("Infrastructure database credentials not found")

    infrastructure_db_name = env.get("INDEXER_DB_NAME")
    infrastructure_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{infrastructure_db_name}"
    infrastructure_db_config = DatabaseConfig(url=infrastructure_db_url)

    log_with_context(logger, logging.INFO, "Creating infrastructure database manager", database=infrastructure_db_name)

    db_manager = SharedDatabaseManager(infrastructure_db_config)
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