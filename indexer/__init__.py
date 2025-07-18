# indexer/__init__.py

import os
from pathlib import Path
from typing import Set, Dict

from .core.logging import (
    IndexerLogger, 
    log_with_context,
    INFO,
    DEBUG,
    WARNING,
    ERROR,
    CRITICAL,
)
from .core.config_service import ConfigService
from .core.container import IndexerContainer
from .core.indexer_config import IndexerConfig
from .core.secrets_service import SecretsService
from .contracts.registry import ContractRegistry
from .contracts.manager import ContractManager
from .contracts.abi_loader import ABILoader
from .clients.quicknode_rpc import QuickNodeRpcClient
from .database.connection import SharedDatabaseManager, ModelDatabaseManager
from .database.repository_manager import RepositoryManager
from .database.writers.domain_event_writer import DomainEventWriter
from .database.migration_manager import MigrationManager
from .decode.block_decoder import BlockDecoder
from .decode.transaction_decoder import TransactionDecoder
from .decode.log_decoder import LogDecoder
from .pipeline.indexing_pipeline import IndexingPipeline  
from .pipeline.batch_pipeline import BatchPipeline
from .storage.gcs_handler import GCSHandler
from .transform.manager import TransformManager
from .transform.registry import TransformRegistry
from .types import DatabaseConfig, EvmAddress, ContractConfig, StorageConfig
    

class Indexer:    
    def __init__(self, container: IndexerContainer):
        self._container = container
        self._config = container._config
    
    # === Service Access ===
    def get_rpc_client(self) -> QuickNodeRpcClient:
        return self._container.get(QuickNodeRpcClient)
    
    def get_storage(self) -> GCSHandler:
        return self._container.get(GCSHandler)
    
    def get_repository_manager(self) -> RepositoryManager:
        return self._container.get(RepositoryManager)
    
    def get_decoder(self) -> BlockDecoder:
        return self._container.get(BlockDecoder)
    
    def get_transform_manager(self) -> TransformManager:
        return self._container.get(TransformManager)
    
    def get_indexing_pipeline(self) -> IndexingPipeline:
        return self._container.get(IndexingPipeline)
    
    def get_batch_pipeline(self) -> BatchPipeline:
        return self._container.get(BatchPipeline)
    
    # === Database Convenience ===
    def get_model_session(self):
        return self.get_repository_manager().get_model_session()
    
    def get_shared_session(self):
        return self.get_repository_manager().get_shared_session()
    
    # === Pipeline Operations ===
    def process_block(self, block_number: int) -> bool:
        pipeline = self.get_indexing_pipeline()
        return pipeline.process_single_block(block_number)
    
    '''
    def queue_blocks(self, start: int, end: int, batch_size: int = 100):
        batch_pipeline = self.get_batch_pipeline()
        return batch_pipeline.queue_block_range(start, end, batch_size)
    '''
    # === Configuration Access ===
    @property
    def model_name(self) -> str:
        return self._config.model_name
    
    @property
    def model_version(self) -> str:
        return self._config.model_version
    
    @property
    def tracked_tokens(self) -> Set[EvmAddress]:
        return self._config.tracked_tokens
    
    @property
    def contracts(self) -> Dict[EvmAddress, ContractConfig]:
        return self._config.contracts
    
    # === Escape Hatch ===
    def get_service(self, service_type):
        """Get any service from container (for advanced usage)"""
        return self._container.get(service_type)


def create_indexer(model_name: str = None, env_vars: dict = None, **overrides) -> Indexer:
    env = env_vars or os.environ
    _configure_logging_early(env)
    
    logger = IndexerLogger.get_logger('core.init')
    logger.info("Creating indexer instance with database-driven configuration")
    
    if not model_name:
        model_name = env.get("INDEXER_MODEL")
        if not model_name:
            logger.error("No model name provided and INDEXER_MODEL not set")
            raise ValueError("Must provide model_name or set INDEXER_MODEL environment variable")
    
    log_with_context(logger, INFO, "Loading configuration for model", model_name=model_name)

    secrets_service = _create_secrets_service_singleton(env)
    shared_db_manager = _create_shared_db_manager(env,secrets_service)
    config_service = ConfigService(shared_db_manager)

    config = IndexerConfig.from_database(model_name, config_service, env, **overrides)
    model_db_manager = _create_model_db_manager(env,secrets_service,config.model_db)


    log_with_context(logger, INFO, "Configuration loaded successfully", 
                    model_name=config.model_name,
                    model_version=config.model_version,
                    contract_count=len(config.contracts),
                    model_database=config.model_db)
    
    container = IndexerContainer(config)
    
    _register_services(container, shared_db_manager, model_db_manager, secrets_service)
    
    log_with_context(logger, INFO, "Indexer created successfully")
    
    return Indexer(container)

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

def _register_services(container: IndexerContainer, shared_db_manager: SharedDatabaseManager, model_db_manager: ModelDatabaseManager, secrets_service: SecretsService):
    logger = IndexerLogger.get_logger('core.services')
    logger.info("Registering services in container")
    
    logger.debug("Registering core services")
    container.register_instance(SecretsService, secrets_service)
    container.register_instance(SharedDatabaseManager, shared_db_manager)
    container.register_factory(ModelDatabaseManager, model_db_manager)

    logger.debug("Registering client services")
    container.register_factory(QuickNodeRpcClient, _create_rpc_client)

    logger.debug("Registering storage services")
    container.register_factory(GCSHandler, _create_gcs_handler)

    logger.debug("Registering contract services")
    container.register_singleton(ABILoader, ABILoader)
    container.register_singleton(ContractRegistry, ContractRegistry)
    container.register_singleton(ContractManager, ContractManager)
    
    logger.debug("Registering decoder services")
    container.register_singleton(LogDecoder, LogDecoder)
    container.register_singleton(TransactionDecoder, TransactionDecoder)
    container.register_singleton(BlockDecoder, BlockDecoder)
    
    logger.debug("Registering transform services")
    container.register_singleton(TransformRegistry, TransformRegistry)
    container.register_singleton(TransformManager, TransformManager)
    
    logger.debug("Registering database services")
    container.register_singleton(RepositoryManager, RepositoryManager)

    logger.debug("Registering database writers")
    container.register_singleton(DomainEventWriter, DomainEventWriter)

    logger.debug("Registering pipeline services")
    container.register_singleton(IndexingPipeline, IndexingPipeline)
    container.register_singleton(BatchPipeline, BatchPipeline)

    logger.debug("Registering migration services")
    container.register_singleton(MigrationManager, MigrationManager)

    logger.info("Service registration completed")

def _create_secrets_service_singleton(env: dict) -> SecretsService:
    """Create a singleton SecretsService instance before container initialization"""
    logger = IndexerLogger.get_logger('core.factory.secrets')
    
    project_id = env.get("INDEXER_GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("INDEXER_GCP_PROJECT_ID environment variable required for SecretsService")
    
    log_with_context(logger, DEBUG, "Creating singleton SecretsService", project_id=project_id)
    
    return SecretsService(project_id)

def _create_shared_db_manager(env: dict, secrets_service: SecretsService) -> SharedDatabaseManager:
    logger = IndexerLogger.get_logger('core.factory.shared_db')
    
    project_id = env.get("INDEXER_GCP_PROJECT_ID")
    
    if project_id:
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
        raise ValueError("Infrastructure database credentials not found")

    shared_db_name = env.get("INDEXER_SHARED_DB")
    shared_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{shared_db_name}"
    shared_db_config = DatabaseConfig(url=shared_db_url)

    log_with_context(logger, DEBUG, "Creating shared database manager", database=shared_db_name)

    db_manager = SharedDatabaseManager(shared_db_config)
    db_manager.initialize()
    
    return db_manager

def _create_model_db_manager(env: dict, secrets_service: SecretsService, model_db_name: str) -> ModelDatabaseManager:
    logger = IndexerLogger.get_logger('core.factory.model_db')

    project_id = env.get("INDEXER_GCP_PROJECT_ID")

    if project_id:
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

    model_db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{model_db_name}"
    model_db_config = DatabaseConfig(url=model_db_url)

    log_with_context(logger, DEBUG, "Creating model database manager", database=model_db_name)

    db_manager = ModelDatabaseManager(model_db_config)
    db_manager.initialize()
    
    return db_manager

def _create_rpc_client(secrets_service: SecretsService) -> QuickNodeRpcClient:
    logger = IndexerLogger.get_logger('core.factory.rpc')

    endpoint_url = secrets_service.get_rpc_endpoint()
        
    if not endpoint_url:
        raise ValueError("RPC endpoint not found in secrets")
    
    return QuickNodeRpcClient(endpoint_url= endpoint_url)

def _create_gcs_handler(env: dict) -> GCSHandler:
    logger = IndexerLogger.get_logger('core.factory.gcs')
    
    model_name = env.get("INDEXER_MODEL_NAME")
    project_id = env.get("INDEXER_GCP_PROJECT_ID")
    bucket_name = env.get("INDEXER_GCS_BUCKET_NAME")
    
    if not (project_id or model_name or bucket_name):
        raise ValueError("GCS environment variable missing")
    
    storage = StorageConfig(
            processing_prefix=f"models/{model_name}/processing/",
            complete_prefix=f"models/{model_name}/complete/",
            processing_format="block_{:012d}.json",
            complete_format="block_{:012d}.json"
        )

    log_with_context(logger, DEBUG, "Storage configuration created",
                    model_name=model_name,
                    processing_prefix=storage.processing_prefix,
                    complete_prefix=storage.complete_prefix)
        
    return GCSHandler(
        storage_config=storage,
        gcs_project=project_id,
        bucket_name=bucket_name,
    )
