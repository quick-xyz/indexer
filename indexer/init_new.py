# indexer/__init__.py

import logging
import os
from pathlib import Path
from typing import Set, Dict

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
from .types import DatabaseConfig, EvmAddress, ContractConfig


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
    """Create indexer facade with all services configured"""
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
    
    # Return facade instead of container
    return Indexer(container)


# Rest of the file stays the same...
def _configure_logging_early(env: dict):
    # ... existing implementation

def _register_services(container: IndexerContainer, shared_db_manager: SharedDatabaseManager, secrets_service: SecretsService):
    # ... existing implementation

def _create_shared_db_manager(env: dict) -> SharedDatabaseManager:
    # ... existing implementation

def _create_secrets_service_singleton(env: dict) -> SecretsService:
    # ... existing implementation

# ... all other helper functions stay the same