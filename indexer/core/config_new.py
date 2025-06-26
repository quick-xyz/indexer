# indexer/core/config.py

import msgspec
from msgspec import Struct
from typing import Dict, Optional, List, Set
from pathlib import Path
import os
import logging

from ..types import (
    EvmAddress, 
    ContractConfig, 
    AddressConfig, 
    DatabaseConfig, 
    RpcConfig, 
    StorageConfig, 
    PathsConfig,
    GCSConfig,
    TokenConfig
)
from ..database.models.config import Model, Contract, Token, Address
from .config_service import ConfigService
from .secrets_service import SecretsService
from .logging_config import IndexerLogger, log_with_context


class IndexerConfig(Struct):
    """
    Dynamic configuration loaded from database instead of JSON.
    Combines infrastructure config (env/secrets) with model-specific config (database).
    """
    
    # Infrastructure configuration (from env/secrets/hardcoded)
    model_name: str
    model_version: str
    database: DatabaseConfig
    rpc: RpcConfig
    gcs: GCSConfig
    paths: PathsConfig
    
    # Model-specific configuration (from database)
    model_database_name: str
    source_paths: List[str]
    contracts: Dict[EvmAddress, Contract]
    tokens_of_interest: Dict[EvmAddress, Token]  # Tokens this model actively tracks
    addresses: Dict[EvmAddress, Address]
    
    # Derived storage configuration (hardcoded patterns)
    storage: StorageConfig
    
    @classmethod
    def from_model(cls, model_name: str, config_service: ConfigService, 
                   env_vars: dict = None) -> 'IndexerConfig':
        """
        Create configuration by loading model from database and combining with infrastructure config.
        
        Args:
            model_name: Name of the model to load (e.g., 'smol-ecosystem')
            config_service: Service for database configuration queries
            env_vars: Environment variable overrides
        """
        logger = IndexerLogger.get_logger('core.config')
        log_with_context(logger, logging.INFO, "Loading configuration for model", model_name=model_name)
        
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv()
        env = env_vars or os.environ
        
        # Validate model configuration exists
        if not config_service.validate_model_configuration(model_name):
            raise ValueError(f"Invalid model configuration for: {model_name}")
        
        # Load model-specific configuration from database
        model = config_service.get_model_by_name(model_name)
        contracts = config_service.get_contracts_for_model(model_name)
        tokens_of_interest = config_service.get_tokens_of_interest_for_model(model_name)
        addresses = config_service.get_all_addresses()
        
        log_with_context(logger, logging.INFO, "Model configuration loaded from database",
                       model_name=model_name,
                       model_version=model.version,
                       contract_count=len(contracts),
                       tokens_of_interest_count=len(tokens_of_interest),
                       address_count=len(addresses))
        
        # Create infrastructure configuration
        database = cls._create_database_config(env)
        rpc = cls._create_rpc_config(env)
        gcs = cls._create_gcs_config(env)
        paths = cls._create_paths_config(env)
        storage = cls._create_storage_config(model_name)
        
        config = cls(
            model_name=model_name,
            model_version=model.version,
            model_database_name=model.database_name,
            source_paths=model.source_paths,
            contracts=contracts,
            tokens_of_interest=tokens_of_interest,
            addresses=addresses,
            database=database,
            rpc=rpc,
            gcs=gcs,
            paths=paths,
            storage=storage
        )
        
        log_with_context(logger, logging.INFO, "IndexerConfig created successfully",
                       model_name=model_name,
                       model_version=model.version,
                       model_database=model.database_name)
        
        return config
    
    @staticmethod
    def _create_database_config(env: dict) -> DatabaseConfig:
        logger = IndexerLogger.get_logger('core.config.database')
        
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
        
        if not db_user or not db_password:
            raise ValueError("Database credentials not found in secrets or environment variables")
        
        db_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        log_with_context(logger, logging.DEBUG, "Database configuration created",
                       db_host=db_host, db_port=db_port, db_name=db_name)
        
        return DatabaseConfig(url=db_url)
    
    @staticmethod
    def _create_rpc_config(env: dict) -> RpcConfig:
        logger = IndexerLogger.get_logger('core.config.rpc')
        
        project_id = env.get("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("INDEXER_GCP_PROJECT_ID environment variable required")
        
        secrets_service = SecretsService(project_id)
        
        endpoint_url = secrets_service.get_rpc_endpoint() or env.get("INDEXER_AVAX_RPC")
        
        if not endpoint_url:
            raise ValueError("RPC endpoint not found in secrets or environment variables")
        
        log_with_context(logger, logging.DEBUG, "RPC configuration created")
        
        return RpcConfig(
            endpoint_url=endpoint_url,
            timeout=30,
            max_retries=3
        )
    
    @staticmethod
    def _create_gcs_config(env: dict) -> GCSConfig:
        logger = IndexerLogger.get_logger('core.config.gcs')
        
        project_id = env.get("INDEXER_GCP_PROJECT_ID")
        if not project_id:
            raise ValueError("INDEXER_GCP_PROJECT_ID environment variable required")
        
        bucket_name = "indexer-blocks"
        
        log_with_context(logger, logging.DEBUG, "GCS configuration created",
                       project_id=project_id, bucket_name=bucket_name)
        
        return GCSConfig(
            project_id=project_id,
            bucket_name=bucket_name,
            credentials_path=None
        )
    
    @staticmethod
    def _create_paths_config(env: dict) -> PathsConfig:
        logger = IndexerLogger.get_logger('core.config.paths')
        
        project_root = Path.cwd()
        log_dir = Path(env.get("INDEXER_LOG_DIR", project_root / "logs"))
        
        paths = PathsConfig(
            project_root=project_root,
            indexer_root=project_root / 'indexer',
            config_dir=project_root / 'config',
            data_dir=project_root / 'data',
            log_dir=log_dir,
            abi_dir=project_root / 'config' / 'abis'
        )
        
        for dir_name, dir_path in [
            ("data", paths.data_dir), 
            ("logs", paths.log_dir), 
            ("abi", paths.abi_dir)
        ]:
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                log_with_context(logger, logging.DEBUG, "Directory ensured", 
                               directory=dir_name, path=str(dir_path))
            except Exception as e:
                log_with_context(logger, logging.ERROR, "Failed to create directory",
                               directory=dir_name, path=str(dir_path), error=str(e))
                raise
        
        return paths
    
    @staticmethod
    def _create_storage_config(model_name: str) -> StorageConfig:
        """Create storage configuration with hardcoded patterns"""
        logger = IndexerLogger.get_logger('core.config.storage')
        
        # Hardcoded storage patterns for model workspace
        # Model workspace: indexer-blocks/{model_name}/processing|complete/
        storage = StorageConfig(
            rpc_prefix="",  # Not used - source paths come from model.source_paths
            processing_prefix=f"workspaces/{model_name}/processing/",
            complete_prefix=f"workspaces/{model_name}/complete/",
            rpc_format="",  # Not used - source format determined by streams
            processing_format="block_{:012d}.json",
            complete_format="block_{:012d}.json"
        )
        
        log_with_context(logger, logging.DEBUG, "Storage configuration created",
                       model_name=model_name,
                       processing_prefix=storage.processing_prefix,
                       complete_prefix=storage.complete_prefix)
        
        return storage
    
    def get_model_database_config(self) -> DatabaseConfig:
        """Get database configuration for the model's specific database"""
        # Create a new database config pointing to the model's database
        base_url = self.database.url
        # Replace the database name in the URL
        url_parts = base_url.rsplit('/', 1)
        model_db_url = f"{url_parts[0]}/{self.model_database_name}"
        
        return DatabaseConfig(
            url=model_db_url,
            pool_size=self.database.pool_size,
            max_overflow=self.database.max_overflow
        )
    
    def get_contract_by_address(self, address: str) -> Optional[Contract]:
        """Get contract configuration by address"""
        return self.contracts.get(EvmAddress(address.lower()))
    
    def get_token_by_address(self, address: str) -> Optional[Token]:
        """Get token metadata by address (from tokens of interest)"""
        return self.tokens_of_interest.get(EvmAddress(address.lower()))
    
    def is_token_of_interest(self, address: str) -> bool:
        """Check if a token address is actively tracked by this model"""
        return EvmAddress(address.lower()) in self.tokens_of_interest
    
    def get_all_tokens_of_interest(self) -> Dict[EvmAddress, Token]:
        """Get all tokens this model actively tracks for balance/accounting"""
        return self.tokens_of_interest
    
    def get_address_by_address(self, address: str) -> Optional[Address]:
        """Get address metadata by address"""
        return self.addresses.get(EvmAddress(address.lower()))