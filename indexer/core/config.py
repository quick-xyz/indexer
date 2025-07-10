# indexer/core/config.py

from msgspec import Struct
from typing import Dict, Optional, List
from pathlib import Path
import os
import logging

from ..types import (
    EvmAddress, 
    DatabaseConfig, 
    RpcConfig, 
    StorageConfig, 
    PathsConfig,
    GCSConfig,
)
from ..database.shared.tables.config import Contract, Token, Address, Source
from .config_service import ConfigService
from .secrets_service import SecretsService
from .logging_config import IndexerLogger, log_with_context
from ..types import ContractConfig, DecoderConfig, TransformerConfig

class IndexerConfig(Struct):
    model_name: str
    model_version: str
    model_db_name: str
    source_paths: List[str]  # Deprecated, use sources instead

    database: DatabaseConfig
    rpc: RpcConfig
    gcs: GCSConfig
    storage: StorageConfig
    paths: PathsConfig

    contracts: Dict[EvmAddress, ContractConfig]
    model_tokens: Dict[EvmAddress, Token]
    addresses: Dict[EvmAddress, Address]
    sources: Dict[int, Source]  # source_id -> Source object

    
    @classmethod
    def from_model(cls, model_name: str, config_service: ConfigService, 
                   env_vars: dict = None) -> 'IndexerConfig':
        logger = IndexerLogger.get_logger('core.config')
        log_with_context(logger, logging.INFO, "Loading configuration for model", model_name=model_name)
        
        from dotenv import load_dotenv
        load_dotenv()
        env = env_vars or os.environ
        
        if not config_service.validate_model_configuration(model_name):
            raise ValueError(f"Invalid model configuration for: {model_name}")
        
        model = config_service.get_model_by_name(model_name)
        db_contracts = config_service.get_contracts_for_model(model_name)
        model_tokens = config_service.get_model_tokens(model_name)
        addresses = config_service.get_all_addresses()
        
        # Convert database Contract objects to ContractConfig objects
        contracts = {
            address: cls._convert_db_contract_to_config(contract)
            for address, contract in db_contracts.items()
        }
        
        # NEW: Get sources from database
        sources_list = config_service.get_sources_for_model(model_name)
        sources = {source.id: source for source in sources_list}
        
        # Fallback to old source_paths for backward compatibility
        source_paths = model.source_paths if model.source_paths else []
        
        log_with_context(logger, logging.INFO, "Model configuration loaded from database",
                       model_name=model_name,
                       model_version=model.version,
                       contract_count=len(contracts),
                       model_tokens_count=len(model_tokens),
                       address_count=len(addresses),
                       sources_count=len(sources))
        
        database = cls._create_database_config(env)
        rpc = cls._create_rpc_config(env)
        gcs = cls._create_gcs_config(env)
        paths = cls._create_paths_config(env)
        storage = cls._create_storage_config(model_name)
        
        config = cls(
            model_name=model_name,
            model_version=model.version,
            model_db_name=model.database_name,
            source_paths=source_paths,  # Backward compatibility
            contracts=contracts,  # Now contains ContractConfig objects
            model_tokens=model_tokens,
            addresses=addresses,
            sources=sources,  # NEW
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
    def _convert_db_contract_to_config(db_contract: Contract) -> ContractConfig:
        """Convert database Contract object to ContractConfig type"""
        # Build DecoderConfig if decode_config exists
        decode = None
        if db_contract.decode_config:
            decode = DecoderConfig(
                abi_dir=db_contract.decode_config.get('abi_dir', ''),
                abi=db_contract.decode_config.get('abi_file', '')  # Note: DecoderConfig expects 'abi' not 'abi_file'
            )
        
        # Build TransformerConfig if transform_config exists
        transform = None
        if db_contract.transform_config:
            transform = TransformerConfig(
                name=db_contract.transform_config.get('name', ''),
                instantiate=db_contract.transform_config.get('instantiate', {})
            )
        
        return ContractConfig(
            name=db_contract.name,
            project=db_contract.project or '',
            type=db_contract.type,
            decode=decode,
            transform=transform,
            token=None,  # Not used in current implementation
            abi=None  # This will be loaded by ABILoader
        )

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
        logger = IndexerLogger.get_logger('core.config.storage')
        
        storage = StorageConfig(
            processing_prefix=f"models/{model_name}/processing/",
            complete_prefix=f"models/{model_name}/complete/",
            processing_format="block_{:012d}.json",
            complete_format="block_{:012d}.json"
        )
        
        log_with_context(logger, logging.DEBUG, "Storage configuration created",
                       model_name=model_name,
                       processing_prefix=storage.processing_prefix,
                       complete_prefix=storage.complete_prefix)
        
        return storage

    def get_model_db_config(self) -> DatabaseConfig:
        base_url = self.database.url
        url_parts = base_url.rsplit('/', 1)
        model_db_url = f"{url_parts[0]}/{self.model_db_name}"
        
        return DatabaseConfig(
            url=model_db_url,
            pool_size=self.database.pool_size,
            max_overflow=self.database.max_overflow
        )

    def get_contract_by_address(self, address: str) -> Optional[Contract]:
        return self.contracts.get(EvmAddress(address.lower()))
    
    def get_token_metadata(self, address: str) -> Optional[Token]:
        return self.model_tokens.get(EvmAddress(address.lower()))
    
    def is_model_token(self, address: str) -> bool:
        return EvmAddress(address.lower()) in self.model_tokens
    
    def get_all_model_tokens(self) -> Dict[EvmAddress, Token]:
        return self.model_tokens
    
    def get_address_metadata(self, address: str) -> Optional[Address]:
        return self.addresses.get(EvmAddress(address.lower()))
    
    def get_source_by_id(self, source_id: int) -> Optional[Source]:
        """Get source by ID"""
        return self.sources.get(source_id)

    def get_all_sources(self) -> List[Source]:
        """Get all sources for this model"""
        return list(self.sources.values())

    def get_primary_source(self) -> Optional[Source]:
        """Get the primary (first) source for this model"""
        if self.sources:
            return next(iter(self.sources.values()))
        return None

    def get_rpc_path_for_block(self, block_number: int, source_id: Optional[int] = None) -> str:
        if source_id is None:
            source = self.get_primary_source()
        else:
            source = self.get_source_by_id(source_id)
            if not source:
                raise ValueError(f"Source ID {source_id} not found")
        
        path = source.path
        format_str = source.format
        
        return f"{path}{format_str.format(block_number, block_number)}"