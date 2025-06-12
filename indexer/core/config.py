# indexer/core/config.py

import msgspec
from msgspec import Struct
from typing import Dict, Optional
from pathlib import Path
import json
import os
import logging

from ..types import (
    EvmAddress, 
    ContractConfig, 
    AddressConfig, 
    ABIConfig, 
    DatabaseConfig, 
    RpcConfig, 
    StorageConfig, 
    PathsConfig,
    GCSConfig,
    TokenConfig
)
from .logging_config import IndexerLogger, log_with_context

class IndexerConfig(Struct):
    name: str
    version: str
    storage: StorageConfig
    database: DatabaseConfig
    rpc: RpcConfig
    gcs: GCSConfig
    contracts: Dict[EvmAddress, ContractConfig] = msgspec.field(default_factory=dict)
    addresses: Dict[EvmAddress, AddressConfig] = msgspec.field(default_factory=dict)
    tokens: Dict[EvmAddress, TokenConfig] = msgspec.field(default_factory=dict)
    paths: Optional[PathsConfig] = None
    
    @classmethod
    def from_file(cls, config_path: str, env_vars: dict = None, **overrides) -> 'IndexerConfig':
        # Get logger (logging should already be configured by this point)
        logger = IndexerLogger.get_logger('core.config')
        log_with_context(logger, logging.INFO, "Loading configuration from file", config_path=config_path)
        
        config_path = Path(config_path)
        
        if not config_path.exists():
            log_with_context(logger, logging.ERROR, "Configuration file not found", config_path=str(config_path))
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        try:
            with open(config_path) as f:
                config_dict = json.load(f)
                
            log_with_context(logger, logging.DEBUG, "Configuration file loaded successfully", 
                           config_keys=list(config_dict.keys()),
                           file_size=config_path.stat().st_size)
                
        except json.JSONDecodeError as e:
            log_with_context(logger, logging.ERROR, "Invalid JSON in configuration file",
                           config_path=str(config_path),
                           error=str(e))
            raise
        except Exception as e:
            log_with_context(logger, logging.ERROR, "Failed to read configuration file",
                           config_path=str(config_path),
                           error=str(e),
                           exception_type=type(e).__name__)
            raise
            
        return cls.from_dict(config_dict, config_path.parent, env_vars, **overrides)
        
    @classmethod
    def from_dict(cls, config_dict: dict, config_dir: Path = None,
                  env_vars: dict = None, **overrides) -> 'IndexerConfig':
        logger = IndexerLogger.get_logger('core.config')
        
        from dotenv import load_dotenv
        load_dotenv()  
        
        log_with_context(logger, logging.DEBUG, "Processing configuration dictionary",
                       override_count=len(overrides),
                       has_env_vars=bool(env_vars))
        
        config_dict = {**config_dict, **overrides}
        env = env_vars or os.environ
        
        # Default config_dir to current working directory if not provided
        if config_dir is None:
            config_dir = Path.cwd() / "config"
            log_with_context(logger, logging.DEBUG, "Using default config directory", config_dir=str(config_dir))
        
        try:
            logger.debug("Creating storage configuration")
            storage = msgspec.convert(config_dict["storage"], type=StorageConfig)
            
            logger.debug("Creating GCS configuration")
            gcs = cls._create_gcs_config(env)

            log_with_context(logger, logging.DEBUG, "Processing contracts", contract_count=len(config_dict.get("contracts", {})))
            contracts = {
                str(address).lower(): cls._process_contract(contract_data, config_dir)
                for address, contract_data in config_dict.get("contracts", {}).items()
            }
            
            log_with_context(logger, logging.DEBUG, "Processing addresses", address_count=len(config_dict.get("addresses", {})))        
            addresses = {str(addr).lower(): msgspec.convert(data, type=AddressConfig)
                        for addr, data in config_dict.get("addresses", {}).items()}

            logger.debug("Processing tokens")
            tokens = {
                address.lower(): contract.token 
                for address, contract in contracts.items()
                if contract.token and contract.token.symbol and contract.token.decimals
            }

            logger.debug("Creating database configuration")
            database = cls._create_database_config(env)
            
            logger.debug("Creating RPC configuration")
            rpc = cls._create_rpc_config(env)
            
            logger.debug("Creating paths configuration")
            paths = cls._create_paths(config_dir)
            
            config = cls(
                name=config_dict["name"],
                version=config_dict["version"],
                storage=storage,
                gcs=gcs,
                contracts=contracts,
                addresses=addresses,
                tokens=tokens,
                database=database,
                rpc=rpc,
                paths=paths
            )
            
            log_with_context(logger, logging.INFO, "Configuration created successfully",
                           indexer_name=config.name,
                           indexer_version=config.version,
                           contract_count=len(contracts),
                           address_count=len(addresses))
            
            return config
            
        except KeyError as e:
            log_with_context(logger, logging.ERROR, "Missing required configuration key", missing_key=str(e))
            raise
        except Exception as e:
            log_with_context(logger, logging.ERROR, "Failed to process configuration",
                           error=str(e),
                           exception_type=type(e).__name__)
            raise

    @staticmethod
    def _process_contract(contract_data: dict, config_dir: Path) -> ContractConfig:
        logger = IndexerLogger.get_logger('core.config.contracts')
        
        contract_config = msgspec.convert(contract_data, type=ContractConfig)
        
        log_with_context(logger, logging.DEBUG, "Processing contract ABI",
                       contract_name=contract_config.name,
                       abi_dir=contract_config.decode.abi_dir,
                       abi_file=contract_config.decode.abi)
        
        abi_path = IndexerConfig._resolve_abi_path(contract_config, config_dir)
        
        if not abi_path.exists():
            log_with_context(logger, logging.ERROR, "ABI file not found",
                           contract_name=contract_config.name,
                           abi_path=str(abi_path))
            raise FileNotFoundError(f"ABI file not found: {abi_path}")
        
        try:
            with open(abi_path) as f:
                abi_data = msgspec.json.decode(f.read(), type=ABIConfig)
            
            contract_config.abi = abi_data.abi
            
            log_with_context(logger, logging.DEBUG, "Contract ABI loaded successfully",
                           contract_name=contract_config.name,
                           abi_functions=len([item for item in abi_data.abi if item.get("type") == "function"]),
                           abi_events=len([item for item in abi_data.abi if item.get("type") == "event"]))
            
        except Exception as e:
            log_with_context(logger, logging.ERROR, "Failed to load contract ABI",
                           contract_name=contract_config.name,
                           abi_path=str(abi_path),
                           error=str(e),
                           exception_type=type(e).__name__)
            raise
        
        return contract_config

    @staticmethod
    def _resolve_abi_path(contract_config: ContractConfig, config_dir: Path) -> Path:
        abi_base = config_dir / "abis"
        return abi_base / contract_config.decode.abi_dir / contract_config.decode.abi
        
    @staticmethod
    def _create_database_config(env: dict) -> DatabaseConfig:
        logger = IndexerLogger.get_logger('core.config.database')
        
        required_vars = ["INDEXER_DB_USER", "INDEXER_DB_PASSWORD", "INDEXER_DB_NAME", "INDEXER_DB_HOST"]
        missing_vars = [var for var in required_vars if var not in env]
        
        if missing_vars:
            log_with_context(logger, logging.ERROR, "Missing required database environment variables", missing_vars=missing_vars)
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        db_user = env["INDEXER_DB_USER"]
        db_password = env["INDEXER_DB_PASSWORD"]
        db_name = env["INDEXER_DB_NAME"]
        db_host = env["INDEXER_DB_HOST"]
        db_port = env.get("INDEXER_DB_PORT", "5432")
        
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        log_with_context(logger, logging.DEBUG, "Database configuration created",
                       db_host=db_host,
                       db_port=db_port,
                       db_name=db_name,
                       db_user=db_user)
        
        return DatabaseConfig(url=db_url)
        
    @staticmethod
    def _create_rpc_config(env: dict) -> RpcConfig:
        logger = IndexerLogger.get_logger('core.config.rpc')
        
        if "INDEXER_AVAX_RPC" not in env:
            log_with_context(logger, logging.ERROR, "Missing required RPC environment variable", missing_var="INDEXER_AVAX_RPC")
            raise ValueError("Missing required environment variable: INDEXER_AVAX_RPC")
        
        endpoint_url = env["INDEXER_AVAX_RPC"]
        
        log_with_context(logger, logging.DEBUG, "RPC configuration created", endpoint_url=endpoint_url)
        
        return RpcConfig(endpoint_url=endpoint_url)

    @staticmethod
    def _create_gcs_config(env: dict) -> GCSConfig:
        logger = IndexerLogger.get_logger('core.config.gcs')
        
        required_vars = ["INDEXER_GCS_PROJECT_ID", "INDEXER_GCS_BUCKET_NAME"]
        missing_vars = [var for var in required_vars if var not in env]
        
        if missing_vars:
            log_with_context(logger, logging.ERROR, "Missing required GCS environment variables", missing_vars=missing_vars)
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        project_id = env["INDEXER_GCS_PROJECT_ID"]
        bucket_name = env["INDEXER_GCS_BUCKET_NAME"]
        credentials_path = env.get("INDEXER_GCS_CREDENTIALS_PATH")  # Optional
        
        log_with_context(logger, logging.DEBUG, "GCS configuration created",
                       project_id=project_id,
                       bucket_name=bucket_name,
                       has_credentials=bool(credentials_path))
        
        return GCSConfig(
            project_id=project_id,
            bucket_name=bucket_name,
            credentials_path=credentials_path
        )

    @staticmethod
    def _create_paths(config_dir: Path) -> PathsConfig:       
        logger = IndexerLogger.get_logger('core.config.paths')
        
        # Assumes repository root is parent of config directory
        repo_root = config_dir.parent

        paths = PathsConfig(
            project_root=repo_root,
            indexer_root=repo_root / 'indexer',
            config_dir=config_dir,
            data_dir=repo_root / 'data',
            log_dir=repo_root / 'logs',
            abi_dir=repo_root / 'config' / 'abis'
        )
        
        logger.debug("Creating directory structure")
        for dir_name, dir_path in [
            ("data", paths.data_dir), 
            ("logs", paths.log_dir), 
            ("abi", paths.abi_dir)
        ]:
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                log_with_context(logger, logging.DEBUG, "Directory ensured", directory=dir_name, path=str(dir_path))
            except Exception as e:
                log_with_context(logger, logging.ERROR, "Failed to create directory",
                               directory=dir_name,
                               path=str(dir_path),
                               error=str(e))
                raise
        
        log_with_context(logger, logging.DEBUG, "Paths configuration created",
                       project_root=str(paths.project_root),
                       log_dir=str(paths.log_dir))
            
        return paths
    
    def get_tokens_of_interest(self) -> Dict[EvmAddress, TokenConfig]:
        tokens = set()
        for address, contract in self.contracts.items():
            if contract.token and contract.token.symbol and contract.token.decimals:
                tokens.add(EvmAddress(address.lower()))
        return tokens