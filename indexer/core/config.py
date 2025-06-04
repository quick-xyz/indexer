# indexer/core/config.py

import msgspec
from msgspec import Struct
from typing import Dict, Optional
from pathlib import Path
import json
import os

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
)
from .container import IndexerContainer
from ..storage.gcs_handler import GCSHandler

class IndexerConfig(Struct):
    name: str
    version: str
    storage: StorageConfig
    database: DatabaseConfig
    rpc: RpcConfig
    gcs: GCSConfig
    contracts: Dict[EvmAddress, ContractConfig] = msgspec.field(default_factory=dict)
    addresses: Dict[EvmAddress, AddressConfig] = msgspec.field(default_factory=dict)
    paths: Optional[PathsConfig] = None
    
    @classmethod
    def from_file(cls, config_path: str, env_vars: dict = None, **overrides) -> 'IndexerConfig':
        config_path = Path(config_path)
        with open(config_path) as f:
            config_dict = json.load(f)
            
        return cls.from_dict(config_dict, env_vars, **overrides)
        
    @classmethod
    def from_dict(cls, config_dict: dict, config_dir: Path = None,
                  env_vars: dict = None, **overrides) -> 'IndexerConfig':
        from dotenv import load_dotenv
        load_dotenv()  
        
        config_dict = {**config_dict, **overrides}
        env = env_vars or os.environ
        
        # Default config_dir to current working directory if not provided
        if config_dir is None:
            config_dir = Path.cwd() / "config"
        
        storage = msgspec.convert(config_dict["storage"], type=StorageConfig)
        gcs = cls._create_gcs_config(env)

        contracts = {
            str(address).lower(): cls._process_contract(contract_data,config_dir)
            for address, contract_data in config_dict["contracts"].items()
        }
                
        addresses = {str(addr).lower(): msgspec.convert(data, type=AddressConfig)
                    for addr, data in config_dict.get("addresses", {}).items()}
                        
        database = cls._create_database_config(env)
        
        rpc = cls._create_rpc_config(env)
        
        paths = cls._create_paths(config_dir)
        
        return cls(
            name=config_dict["name"],
            version=config_dict["version"],
            storage=storage,
            gcs=gcs,
            contracts=contracts,
            addresses=addresses,
            database=database,
            rpc=rpc,
            paths=paths
        )

    @staticmethod
    def _process_contract(contract_data: dict, config_dir: Path) -> ContractConfig:
        contract_config = msgspec.convert(contract_data, type=ContractConfig)
        
        abi_path = IndexerConfig._resolve_abi_path(contract_config,config_dir)
        with open(abi_path) as f:
            abi_data = msgspec.json.decode(f.read(), type=ABIConfig)
        
        contract_config.abi = abi_data.abi
        return contract_config

    @staticmethod
    def _resolve_abi_path(contract_config: ContractConfig,config_dir: Path) -> Path:
        abi_base = config_dir /  "abis"
        return abi_base / contract_config.decode.abi_dir / contract_config.decode.abi
        
    @staticmethod
    def _create_database_config(env: dict) -> DatabaseConfig:
        db_user = env["INDEXER_DB_USER"]
        db_password = env["INDEXER_DB_PASSWORD"]
        db_name = env["INDEXER_DB_NAME"]
        db_host = env["INDEXER_DB_HOST"]
        db_port = env.get("INDEXER_DB_PORT", "5432")
        
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return DatabaseConfig(url=db_url)
        
    @staticmethod
    def _create_rpc_config(env: dict) -> RpcConfig:
        endpoint_url = env["INDEXER_AVAX_RPC"]
        return RpcConfig(endpoint_url=endpoint_url)

    @staticmethod
    def _create_gcs_config(env: dict) -> GCSConfig:
        project_id = env["INDEXER_GCS_PROJECT_ID"]
        bucket_name = env["INDEXER_GCS_BUCKET_NAME"]
        credentials_path = env.get("INDEXER_GCS_CREDENTIALS_PATH")  # Optional
        return GCSConfig(
            project_id=project_id,
            bucket_name=bucket_name,
            credentials_path=credentials_path
        )

    @staticmethod
    def _create_paths(config_dir: Path) -> PathsConfig:       
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
        
        for dir_path in [paths.data_dir, paths.log_dir, paths.abi_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        return paths