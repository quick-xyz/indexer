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
    RPCConfig, 
    StorageConfig, 
    PathsConfig,
)


class IndexerConfig(Struct):
    name: str
    version: str
    storage: StorageConfig
    contracts: Dict[EvmAddress, ContractConfig] = msgspec.field(default_factory=dict)
    addresses: Dict[EvmAddress, AddressConfig] = msgspec.field(default_factory=dict)
    database: DatabaseConfig
    rpc: RPCConfig
    paths: Optional[PathsConfig] = None
    
    @classmethod
    def from_file(cls, config_path: str, env_vars: dict = None, **overrides) -> 'IndexerConfig':
        config_path = Path(config_path)
        with open(config_path) as f:
            config_dict = json.load(f)
            
        return cls.from_dict(config_dict, env_vars, **overrides)
        
    @classmethod
    def from_dict(cls, config_dict: dict, env_vars: dict = None, **overrides) -> 'IndexerConfig':
        config_dict = {**config_dict, **overrides}
        env = env_vars or os.environ
        
        storage = msgspec.convert(config_dict["storage"], type=StorageConfig)
        
        contracts = {
            address.lower(): cls._process_contract(address, contract_data)
            for address, contract_data in config_dict["contracts"].items()
        }
                
        addresses = {addr.lower(): msgspec.convert(data, type=AddressConfig)
                    for addr, data in config_dict.get("addresses", {}).items()}
                        
        database = cls._create_database_config(env)
        
        rpc = cls._create_rpc_config(env)
        
        paths = cls._create_paths()
        
        return cls(
            name=config_dict["name"],
            version=config_dict["version"],
            storage=storage,
            contracts=contracts,
            addresses=addresses,
            database=database,
            rpc=rpc,
            paths=paths
        )

    @classmethod
    def _process_contract(cls, address: EvmAddress, contract_data: dict) -> ContractConfig:
        contract_config = msgspec.convert(contract_data, type=ContractConfig)
        
        abi_path = cls._resolve_abi_path(contract_config)
        with open(abi_path) as f:
            abi_data = msgspec.json.decode(f.read(), type=ABIConfig)
        
        contract_config.abi = abi_data.abi
        return contract_config

    @staticmethod
    def _resolve_abi_path(contract_config: ContractConfig) -> Path:
        current_file = Path(__file__).resolve()
        indexer_root = current_file.parents[1]
        abi_base = indexer_root / "config" / "abis"
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
    def _create_rpc_config(env: dict) -> RPCConfig:
        endpoint_url = env["INDEXER_AVAX_RPC"]
        return RPCConfig(endpoint_url=endpoint_url)
        
    @staticmethod
    def _create_paths() -> PathsConfig:
        current_file = Path(__file__).resolve()
        indexer_root = current_file.parents[1]
        project_root = indexer_root.parent
        
        paths = PathsConfig(
            project_root=project_root,
            indexer_root=indexer_root,
            config_dir=indexer_root / 'config',
            data_dir=project_root / 'data',
            log_dir=project_root / 'logs',
            abi_dir=indexer_root / 'config' / 'abis'
        )
        
        for dir_path in [paths.data_dir, paths.log_dir, paths.abi_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        return paths