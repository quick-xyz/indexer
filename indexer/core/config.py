import msgspec
from msgspec import Struct, Raw
from typing import Dict, Optional, List
from pathlib import Path
import json
import os


class IndexerConfig(Struct, frozen=True):
    name: str
    version: str
    storage: StorageConfig
    contracts: Dict[EvmAddress, ContractWithABI]
    addresses: Dict[EvmAddress, AddressConfig] = msgspec.field(default_factory=dict)
    tokens: Dict[EvmAddress, TokenConfig] = msgspec.field(default_factory=dict)
    transformers: Dict[str, TransformerConfig] = msgspec.field(default_factory=dict)
    database: DatabaseConfig
    rpc: RPCConfig
    paths: Optional[PathsConfig] = None
    
    @classmethod
    def from_file(cls, config_path: str, env_vars: dict = None, **overrides) -> 'IndexerConfig':
        """Load configuration from JSON file"""
        config_path = Path(config_path)
        with open(config_path) as f:
            config_dict = json.load(f)
            
        return cls.from_dict(config_dict, env_vars, **overrides)
        
    @classmethod
    def from_dict(cls, config_dict: dict, env_vars: dict = None, **overrides) -> 'IndexerConfig':
        """Load configuration from dictionary with optional environment variables"""
        # Apply overrides
        config_dict = {**config_dict, **overrides}
        
        # Use provided env_vars or fall back to os.environ
        env = env_vars or os.environ
        
        # Parse storage config
        storage = msgspec.convert(config_dict["storage"], type=StorageConfig)
        
        # Parse contracts with ABIs
        contracts = {}
        for address, contract_data in config_dict["contracts"].items():
            contract_config = msgspec.convert(contract_data, type=ContractConfig)
            
            # Load ABI from file
            abi_path = cls._resolve_abi_path(contract_config)
            with open(abi_path) as f:
                abi_data = msgspec.json.decode(f.read(), type=ABIConfig)
                
            contracts[address.lower()] = ContractWithABI(
                contract_info=contract_config,
                abi=abi_data.abi
            )
                
        # Parse other configurations
        addresses = {addr.lower(): msgspec.convert(data, type=AddressConfig)
                    for addr, data in config_dict.get("addresses", {}).items()}
                
        tokens = {addr.lower(): msgspec.convert(data, type=TokenConfig)
                 for addr, data in config_dict.get("tokens", {}).items()}
                
        transformers = {}
        for addr, contract_data in config_dict["contracts"].items():
            if "transform" in contract_data:
                transformer_config = msgspec.convert(contract_data["transform"], type=TransformerConfig)
                transformers[addr.lower()] = transformer_config
        
        # Create database config from environment
        database = cls._create_database_config(env)
        
        # Create RPC config from environment  
        rpc = cls._create_rpc_config(env)
        
        # Create paths
        paths = cls._create_paths()
        
        return cls(
            name=config_dict["name"],
            version=config_dict["version"],
            storage=storage,
            contracts=contracts,
            addresses=addresses,
            tokens=tokens,
            transformers=transformers,
            database=database,
            rpc=rpc,
            paths=paths
        )
        
    @staticmethod
    def _resolve_abi_path(contract_config: ContractConfig) -> Path:
        """Resolve ABI file path"""
        current_file = Path(__file__).resolve()
        indexer_root = current_file.parents[1]
        abi_base = indexer_root / "config" / "abis"
        return abi_base / contract_config.decode.abi_dir / contract_config.decode.abi
        
    @staticmethod
    def _create_database_config(env: dict) -> DatabaseConfig:
        """Create database config from environment variables"""
        db_user = env["INDEXER_DB_USER"]
        db_password = env["INDEXER_DB_PASSWORD"]
        db_name = env["INDEXER_DB_NAME"]
        db_host = env["INDEXER_DB_HOST"]
        db_port = env.get("INDEXER_DB_PORT", "5432")
        
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return DatabaseConfig(url=db_url)
        
    @staticmethod
    def _create_rpc_config(env: dict) -> RPCConfig:
        """Create RPC config from environment variables"""
        endpoint_url = env["INDEXER_AVAX_RPC"]
        return RPCConfig(endpoint_url=endpoint_url)
        
    @staticmethod
    def _create_paths() -> PathsConfig:
        """Create standard paths configuration"""
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
        
        # Ensure directories exist
        for dir_path in [paths.data_dir, paths.log_dir, paths.abi_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        return paths