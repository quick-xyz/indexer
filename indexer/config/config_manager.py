from dotenv import load_dotenv
from pathlib import Path
import msgspec
from typing import Any,  Optional, List, Dict
import json
import logging
import sys
import os

from .types import (
    StorageConfig,
    TokenConfig,
    AddressConfig,
    ABIConfig,
    ContractConfig,
    ContractWithABI,
    TransformerConfig,
)


class ConfigManager:
    """
    Manages configuration for the blockchain indexer.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized') and self._initialized:
            return

        # Initialize dictionaries for different config types
        self.tokens = {}
        self.addresses = {}
        self.contracts = {}
        self.config_dict = {}
        self.transformers = {}

        # Load environment variables and config
        self._load_env_vars()
        self._configure_logging()
        self._init_paths()
        self._load_config()
        self._initialized = True


    def _load_env_vars(self):
        """Load environment variables from .env file."""
        load_dotenv()


    def _configure_logging(self):
        """Configure logging for the configuration manager."""
        # This initializes a basic logger for bootstrap purposes
        # The proper logger will be initialized later by the logger module
        self.logger = logging.getLogger("indexer.config")
        
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            
            # Set level from environment
            level_name = os.getenv("LOG_LEVEL", "INFO").upper()
            valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            level = level_name if level_name in valid_levels else "INFO"
            self.logger.setLevel(getattr(logging, level))


    def _init_paths(self):
        """Initialize directory paths."""

        current_file = Path(__file__).resolve()
        indexer_root = current_file.parents[1]
        project_root = indexer_root.parents[1]

        self.paths = {
            "project_root": project_root,
            "indexer_root": indexer_root,
            "config_dir": indexer_root / 'config',
            'data_dir': project_root / 'data',
            'log_dir': project_root / 'logs',
            'abi_dir': indexer_root / 'config' / 'abis',
        }  

        for path_name in ['data_dir', 'log_dir', 'abi_dir']:
            os.makedirs(self.paths[path_name], exist_ok=True)


    def _load_config(self):
        """Load all configuration elements."""
        self._load_config_json()
        if self.config_dict:
            self.storage = self._load_storage()
            self._load_tokens()
            self._load_addresses()
            self._load_contracts()
            self._load_transformers()



    def _load_config_json(self, config_path=None):
        if config_path is None:
            config_path = self.paths['config_dir'] / 'config.json'
        else:
            config_path = Path(config_path)
        
        self.logger.info(f"Loading configuration from {config_path}")
        try:
            with open(config_path) as f:
                self.config_dict = json.load(f)
            self.logger.info(f"Loaded configuration json file: {config_path}")   
        except FileNotFoundError:
            self.logger.warning(f"Configuration file not found: {config_path}")
            self.config_dict = {}
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in tokens file: {e}")
            raise
        return None


    def _load_storage(self):
        self.logger.info(f"Loading storage config from {self.config_dict}")
        self.storage = msgspec.convert(self.config_dict["storage"], type=StorageConfig)
        

    def _load_tokens(self):
        self.logger.info(f"Loading tokens config from {self.config_dict}")
        tokens_dict = self.config_dict["tokens"]
        for address, data in tokens_dict.items():
            address = address.lower()
            try:
                token_config = msgspec.convert(data, type=TokenConfig)
                self.tokens[address] = token_config
            except msgspec.ValidationError as e:
                self.logger.warning(f"Invalid token config for {address}: {e}")


    def _load_addresses(self):
        self.logger.info(f"Loading addresses config from {self.config_dict}")
        address_dict = self.config_dict["addresses"]
        for address, data in address_dict.items():
            address = address.lower()
            try:
                address_config = msgspec.convert(data, type=AddressConfig)
                self.addresses[address] = address_config
            except msgspec.ValidationError as e:
                self.logger.warning(f"Invalid addresses config for {address}: {e}")

    def _load_transformers(self):
        self.logger.info(f"Loading transformers config from {self.config_dict}")

        contracts_dict = self.config_dict.get("contracts", {})

        for address, contract_data in contracts_dict.items():
            address = address.lower()

            if "transformer" in contract_data:
                try:
                    transformer_config = msgspec.convert(contract_data["transformer"], type=TransformerConfig)
                    self.transformers[address] = transformer_config
                except msgspec.ValidationError as e:
                    self.logger.warning(f"Invalid transformer config for {address}: {e}")


    def _load_contracts(self):  
        self.logger.info(f"Loading contracts config from {self.config_dict}")
        contracts_dict = self.config_dict["contracts"]
        contract_count = 0
        error_count = 0

        for address, data in contracts_dict.items():
            address = address.lower()
            try:
                contract_config = msgspec.convert(data, type=ContractConfig)
                abi_path = self.paths['abi_dir'] / contract_config.abi_dir / contract_config.abi

                try:
                    with open(abi_path) as f:
                        abi_data = msgspec.json.decode(f.read(), type=ABIConfig)
                        self.contracts[address] = ContractWithABI(
                            contract_info=contract_config,
                            abi=abi_data.abi
                        )
                        contract_count += 1
                except FileNotFoundError:
                    self.logger.warning(f"ABI file not found: {abi_path}")
                    error_count += 1
                except msgspec.ValidationError as e:
                    self.logger.warning(f"Invalid ABI for {address}: {e}")
                    error_count += 1
                    
            except msgspec.ValidationError as e:
                self.logger.warning(f"Invalid contract config for {address}: {e}")
                error_count += 1
                
        self.logger.info(f"Loaded {contract_count} contracts successfully. Errors: {error_count}")
    

    def get_env(self, name: str, default=None) -> Any:
        return os.getenv(name, default)

    def get_path(self, name: str) -> Optional[Path]:
        return self.paths.get(name)

    def get_environment(self)-> str:
        return self.get_env("ENVIRONMENT", "development").lower()
    
    def get_log_level(self):
        """Get configured log level."""
        level = self.get_env("LOG_LEVEL", "INFO").upper()
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        return level if level in valid_levels else "INFO"

    def get_db_url(self):
        """Get database connection URL from environment."""

        db_user = self.get_env("INDEXER_DB_USER")
        db_pass = self.get_env("INDEXER_DB_PASSWORD")
        db_name = self.get_env("INDEXER_DB_NAME")
        db_host = self.get_env("INDEXER_DB_HOST")
        db_port = self.get_env("INDEXER_DB_PORT", "5432")
        
        return (
            f"postgresql://{db_user}:{db_pass}"
            f"@{db_host}:{db_port}"
            f"/{db_name}"
        )
 
    def get_contract(self, address: str) -> Optional[ContractWithABI]:
        """Get contract by address."""
        return self.contracts.get(address.lower())
    
    def get_abi(self, address: str) -> Optional[List[Dict[str, Any]]]:
        """Get contract ABI by address."""
        contract = self.contracts.get(address.lower())
        return contract.abi if contract else None
    
    def has_contract(self, address: str) -> bool:
        """Check if address is a known contract."""
        return address.lower() in self.contracts
    
    def register_contract_abi(self, address: str, abi: List[Dict[str, Any]], 
                            name: str = None, contract_type: str = "unknown") -> None:
        """Register a contract ABI."""
        address = address.lower()
        
        # Create contract config
        contract_info = ContractConfig(
            name=name or f"Contract-{address[:8]}",
            project="runtime-added",
            type=contract_type,
            abi_dir="runtime",
            abi="runtime.json",
        )
        
        # Create contract with ABI
        self.contracts[address] = ContractWithABI(
            contract_info=contract_info,
            abi=abi
        )
        
        # Update config dict
        if "contracts" not in self.config_dict:
            self.config_dict["contracts"] = {}
            
        self.config_dict["contracts"][address] = {
            "name": contract_info.name,
            "project": contract_info.project,
            "type": contract_info.type,
            "abi_dir": contract_info.abi_dir,
            "abi": contract_info.abi
        }
        
        self.logger.info(f"Registered contract {name or address[:8]} at {address}")
    
config = ConfigManager()