from typing import Optional, Dict, Union
from pathlib import Path
import json
import msgspec
from msgspec import Struct

from ..interfaces import ContractRegistryInterface
from ..model.types import EvmAddress
from ...utils.logging import setup_logger

class ABIConfig(Struct):
    abi: list

class TokenConfig(Struct):
    name: str
    symbol: str
    type: str
    decimals: int

class AddressConfig(Struct):
    type: str
    name: str
    grouping: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[list[str]] = None    

class ContractConfig(Struct):
    name: str
    project: str
    type: str
    abi_dir: str
    abi: str
    description: Optional[str] = None
    version: Optional[str] = None
    implementation: Optional[EvmAddress] = None

class ContractWithABI(Struct):
    contract_info: ContractConfig
    abi: list

class ContractRegistry(ContractRegistryInterface):
    _instance = None

    @classmethod
    def get_instance(cls, config_dir=None):
        if cls._instance is None:
            if config_dir is None:
                from indexer.indexer.env import env
                config_dir = env.get_path('config_dir')
            cls._instance = cls(config_dir)
        return cls._instance

    def __init__(self, config_dir: Union[str, Path]):
        self.config_dir = Path(config_dir)
        self.contracts: Dict[str, ContractWithABI] = {}  # Contracts keyed by address
        self.tokens: Dict[str, TokenConfig] = {}  # Tokens keyed by address
        self.addresses: Dict[str, AddressConfig] = {}  # Addresses keyed by address
        self.logger = setup_logger(__name__)
        self.abi_decoder = msgspec.json.Decoder(type=ABIConfig)
        self._load_addresses()
        self._load_tokens()
        self._load_contracts()


    def _load_addresses(self):
        addresses_file = self.config_dir / 'addresses.json'
        self.logger.info(f"Loading addresses from {addresses_file}")

        try:
            with open(addresses_file) as f:
                addresses_data = json.load(f)
                
            for address, data in addresses_data.items():
                address = address.lower()
                try:
                    address_config = msgspec.convert(data, type=AddressConfig)
                    self.addresses[address] = address_config
                except msgspec.ValidationError as e:
                    self.logger.warning(f"Invalid address metadata for {address}: {e}")
                    
            self.logger.info(f"Loaded {len(self.addresses)} addresses")
                
        except FileNotFoundError:
            self.logger.warning(f"Addresses file not found: {addresses_file}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in addresses file: {e}")
            raise

    def _load_tokens(self):
        """Load token registry."""
        tokens_file = self.config_dir / 'tokens.json'
        self.logger.info(f"Loading tokens from {tokens_file}")

        try:
            with open(tokens_file) as f:
                tokens_data = json.load(f)
                
            for address, data in tokens_data.items():
                address = address.lower()
                try:
                    token_config = msgspec.convert(data, type=TokenConfig)
                    self.tokens[address] = token_config
                except msgspec.ValidationError as e:
                    self.logger.warning(f"Invalid token metadata for {address}: {e}")
                    
            self.logger.info(f"Loaded {len(self.tokens)} tokens")
                
        except FileNotFoundError:
            self.logger.warning(f"Tokens file not found: {tokens_file}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in tokens file: {e}")
            raise

    def _load_contracts(self):
        """Load contract registry and ABIs."""
        contracts_file = self.config_dir / 'contracts.json'
        abi_directory = self.config_dir / 'abis'
        
        self.logger.info(f"Loading contracts from {contracts_file}")

        try:
            with open(contracts_file) as f:
                contracts_data = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Contract registry file not found: {contracts_file}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in contract registry: {e}")
            raise
            
        contract_count = 0
        error_count = 0

        for address, data in contracts_data.items():
            address = address.lower()
            try:
                contract_config = msgspec.convert(data, type=ContractConfig)
                
                # Load the ABI file
                abi_path = abi_directory / contract_config.abi_dir / contract_config.abi
                
                self.logger.debug(f"Loading ABI for {address} ({contract_config.name}) from {abi_path}")

                try:
                    with open(abi_path) as f:
                        abi_data = self.abi_decoder.decode(f.read())
                        
                       
                    # Store contract info and ABI
                    self.contracts[address] = ContractWithABI(
                        contract_info=contract_config,
                        abi=abi_data.abi   
                    )

                    contract_count += 1
                        
                except FileNotFoundError:
                    self.logger.warning(f"No ABI file found at {abi_path}")
                    error_count += 1
                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON in ABI file for {address}: {e}")
                    error_count += 1
                    
            except msgspec.ValidationError as e:
                self.logger.warning(f"Invalid contract metadata for {address}: {e}")
                error_count += 1
        
        self.logger.info(f"Loaded {contract_count} contracts successfully. Errors: {error_count}")

    def get_contract(self, address: str) -> Optional[ContractWithABI]:
        """Get full contract info by address."""
        return self.contracts.get(address.lower())
    
    def get_abi(self, address: str) -> Optional[list]:
        """Get contract ABI by address."""
        contract = self.contracts.get(address.lower())
        return contract.abi if contract else None

    def get_token(self, address: str) -> Optional[TokenConfig]:
        """Get token info by address."""
        return self.tokens.get(address.lower())
    
    def get_address(self, address: str) -> Optional[AddressConfig]:
        """Get address info by address."""
        return self.addresses.get(address.lower())
    
    def has_contract(self, address: str) -> bool:
        """Check if address is a known contract."""
        address = address.lower()
        return address in self.contracts and address in self.abis