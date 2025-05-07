from typing import Optional
from pathlib import Path
import json
import msgspec
from msgspec import Struct

from ..model.types import EvmAddress
from ..utils.logging import setup_logger

class ABIConfig(Struct):
    address: EvmAddress
    abi: list

class ContractMetadata(Struct):
    name: str
    protocol: str
    type: str
    description: Optional[str] = None
    version: Optional[str] = None
    implementation: Optional[EvmAddress] = None

class ContractConfig(Struct):
    metadata: ContractMetadata
    abi: list

class ContractRegistry:
    _instance = None

    @classmethod
    def get_instance(cls, contracts_file=None, abi_directory=None):
        if cls._instance is None:
            if contracts_file is None or abi_directory is None:
                from indexer.indexer.env import env
                contracts_file = env.get_path('config_dir') / 'contracts.json'
                abi_directory = env.get_path('config_dir') / 'abis'
            cls._instance = cls(contracts_file, abi_directory)
        return cls._instance

    def __init__(self, contracts_file: str, abi_directory: str):
        self.contracts: dict[str, ContractConfig] = {}  # Contracts keyed by address
        self.logger = setup_logger(__name__)
        self._load_contracts(contracts_file, abi_directory)
        self.abi_decoder = msgspec.json.Decoder(type=ABIConfig)


    def _load_contracts(self, contracts_file: str, abi_directory: str):
        """Load contract registry and ABIs."""
        self.logger.info(f"Loading contracts from {contracts_file}")

        try:
            with open(contracts_file) as f:
                registry = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"Contract registry file not found: {contracts_file}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in contract registry: {e}")
            raise
            
        contract_count = 0
        error_count = 0

        for subdir, contracts in registry.items():
            for address, metadata in contracts.items():
                address = address.lower()
                abi_path = Path(abi_directory) / subdir / f"{address}.json"

                try:
                    contract_metadata = msgspec.convert(metadata, type=ContractMetadata)
                    self.logger.debug(f"Loading ABI for {address} ({contract_metadata.name})")

                    with open(abi_path) as f:
                        try:
                            contract_abi = self.abi_decoder.decode(f.read())
                            self.contracts[address] = ContractConfig(
                                metadata=contract_metadata,
                                abi=contract_abi.abi
                            )
                            contract_count += 1
                        except msgspec.ValidationError as e:
                            self.logger.warning(f"Invalid ABI format for {address}: {e}")
                            error_count += 1
                        except Exception as e:
                            self.logger.warning(f"Error loading ABI for {address}: {e}")
                            error_count += 1
                            
                except FileNotFoundError:
                    self.logger.warning(f"No ABI file found at {abi_path}")
                    error_count += 1
                except msgspec.ValidationError as e:
                    self.logger.warning(f"Invalid Contract metadata for {address}: {e}")
                    error_count += 1
        
        self.logger.info(f"Loaded {contract_count} contracts successfully. Errors: {error_count}")
    
    '''
    TODO: ADD CROSS VALIDATION TO INIT

    def _validate_configs(self, contracts: Dict[str,ContractConfig], abis: Dict[str, LogTemplate]) -> None:
        """Cross validate contracts and ABIs"""
        template_refs = set()
        for contract in contracts.values():
            for event in contract.events.values():
                template_refs.add(event.template)
        # Verify all referenced templates exist
        missing = template_refs - set(abis.keys())
        if missing:
            raise ConfigError(f"Missing ABI templates: {missing}")

    '''

    def get_contract(self, address: str) -> Optional[ContractConfig]:
        """Get full contract info by address."""
        return self.contracts.get(address.lower())
    
    def get_abi(self, address: str) -> Optional[list]:
        """Get contract ABI by address."""
        contract = self.contracts.get(address.lower())
        return contract.abi if contract else None