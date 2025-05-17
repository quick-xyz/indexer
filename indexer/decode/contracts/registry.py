from typing import Optional, Dict, List, Any
import json
import msgspec
from msgspec import Struct
from web3 import Web3
from web3.contract import Contract

from ..interfaces import ContractRegistryInterface
from ...utils.logging import setup_logger

from ...config.types import (
    ABIConfig, 
    ContractConfig,
    ContractWithABI,
)
from ...config.config_manager import config


class ContractRegistry(ContractRegistryInterface):
    def __init__(self, config: ConfigManager):
        self.contracts: Dict[str, ContractWithABI] = {}  # Contracts keyed by address
        self.logger = setup_logger(__name__)
        self.abi_decoder = msgspec.json.Decoder(type=ABIConfig)
        self._load_contracts_from_config()

    def _load_contracts_from_config(self):
        """Load contracts from the config manager."""
        # Copy contracts from config manager
        for address, contract in config.contracts.items():
            self.contracts[address] = contract

        self.logger.info(f"Loaded {len(config.contracts)} contracts from config manager")
    

    def get_contract(self, address: str) -> Optional[ContractWithABI]:
        """Get full contract info by address."""
        return self.contracts.get(address.lower())
    

    def get_abi(self, address: str) -> Optional[list]:
        """Get contract ABI by address."""
        contract = self.get_contract(address)
        return contract.abi if contract else None
    

    def has_contract(self, address: str) -> bool:
        """Check if address is a known contract."""
        return address.lower() in self.contracts
    

    def get_contract_count(self) -> int:
        return len(self.contracts)


    def register_contract(self, address: str, abi: List[Dict[str, Any]], name: str = None, contract_type: str = "unknown") -> None:
        """
        Register a contract in the registry.
        """
        address = address.lower()
        config.register_contract_abi(address, abi, name, contract_type)
        contract = config.get_contract(address)

        if contract:
            self.contracts[address] = contract
            self.logger.info(f"Registered contract {name or address[:8]} at {address}")