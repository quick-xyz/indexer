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
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_manager=None):
        if hasattr(self, '_initialized') and self._initialized:
            return
        self.config_manager = config_manager or config
        self.contracts: Dict[str, ContractWithABI] = {}  # Contracts keyed by address
        self.web3_contracts: Dict[str, Contract] = {}    # Web3 Contract instances
        self.logger = setup_logger(__name__)
        self.abi_decoder = msgspec.json.Decoder(type=ABIConfig)
        self._load_contracts_from_config()
        self._initialized = True

    def load_contracts(self):
        self._load_contracts_from_config()
        self.logger.info(f"Reloaded contracts from config manager")

    def _load_contracts_from_config(self):
        """Load contracts from the config manager."""
        if not self.config_manager:
            self.logger.warning("No config manager provided, can't load contracts")
            return

        for address, contract in self.config_manager.contracts.items():
            self.contracts[address] = contract

        self.logger.info(f"Loaded {len(self.config_manager.contracts)} contracts from config manager")
    

    def get_contract(self, address: str) -> Optional[ContractWithABI]:
        """Get full contract info by address."""
        return self.contracts.get(address.lower())
    

    def get_web3_contract(self, address: str, w3: Web3) -> Optional[Contract]:
        address = address.lower()
        
        if address in self.web3_contracts:
            return self.web3_contracts[address]
            
        contract_data = self.get_contract(address)
        if not contract_data or not contract_data.abi:
            return None
            
        try:
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(address),
                abi=contract_data.abi
            )
            
            self.web3_contracts[address] = contract
            
            return contract
        except Exception as e:
            self.logger.error(f"Error creating Web3 contract for {address}: {e}")
            return None

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
        self.config_manager.register_contract_abi(address, abi, name, contract_type)
        contract = self.config_manager.get_contract(address)

        if contract:
            self.contracts[address] = contract
            if address in self.web3_contracts:
                del self.web3_contracts[address]
            self.logger.info(f"Registered contract {name or address[:8]} at {address}")

contract_registry = ContractRegistry(config_manager=config)