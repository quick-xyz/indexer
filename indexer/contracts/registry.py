# indexer/contracts/registry.py

from typing import Optional, Dict, List, Any
from web3 import Web3
from web3.contract import Contract

from ..core.indexer_config import IndexerConfig
from ..types import EvmAddress
from .abi_loader import ABILoader


class ContractRegistry:
    """Registry for contract data and Web3 contract instances with filesystem ABI loading"""
    
    def __init__(self, config: IndexerConfig, abi_loader: ABILoader):
        self.config = config
        self.abi_loader = abi_loader
        self.contracts: Dict[EvmAddress, Any] = {}  # Store database Contract objects
        self.web3_contracts: Dict[EvmAddress, Contract] = {}
        self._abi_cache: Dict[EvmAddress, Optional[List[Dict[str, Any]]]] = {}
        self._load_contracts_from_config()

    def _load_contracts_from_config(self):
        """Load contracts from the injected config"""
        for address, contract in self.config.contracts.items():
            self.contracts[address] = contract

    def get_contract(self, address: str) -> Optional[Any]:
        """Get contract config by address"""
        return self.contracts.get(address.lower())

    def get_abi(self, address: str) -> Optional[List[Dict[str, Any]]]:
        """Get contract ABI by address, loading from filesystem if needed"""
        address = address.lower()
        
        # Check cache first
        if address in self._abi_cache:
            return self._abi_cache[address]
        
        contract = self.get_contract(address)
        if not contract:
            self._abi_cache[address] = None
            return None
        
        abi_dir = getattr(contract, 'abi_dir', None)
        abi_file = getattr(contract, 'abi_file', None)

        if not abi_dir or not abi_file:
            self._abi_cache[address] = None
            return None

        abi = self.abi_loader.load_abi(abi_dir, abi_file)
        self._abi_cache[address] = abi
        return abi

    def get_web3_contract(self, address: str, w3: Web3) -> Optional[Contract]:
        """Get or create Web3 contract instance"""
        address = address.lower()
        
        if address in self.web3_contracts:
            return self.web3_contracts[address]
        
        abi = self.get_abi(address)
        if not abi:
            return None
            
        try:
            contract = w3.eth.contract(
                address=Web3.to_checksum_address(address),
                abi=abi
            )
            
            self.web3_contracts[address] = contract
            return contract
        except Exception:
            return None

    def has_contract(self, address: str) -> bool:
        """Check if address is a known contract"""
        return address.lower() in self.contracts

    def get_contract_count(self) -> int:
        """Get total number of registered contracts"""
        return len(self.contracts)
    
    def clear_caches(self):
        """Clear all caches"""
        self.web3_contracts.clear()
        self._abi_cache.clear()
        self.abi_loader.clear_cache()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "web3_contracts_cached": len(self.web3_contracts),
            "abi_cache_size": len(self._abi_cache),
            "abi_loader_stats": self.abi_loader.get_cache_stats()
        }