# indexer/contracts/registry.py

from typing import Optional, Dict, List, Any
from web3 import Web3
from web3.contract import Contract

from ..core.config import IndexerConfig
from ..types import ContractConfig, EvmAddress


class ContractRegistry:
    """Registry for contract data and Web3 contract instances"""
    
    def __init__(self, config: IndexerConfig):
        self.config = config
        self.contracts: Dict[EvmAddress, ContractConfig] = {}
        self.web3_contracts: Dict[EvmAddress, Contract] = {}
        self._load_contracts_from_config()

    def _load_contracts_from_config(self):
        """Load contracts from the injected config"""
        for address, contract in self.config.contracts.items():
            self.contracts[address] = contract

    def get_contract(self, address: str) -> Optional[ContractConfig]:
        """Get contract config by address"""
        return self.contracts.get(address.lower())

    def get_web3_contract(self, address: str, w3: Web3) -> Optional[Contract]:
        """Get or create Web3 contract instance"""
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
        except Exception:
            return None

    def get_abi(self, address: str) -> Optional[List[Dict[str, Any]]]:
        """Get contract ABI by address"""
        contract = self.get_contract(address)
        return contract.abi if contract else None

    def has_contract(self, address: str) -> bool:
        """Check if address is a known contract"""
        return address.lower() in self.contracts

    def get_contract_count(self) -> int:
        """Get total number of registered contracts"""
        return len(self.contracts)