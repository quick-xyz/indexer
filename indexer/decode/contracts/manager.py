from web3 import Web3
from web3.contract import Contract
from typing import Optional, Dict

from .registry import ContractRegistry

class ContractManager:
    """
    Caches Web3 contract instances (subset of the registry)
    """
    def __init__(self, registry: ContractRegistry):
        self.registry = registry
        self.w3 = Web3()  # No provider needed for ABI decoding
        self.contracts: Dict[str, Contract] = {}  # address -> Contract instance
    
    def get_contract(self, address: str) -> Optional[Contract]:
        """
        Get or create Web3 contract instance for address in registry.
        """
        address = address.lower()
        if address not in self.contracts:
            contract_info = self.registry.get_contract(address)
            if contract_info:
                self.contracts[address] = self.w3.eth.contract(
                    address=Web3.to_checksum_address(address),
                    abi=contract_info.abi
                )
        return self.contracts.get(address)
    
    def has_contract(self, address: str) -> bool:
        """
        Check if address is a known contract in the registry.
        """
        return self.registry.get_contract(address.lower()) is not None