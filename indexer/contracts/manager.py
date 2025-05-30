# indexer/contracts/manager.py  

from web3 import Web3
from web3.contract import Contract
from typing import Optional, Dict, Any

from .registry import ContractRegistry


class ContractManager:
    """
    Manages Web3 contract instances with caching
    """
    
    def __init__(self, registry: ContractRegistry):
        self.registry = registry
        self.w3 = Web3()  # No provider needed for ABI decoding
        self.contract_cache: Dict[str, Contract] = {}

    def get_contract(self, address: str) -> Optional[Contract]:
        """Get or create Web3 contract instance"""
        address = address.lower()

        if address in self.contract_cache:
            return self.contract_cache[address]

        try:
            contract = self.registry.get_web3_contract(address, self.w3)
            if contract:
                self.contract_cache[address] = contract
                return contract
        except Exception:
            pass

        return None

    def has_contract(self, address: str) -> bool:
        """Check if contract exists in registry"""
        return self.registry.has_contract(address.lower())

    def call_function(self, address: str, function_name: str, *args, **kwargs) -> Any:
        """Call a contract function"""
        contract = self.get_contract(address)
        if not contract:
            raise ValueError(f"Contract not found: {address}")

        try:
            func = getattr(contract.functions, function_name)
            return func(*args, **kwargs).call()
        except Exception as e:
            raise ValueError(f"Error calling function {function_name}: {str(e)}")

    def clear_cache(self) -> None:
        """Clear the contract cache"""
        self.contract_cache.clear()