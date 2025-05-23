from typing import Dict, Type, Optional, List
from dataclasses import dataclass

from ..config.config_manager import config

# TRANSFORMER MODULES

from .transformers.pools.lb_pair import LbPairTransformer
from .transformers.pools.lfj_pool import LfjPoolTransformer
from .transformers.pools.phar_clpool import PharClpoolTransformer
from .transformers.pools.phar_pair import PharPairTransformer


TRANSFORMER_CLASSES = {
    "LBPairTransformer": LbPairTransformer,
    "LfjPoolTransformer": LfjPoolTransformer,
    "PharClpoolTransformer": PharClpoolTransformer,
    "PharPairTransformer": PharPairTransformer,
}


@dataclass
class ContractConfig:
    """Configuration for a contract transformer."""
    transformer_class: Type
    priority: int = 0  # Higher priority = processed later (for dependency handling)
    active: bool = True


class TransformerRegistry:
    def __init__(self):
        self._contracts: Dict[str, ContractConfig] = {}
        self._initialized = False
    
    def register_contract(self, contract_address: str, transformer_class: Type, priority: int = 0):
        self._contracts[contract_address.lower()] = ContractConfig(
            transformer_class=transformer_class,
            priority=priority
        )
    
    def get_transformer_class(self, contract_address: str) -> Optional[Type]:
        config = self._contracts.get(contract_address.lower())
        return config.transformer_class if config and config.active else None
    
    def get_contracts_by_priority(self) -> List[tuple[str, ContractConfig]]:
        active_contracts = [
            (address, config) for address, config in self._contracts.items()
            if config.active
        ]
        return sorted(active_contracts, key=lambda x: x[1].priority)
    
    def get_all_contracts(self) -> Dict[str, ContractConfig]:
        return self._contracts.copy()
    
    def setup(self):
        if self._initialized:
            return
        
        for address, transformer_config in config.transformers.items():
            if transformer_config.active:
                transformer_class = TRANSFORMER_CLASSES.get(transformer_config.name)
                if transformer_class:
                    self.register_contract(address, transformer_class, transformer_config.priority)
                else:
                    print(f"Warning: Unknown transformer '{transformer_config.name}' for contract {address}")
        
        self._initialized = True


transformer_registry = TransformerRegistry()