from typing import Dict, Type, Optional, List, Tuple
from dataclasses import dataclass, field

from ..config.config_manager import config
from ..config.types import TransformerConfig
from ..decode.model.types import EvmAddress

# TRANSFORMER MODULES

from .transformers.pools.lb_pair import LbPairTransformer
from .transformers.pools.lfj_pool import LfjPoolTransformer
from .transformers.pools.phar_clpool import PharClpoolTransformer
from .transformers.pools.phar_pair import PharPairTransformer

from .transformers.routers.lfj_aggregator import LfjAggregatorTransformer
from .transformers.routers.phar_cl_manager import PharClManagerTransformer

from .transformers.wesmol.auction import AuctionTransformer
from .transformers.wesmol.farm import FarmTransformer
from .transformers.wesmol.wrapper import WesmolWrapperTransformer

TRANSFORMER_CLASSES = {
    "LBPairTransformer": LbPairTransformer,
    "LfjPoolTransformer": LfjPoolTransformer,
    "PharClpoolTransformer": PharClpoolTransformer,
    "PharPairTransformer": PharPairTransformer,
}

@dataclass
class ContractTransformer:
    """Complete configuration for a contract transformer."""
    transformer_class: Type
    event_priorities: Dict[str, int] = field(default_factory=dict)  # event -> priority
    active: bool = True

class TransformerRegistry:
    def __init__(self):
        self._contracts: Dict[EvmAddress, ContractTransformer] = {}
        self._initialized = False
    
    def register_contract(self, contract_address: EvmAddress, transformer_class: Type, event_priorities: Dict[str, int] = None):
        self._contracts[contract_address.lower()] = ContractTransformer(
            transformer_class=transformer_class,
            event_priorities=event_priorities or {}
        )
    
    def get_transformer_class(self, contract_address: EvmAddress) -> Optional[Type]:
        config = self._contracts.get(contract_address.lower())
        return config.transformer_class if config and config.active else None
    
    def get_event_priority(self, contract_address: EvmAddress, event_name: str) -> int:
        config = self._contracts.get(contract_address.lower())
        if config and config.active:
            return config.event_priorities.get(event_name, 999)
        return 999  # Default to very low priority
    
    def get_logs_by_priority(self, decoded_logs: Dict[str, any]) -> List[Tuple[str, any]]:
        log_items = list(decoded_logs.items())
        return sorted(log_items, key=lambda x: self.get_event_priority(x[1].contract, x[1].name))
    
    def get_all_contracts(self) -> Dict[str, ContractTransformer]:
        return self._contracts.copy()
    
    def setup(self):
        if self._initialized:
            return
        
        for address, transformer_config in config.transformers.items():
            transformer_class = TRANSFORMER_CLASSES.get(transformer_config.name)
            if transformer_class:
                event_priorities = {}
                if hasattr(transformer_config, 'priorities') and transformer_config.priorities:
                    event_priorities = transformer_config.priorities
                
                self.register_contract(address, transformer_class, event_priorities)
            else:
                print(f"Warning: Unknown transformer '{transformer_config.name}' for contract {address}")
        
        self._initialized = True


registry = TransformerRegistry()