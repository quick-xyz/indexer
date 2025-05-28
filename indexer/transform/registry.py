from typing import Dict, Type, Optional, List, Tuple
from dataclasses import dataclass, field

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
    instance: object
    transfer_priorities: Dict[str, int] = field(default_factory=dict)  # event -> priority
    log_priorities: Dict[str, int] = field(default_factory=dict)  # event -> priority
    active: bool = True


class TransformerRegistry:
    def __init__(self):
        self._contracts: Dict[EvmAddress, ContractTransformer] = {}
        self._initialized = False
    
    def register_contract(self, 
                          contract_address: EvmAddress, 
                          instance: object, 
                          transfer_priorities: Dict[str, int] = None,
                          log_priorities: Dict[str, int] = None):
        self._contracts[contract_address.lower()] = ContractTransformer(
            instance=instance,
            transfer_priorities=transfer_priorities or {},
            log_priorities=log_priorities or {}
        )
    
    def get_transformer(self, contract_address: EvmAddress) -> Optional[object]:
        config = self._contracts.get(contract_address.lower())
        return config.instance if config and config.active else None
    
    def get_transfer_priority(self, contract_address: EvmAddress, event_name: str) -> int:
        config = self._contracts.get(contract_address.lower())
        if config and config.active:
            return config.transfer_priorities.get(event_name, 999)
        return 999

    def get_log_priority(self, contract_address: EvmAddress, event_name: str) -> int:
        config = self._contracts.get(contract_address.lower())
        if config and config.active:
            return config.event_priorities.get(event_name, 999)
        return 999
    
    def is_transfer_event(self, contract_address: EvmAddress, event_name: str) -> bool:
        """Check if the event is a transfer event for the given contract."""
        config = self._contracts.get(contract_address.lower())
        if config and config.active:
            return event_name in config.transfer_priorities
        return False
    
    def get_transfers_by_contract(self, decoded_logs: Dict[str, any]) -> List[Tuple[str, any]]:
        transfer_logs = [
            (key, log) for key, log in decoded_logs.items()
            if self.is_transfer_event(log.contract, log.name)
        ]
        return sorted(transfer_logs, key=lambda x: self.get_transfer_priority(x[1].contract, x[1].name))

    def get_remaining_logs_by_priority(self, decoded_logs: Dict[str, any]) -> List[Tuple[str, any]]:
        remaining_logs = [
            (key, log) for key, log in decoded_logs.items()
            if not self.is_transfer_event(log.contract, log.name)
        ]
        return sorted(remaining_logs, key=lambda x: self.get_log_priority(x[1].contract, x[1].name))

    def get_all_contracts(self) -> Dict[str, ContractTransformer]:
        return self._contracts.copy()
    
    def setup(self):
        if self._initialized:
            return
        
        from ..config.config_manager import config

        for address, transformer_config in config.transformers.items():
            transformer_class = TRANSFORMER_CLASSES.get(transformer_config.name)
            if not transformer_class:
                print(f"Warning: Unknown transformer {transformer_config.name} for contract {address}")
                continue
            
            instantiate_params = getattr(transformer_config, 'instantiate', {})
            try:
                transformer_instance = transformer_class(**instantiate_params)
            except Exception as e:
                print(f"Error creating transformer instance for {address}: {e}")
                continue

            transfer_priorities = getattr(transformer_config, 'transfers', {})
            log_priorities = getattr(transformer_config, 'logs', {})

            self.register_contract(address, transformer_instance, transfer_priorities, log_priorities)
        
        self._initialized = True


registry = TransformerRegistry()