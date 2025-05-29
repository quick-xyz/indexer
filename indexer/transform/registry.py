from typing import Dict, Type, Optional, List, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from ..config.types import TransformerConfig
from ..decode.model.types import EvmAddress
from ..decode.model.block import DecodedLog

# TRANSFORMER MODULES

from .transformers.base import TokenTransformer
from .transformers.tokens.wavax import WavaxTransformer

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
    "TokenTransformer": TokenTransformer,
    "WavaxTransformer": WavaxTransformer,
    
    "LBPairTransformer": LbPairTransformer,
    "LfjPoolTransformer": LfjPoolTransformer,
    "PharClpoolTransformer": PharClpoolTransformer,
    "PharPairTransformer": PharPairTransformer,
}

@dataclass
class ContractTransformer:
    instance: object
    transfer_priorities: Dict[str, int] = field(default_factory=dict)  # {log_name: priority}
    log_priorities: Dict[str, int] = field(default_factory=dict)  # {log_name: priority}
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


    def get_transfer_priority(self, contract_address: EvmAddress, event_name: str) -> int | None:
        """Get transfer priority if this is a transfer event, None otherwise."""
        config = self._contracts.get(contract_address.lower())
        if config and config.active and event_name in config.transfer_priorities:
            return config.transfer_priorities[event_name]
        return None


    def get_log_priority(self, contract_address: EvmAddress, event_name: str) -> int | None:
        """Get log priority if this is a monitored event, None otherwise."""
        config = self._contracts.get(contract_address.lower())
        if config and config.active and event_name in config.log_priorities:
            return config.log_priorities[event_name]
        return None


    def get_transfers_ordered(self, decoded_logs: Dict[str, DecodedLog]) -> Dict[EvmAddress, Dict[int, List[DecodedLog]]]:
        transfers_by_contract = defaultdict(lambda: defaultdict(list))

        for _, log in decoded_logs.items():
            priority = self.get_transfer_priority(log.contract, log.name)
            if priority is not None:
                transfers_by_contract[log.contract][priority].append(log)

        return transfers_by_contract


    def get_remaining_logs_ordered(self, decoded_logs: Dict[str, DecodedLog]) -> Dict[EvmAddress, Dict[int, List[DecodedLog]]]:
        logs_by_priority = defaultdict(lambda: defaultdict(list))

        for _, log in decoded_logs.items():
            priority = self.get_log_priority(log.contract, log.name)
            if priority is not None:
                logs_by_priority[priority][log.contract].append(log)

        return logs_by_priority


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