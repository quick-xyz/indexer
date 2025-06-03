# indexer/transform/registry.py

from typing import Dict, Optional, List
from dataclasses import dataclass, field
from collections import defaultdict
import importlib

from ..core.config import IndexerConfig
from ..types import (
    EvmAddress, 
    IndexerConfig,
    DecodedLog,
)

@dataclass
class ContractTransformer:
    instance: object
    transfer_priorities: Dict[str, int] = field(default_factory=dict)
    log_priorities: Dict[str, int] = field(default_factory=dict)
    active: bool = True


class TransformerRegistry:
    """Registry for transformer instances and their configurations"""
    
    def __init__(self, config: IndexerConfig):
        self.config = config
        self._transformers: Dict[EvmAddress, ContractTransformer] = {}
        self._transformer_classes = self._load_transformer_classes()
        self._setup_transformers()

    def _load_transformer_classes(self) -> Dict[str, type]:
        """Dynamically load transformer classes"""
        # Base transformer classes - could be made configurable
        transformer_classes = {}
        
        try:
            # Import base transformers
            from .transformers.base import TokenTransformer
            from .transformers.tokens.wavax import WavaxTransformer
            transformer_classes.update({
                "TokenTransformer": TokenTransformer,
                "WavaxTransformer": WavaxTransformer,
            })
        except ImportError:
            pass
            
        try:
            # Import pool transformers
            from .transformers.pools.lb_pair import LbPairTransformer
            from .transformers.pools.lfj_pool import LfjPoolTransformer
            from .transformers.pools.phar_clpool import PharClpoolTransformer
            from .transformers.pools.phar_pair import PharPairTransformer
            transformer_classes.update({
                "LBPairTransformer": LbPairTransformer,
                "LfjPoolTransformer": LfjPoolTransformer,
                "PharClpoolTransformer": PharClpoolTransformer,
                "PharPairTransformer": PharPairTransformer,
            })
        except ImportError:
            pass

        ''' 
        try:
            # Import router transformers
            from .transformers.routers.lfj_aggregator import LfjAggregatorTransformer
            from .transformers.routers.phar_cl_manager import PharClManagerTransformer
            transformer_classes.update({
                "LfjAggregatorTransformer": LfjAggregatorTransformer,
                "PharClManagerTransformer": PharClManagerTransformer,
            })
        except ImportError:
            pass
        '''     
        try:
            # Import wesmol transformers
            from .transformers.wesmol.auction import AuctionTransformer
            from .transformers.wesmol.farm import FarmTransformer
            from .transformers.wesmol.wrapper import WesmolWrapperTransformer
            transformer_classes.update({
                "AuctionTransformer": AuctionTransformer,
                "FarmTransformer": FarmTransformer,
                "WesmolWrapperTransformer": WesmolWrapperTransformer,
            })
        except ImportError:
            pass
            
        return transformer_classes

    def _setup_transformers(self):
        """Setup transformers from configuration"""
        for address, contract in self.config.contracts.items():
            if not contract.transform:
                continue
                
            transformer_config = contract.transform
            transformer_class = self._transformer_classes.get(transformer_config.name)
            
            if not transformer_class:
                continue
                
            try:
                instantiate_params = transformer_config.instantiate or {}
                transformer_instance = transformer_class(**instantiate_params)
                
                self.register_contract(
                    address,
                    transformer_instance,
                    transformer_config.transfers or {},
                    transformer_config.logs or {}
                )
                
            except Exception:
                # Skip failed transformer creation
                continue

    def register_contract(self, 
                          contract_address: EvmAddress, 
                          instance: object, 
                          transfer_priorities: Dict[str, int] = None,
                          log_priorities: Dict[str, int] = None):
        """Register a transformer for a contract"""
        self._transformers[contract_address.lower()] = ContractTransformer(
            instance=instance,
            transfer_priorities=transfer_priorities or {},
            log_priorities=log_priorities or {}
        )

    def get_transformer(self, contract_address: EvmAddress) -> Optional[object]:
        """Get transformer instance for a contract"""
        transformer = self._transformers.get(contract_address.lower())
        return transformer.instance if transformer and transformer.active else None

    def get_transfer_priority(self, contract_address: EvmAddress, event_name: str) -> Optional[int]:
        """Get transfer priority for a contract event"""
        transformer = self._transformers.get(contract_address.lower())
        if transformer and transformer.active and event_name in transformer.transfer_priorities:
            return transformer.transfer_priorities[event_name]
        return None

    def get_log_priority(self, contract_address: EvmAddress, event_name: str) -> Optional[int]:
        """Get log priority for a contract event"""
        transformer = self._transformers.get(contract_address.lower())
        if transformer and transformer.active and event_name in transformer.log_priorities:
            return transformer.log_priorities[event_name]
        return None

    def get_transfers_ordered(self, decoded_logs: Dict[str, DecodedLog]) -> Dict[EvmAddress, Dict[int, List[DecodedLog]]]:
        """Group transfer logs by contract and priority"""
        transfers_by_contract = defaultdict(lambda: defaultdict(list))

        for _, log in decoded_logs.items():
            priority = self.get_transfer_priority(log.contract, log.name)
            if priority is not None:
                transfers_by_contract[log.contract][priority].append(log)

        return transfers_by_contract

    def get_remaining_logs_ordered(self, decoded_logs: Dict[str, DecodedLog]) -> Dict[int, Dict[EvmAddress, List[DecodedLog]]]:
        """Group non-transfer logs by priority and contract"""
        logs_by_priority = defaultdict(lambda: defaultdict(list))

        for _, log in decoded_logs.items():
            priority = self.get_log_priority(log.contract, log.name)
            if priority is not None:
                logs_by_priority[priority][log.contract].append(log)

        return logs_by_priority

    def get_all_contracts(self) -> Dict[str, ContractTransformer]:
        """Get all registered contract transformers"""
        return self._transformers.copy()