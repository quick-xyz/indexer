# indexer/transform/registry.py

from typing import Dict, Optional, List
from dataclasses import field
from collections import defaultdict
from msgspec import Struct

from ..core.config import IndexerConfig

from .transformers import *
from ..types import (
    EvmAddress, 
    DecodedLog,
)


class ContractTransformer(Struct):
    instance: BaseTransformer
    transfer_priorities: Dict[str, int] = field(default_factory=dict)
    log_priorities: Dict[str, int] = field(default_factory=dict)
    active: bool = True


class TransformerRegistry:    
    def __init__(self, config: IndexerConfig):
        self.config = config
        self._transformers: Dict[EvmAddress, ContractTransformer] = {}
        self._transformer_classes = self._load_transformer_classes()
        self._setup_transformers()

    def _load_transformer_classes(self) -> Dict[str, type]:
        transformer_classes = {}
        
        try:
            transformer_classes.update({
                "TokenTransformer": TokenTransformer,
                "WavaxTransformer": WavaxTransformer,
                "LfjPoolTransformer": LfjPoolTransformer,
                "LbPairTransformer": LbPairTransformer,
                "PharPairTransformer": PharPairTransformer,
                "PharClPoolTransformer": PharClPoolTransformer,
            })
            print(f"🔍 Loaded transformer classes: {list(transformer_classes.keys())}")
        except ImportError as e:
            print(f"❌ Failed to import transformer classes: {e}")
            
        return transformer_classes

    def _setup_transformers(self):
        print(f"🔍 Setting up transformers for {len(self.config.contracts)} contracts")
        for address, contract in self.config.contracts.items():
            print(f"   Processing contract {address}: {contract.name}")
            if not contract.transform:
                print(f"     ❌ No transform config")
                continue
                
            transformer_config = contract.transform
            print(f"     Transform name: {transformer_config.name}")
            transformer_class = self._transformer_classes.get(transformer_config.name)
            print(f"     Transformer class found: {transformer_class}")
            
            if not transformer_class:
                print(f"     ❌ Transformer class '{transformer_config.name}' not found")
                print(f"     Available classes: {list(self._transformer_classes.keys())}")
                continue
                
            try:
                instantiate_params = transformer_config.instantiate or {}
                print(f"     Instantiate params: {instantiate_params}")
                transformer_instance = transformer_class(**instantiate_params)
                print(f"     ✅ Transformer created: {transformer_instance}")

                # Verify it has required methods
                if not hasattr(transformer_instance, 'process_transfers'):
                    raise AttributeError("Missing process_transfers method")
                if not hasattr(transformer_instance, 'process_logs'):
                    raise AttributeError("Missing process_logs method")
                    
                print(f"     ✅ Transformer methods validated")
                self.register_contract(
                    address,
                    transformer_instance,
                    transformer_config.transfers or {},
                    transformer_config.logs or {}
                )
                print(f"     ✅ Transformer registered for {address}")
                
            except TypeError as e:
                print(f"     ❌ TypeError in transformer instantiation: {e}")
                print(f"     Check constructor signature for {transformer_config.name}")
                continue
            except AttributeError as e:
                print(f"     ❌ AttributeError: {e}")
                print(f"     Transformer {transformer_config.name} missing required methods")
                continue
            except Exception as e:
                print(f"     ❌ Unexpected error creating transformer: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
                continue
        print(f"🔍 Final registered transformers: {list(self._transformers.keys())}")

    def register_contract(self, 
                          contract_address: EvmAddress, 
                          instance: object, 
                          transfer_priorities: Dict[str, int] = None,
                          log_priorities: Dict[str, int] = None):
        """Register a transformer for a contract"""
        self._transformers[contract_address] = ContractTransformer(
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
        print(f"     🔍 get_transfer_priority({contract_address}, {event_name})")
        transformer = self._transformers.get(contract_address.lower())
        print(f"       Transformer found: {transformer}")

        if transformer and transformer.active:
            print(f"       Transformer active: {transformer.active}")
            print(f"       Transfer priorities: {transformer.transfer_priorities}")
            
            if event_name in transformer.transfer_priorities:
                priority = transformer.transfer_priorities[event_name]
                print(f"       ✅ Found priority {priority} for {event_name}")
                return priority
            else:
                print(f"       ❌ Event '{event_name}' not in transfer_priorities: {list(transformer.transfer_priorities.keys())}")
        else:
            print(f"       ❌ No active transformer found")
            print(f"       Available transformers: {list(self._transformers.keys())}")
        
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

        print(f"🔍 get_transfers_ordered debug:")
        print(f"   Input decoded_logs: {len(decoded_logs)}")

        for log_key, log in decoded_logs.items():
            print(f"   Checking log {log_key}: {log.name} from {log.contract}")
            priority = self.get_transfer_priority(log.contract, log.name)
            print(f"     Transfer priority: {priority}")

            if priority is not None:
                print(f"     ✅ Adding to transfers_by_contract[{log.contract}][{priority}]")
                transfers_by_contract[log.contract][priority].append(log)
            else:
                print(f"     ❌ No transfer priority found")
        print(f"   Final transfers_by_contract: {dict(transfers_by_contract)}")        
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
        return self._transformers.copy()