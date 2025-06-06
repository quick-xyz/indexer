# indexer/transform/registry.py

from typing import Dict, Optional, List
from dataclasses import field
from collections import defaultdict
from msgspec import Struct

from ..core.config import IndexerConfig
from ..core.mixins import LoggingMixin

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


class TransformerRegistry(LoggingMixin):    
    def __init__(self, config: IndexerConfig):
        self.config = config
        self._transformers: Dict[EvmAddress, ContractTransformer] = {}
        self._transformer_classes = self._load_transformer_classes()
        
        self.log_info("TransformerRegistry initializing", 
                     contract_count=len(config.contracts))
        
        self._setup_transformers()
        
        self.log_info("TransformerRegistry initialized", 
                     active_transformers=len(self._transformers))

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
            self.log_info("Transformer classes loaded successfully", 
                         class_count=len(transformer_classes),
                         classes=list(transformer_classes.keys()))
            
        except ImportError as e:
            self.log_error("Failed to import transformer classes", 
                          error=str(e),
                          exception_type=type(e).__name__)
            
        return transformer_classes

    def _setup_transformers(self):
        self.log_debug("Setting up transformers", contract_count=len(self.config.contracts))
        
        setup_stats = {'successful': 0, 'failed': 0, 'skipped': 0}
        
        for address, contract in self.config.contracts.items():
            contract_context = {'contract_address': address, 'contract_name': contract.name}
            
            self.log_debug("Processing contract", **contract_context)
            
            if not contract.transform:
                self.log_debug("No transform config - skipping", **contract_context)
                setup_stats['skipped'] += 1
                continue
                
            transformer_config = contract.transform
            transformer_context = {**contract_context, 'transformer_name': transformer_config.name}
            
            self.log_debug("Creating transformer", **transformer_context)
            
            transformer_class = self._transformer_classes.get(transformer_config.name)
            
            if not transformer_class:
                self.log_error("Transformer class not found", 
                              available_classes=list(self._transformer_classes.keys()),
                              **transformer_context)
                setup_stats['failed'] += 1
                continue
                
            try:
                instantiate_params = transformer_config.instantiate or {}
                self.log_debug("Instantiating transformer", 
                              params=instantiate_params,
                              **transformer_context)
                
                transformer_instance = transformer_class(**instantiate_params)

                # Verify required methods
                required_methods = ['process_transfers', 'process_logs']
                for method_name in required_methods:
                    if not hasattr(transformer_instance, method_name):
                        raise AttributeError(f"Missing required method: {method_name}")
                    
                self.log_debug("Transformer methods validated", **transformer_context)
                
                self.register_contract(
                    address,
                    transformer_instance,
                    transformer_config.transfers or {},
                    transformer_config.logs or {}
                )
                
                self.log_info("Transformer registered successfully", **transformer_context)
                setup_stats['successful'] += 1
                
            except (TypeError, AttributeError) as e:
                self.log_error("Transformer instantiation failed", 
                              error=str(e),
                              exception_type=type(e).__name__,
                              **transformer_context)
                setup_stats['failed'] += 1
                continue
            except Exception as e:
                self.log_error("Unexpected error creating transformer", 
                              error=str(e),
                              exception_type=type(e).__name__,
                              **transformer_context)
                setup_stats['failed'] += 1
                continue
                
        self.log_info("Transformer setup completed", **setup_stats)

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
        
        transformer = self._transformers.get(contract_address.lower())
        
        if transformer and transformer.active:
            if event_name in transformer.transfer_priorities:
                priority = transformer.transfer_priorities[event_name]
                self.log_debug("Transfer priority found", 
                              contract_address=contract_address,
                              event_name=event_name,
                              priority=priority)
                return priority
            else:
                self.log_debug("Event not in transfer priorities", 
                              contract_address=contract_address,
                              event_name=event_name,
                              available_events=list(transformer.transfer_priorities.keys()))
        else:
            self.log_debug("No active transformer found", 
                          contract_address=contract_address,
                          available_transformers=list(self._transformers.keys()))
        
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

        self.log_debug("Ordering transfers", input_log_count=len(decoded_logs))

        priority_stats = {'found': 0, 'not_found': 0}

        for log_key, log in decoded_logs.items():
            log_context = {
                'log_key': log_key, 
                'event_name': log.name, 
                'contract_address': log.contract
            }
            
            self.log_debug("Checking log for transfer priority", **log_context)
            
            priority = self.get_transfer_priority(log.contract, log.name)

            if priority is not None:
                self.log_debug("Adding to transfers by contract", 
                              priority=priority, **log_context)
                transfers_by_contract[log.contract][priority].append(log)
                priority_stats['found'] += 1
            else:
                self.log_debug("No transfer priority found", **log_context)
                priority_stats['not_found'] += 1
                
        result_summary = {
            contract: {priority: len(logs) for priority, logs in priorities.items()}
            for contract, priorities in transfers_by_contract.items()
        }
        
        self.log_info("Transfer ordering completed", 
                     contracts_with_transfers=len(transfers_by_contract),
                     priority_distribution=result_summary,
                     **priority_stats)
        
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