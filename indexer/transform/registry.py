# indexer/transform/registry.py

from typing import Dict, Optional, Any
from msgspec import Struct

from ..core.config import IndexerConfig
from ..core.mixins import LoggingMixin

from .transformers import *
from ..types import EvmAddress

from .patterns import *

class ContractTransformer(Struct):
    instance: BaseTransformer
    active: bool = True


class TransformRegistry(LoggingMixin):    
    def __init__(self, config: IndexerConfig):
        self.config = config
        self._transformers: Dict[EvmAddress, ContractTransformer] = {}
        self._transformer_classes = self._load_transformer_classes()
        self._patterns: Dict[str, TransferPattern] = {}
        self._pattern_classes = self._load_pattern_classes()

        self.log_info("TransformRegistry initializing", 
                     contract_count=len(config.contracts))
        
        self._setup_transformers()
        self._setup_patterns()
        
        self.log_info("TransformRegistry initialized", 
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
                "PharNfpTransformer": PharNfpTransformer,
                "KyberAggregatorTransformer": KyberAggregatorTransformer,
                "OdosAggregatorTransformer": OdosAggregatorTransformer,
                "LfjAggregatorTransformer": LfjAggregatorTransformer,
            })

            self.log_info("Transformer classes loaded successfully", 
                         class_count=len(transformer_classes),
                         classes=list(transformer_classes.keys()))
            
        except ImportError as e:
            self.log_error("Failed to import transformer classes", 
                          error=str(e),
                          exception_type=type(e).__name__)
            
        return transformer_classes

    def _load_pattern_classes(self) -> Dict[str, type]:
        return {
            "Mint_A": Mint_A,
            "Burn_A": Burn_A,
            "Swap_A": Swap_A,
        }
    
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

                # Validate required method exists
                if not hasattr(transformer_instance, 'process_logs'):
                    raise AttributeError(f"Missing required method: process_logs")
                    
                self.log_debug("Transformer validated", **transformer_context)
                
                self.register_contract(address, transformer_instance)
                
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

    def _setup_patterns(self):
        for name, pattern_class in self._pattern_classes.items():
            try:
                pattern_instance = pattern_class()
                self._patterns[name] = pattern_instance
                self.log_debug("Pattern registered", pattern_name=name)
            except Exception as e:
                self.log_error("Failed to create pattern", pattern_name=name, error=str(e))

    def register_contract(self, contract_address: EvmAddress, instance: object):
        self._transformers[contract_address] = ContractTransformer(instance=instance)

    def get_transformer(self, contract_address: EvmAddress) -> Optional[BaseTransformer]:
        transformer = self._transformers.get(contract_address.lower())
        return transformer.instance if transformer and transformer.active else None

    def get_pattern(self, pattern_name: str) -> Optional[TransferPattern]:
        return self._patterns.get(pattern_name)

    def get_all_contracts(self) -> Dict[str, ContractTransformer]:
        return self._transformers.copy()

    def get_contracts_with_transformers(self) -> Dict[EvmAddress, object]:
        return {
            address: transformer.instance 
            for address, transformer in self._transformers.items() 
            if transformer.active
        }
def get_setup_summary(self) -> Dict[str, Any]:
    """Get summary of transformer setup for debugging"""
    summary = {
        "total_contracts": len(self._transformers),
        "active_transformers": sum(1 for t in self._transformers.values() if t.active),
        "transformer_types": {},
        "contracts_by_transformer": {},
        "setup_errors": []
    }
    
    for address, transformer_info in self._transformers.items():
        if transformer_info.active:
            transformer_name = type(transformer_info.instance).__name__
            
            # Count transformer types
            summary["transformer_types"][transformer_name] = summary["transformer_types"].get(transformer_name, 0) + 1
            
            # Group contracts by transformer type
            if transformer_name not in summary["contracts_by_transformer"]:
                summary["contracts_by_transformer"][transformer_name] = []
            summary["contracts_by_transformer"][transformer_name].append(address)
    
    return summary