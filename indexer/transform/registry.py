# indexer/transform/registry.py

from typing import Dict, Optional
from msgspec import Struct

from ..core.config import IndexerConfig
from ..core.mixins import LoggingMixin

from .transformers import *
from ..types import EvmAddress


class ContractTransformer(Struct):
    instance: BaseTransformer
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

                # Validate required method exists
                if not hasattr(transformer_instance, 'process_signals'):
                    raise AttributeError(f"Missing required method: process_signals")
                    
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

    def register_contract(self, contract_address: EvmAddress, instance: object):
        self._transformers[contract_address] = ContractTransformer(instance=instance)

    def get_transformer(self, contract_address: EvmAddress) -> Optional[BaseTransformer]:
        transformer = self._transformers.get(contract_address.lower())
        return transformer.instance if transformer and transformer.active else None

    def get_all_contracts(self) -> Dict[str, ContractTransformer]:
        return self._transformers.copy()

    def get_contracts_with_transformers(self) -> Dict[EvmAddress, object]:
        return {
            address: transformer.instance 
            for address, transformer in self._transformers.items() 
            if transformer.active
        }