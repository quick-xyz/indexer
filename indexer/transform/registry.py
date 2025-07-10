# indexer/transform/registry.py

from typing import Dict, Optional, Any
from msgspec import Struct
import json

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
        if not config:
            raise ValueError("IndexerConfig cannot be None")
        
        self.config = config
        self._transformers: Dict[EvmAddress, ContractTransformer] = {}
        self._patterns: Dict[str, TransferPattern] = {}
        
        self.log_info("TransformRegistry initializing", 
                     contract_count=len(config.contracts))
        
        try:
            self._transformer_classes = self._load_transformer_classes()
            self._pattern_classes = self._load_pattern_classes()
            
            self._setup_transformers()
            self._setup_patterns()
            
            self.log_info("TransformRegistry initialized successfully", 
                         active_transformers=len(self._transformers),
                         available_patterns=len(self._patterns))
            
        except Exception as e:
            self.log_error("Failed to initialize TransformRegistry",
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def _load_transformer_classes(self) -> Dict[str, type]:
        """Load all available transformer classes"""
        transformer_classes = {}
        
        try:
            # Define all transformer classes
            class_mappings = {
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
            }
            
            # Validate each class
            missing_classes = []
            for name, cls in class_mappings.items():
                if cls is None:
                    missing_classes.append(name)
                    self.log_error("Transformer class not available", class_name=name)
                else:
                    transformer_classes[name] = cls
            
            if missing_classes:
                self.log_warning("Some transformer classes are missing",
                                missing_classes=missing_classes,
                                available_classes=list(transformer_classes.keys()))
            
            self.log_info("Transformer classes loaded successfully", 
                         class_count=len(transformer_classes),
                         classes=list(transformer_classes.keys()))
            
            return transformer_classes
            
        except ImportError as e:
            self.log_error("Failed to import transformer classes", 
                          error=str(e),
                          exception_type=type(e).__name__)
            raise
        except Exception as e:
            self.log_error("Unexpected error loading transformer classes",
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def _load_pattern_classes(self) -> Dict[str, type]:
        """Load all available pattern classes"""
        try:
            pattern_classes = {
                "Mint_A": Mint_A,
                "Burn_A": Burn_A,
                "Swap_A": Swap_A,
            }
            
            # Validate pattern classes
            missing_patterns = []
            for name, cls in pattern_classes.items():
                if cls is None:
                    missing_patterns.append(name)
                    self.log_error("Pattern class not available", pattern_name=name)
            
            if missing_patterns:
                self.log_warning("Some pattern classes are missing",
                                missing_patterns=missing_patterns)
            
            self.log_info("Pattern classes loaded successfully",
                         pattern_count=len(pattern_classes),
                         patterns=list(pattern_classes.keys()))
            
            return pattern_classes
            
        except Exception as e:
            self.log_error("Failed to load pattern classes",
                          error=str(e),
                          exception_type=type(e).__name__)
            raise
    
    def _setup_transformers(self):
        """Setup transformers from configuration"""
        self.log_info("Setting up transformers from configuration")
        
        for contract_address, contract in self.config.contracts.items():
            try:
                # Get transformer config from ContractConfig object
                transform = getattr(contract, 'transform', None)
                if not transform:
                    self.log_debug("No transformer configured for contract",
                                 contract_address=contract_address,
                                 contract_name=contract.name)
                    continue
                
                transformer_name = getattr(transform, 'name', None)
                instantiate_config = getattr(transform, 'instantiate', {})
                
                if not transformer_name:
                    self.log_warning("Transformer name not specified",
                                   contract_address=contract_address,
                                   contract_name=contract.name)
                    continue
                
                transformer_class = self._transformer_classes.get(transformer_name)
                if not transformer_class:
                    self.log_warning("Transformer class not found",
                                   transformer_name=transformer_name,
                                   contract_address=contract_address,
                                   available_transformers=list(self._transformer_classes.keys()))
                    continue
                
                # Validate transformer class
                if not hasattr(transformer_class, 'process_logs'):
                    self.log_error("Transformer missing process_logs method",
                                 transformer_name=transformer_name,
                                 contract_address=contract_address)
                    continue
                
                # Create transformer instance
                self.log_debug("Creating transformer instance",
                             transformer_name=transformer_name,
                             contract_address=contract_address,
                             instantiate_config=instantiate_config)
                
                instance = transformer_class(**instantiate_config)
                
                self._transformers[contract_address.lower()] = ContractTransformer(instance=instance)
                
                self.log_debug("Transformer registered",
                             contract_address=contract_address,
                             transformer_name=transformer_name)
                
            except Exception as e:
                self.log_error("Failed to setup transformer for contract",
                             contract_address=contract_address,
                             contract_name=getattr(contract, 'name', 'unknown'),
                             error=str(e),
                             exception_type=type(e).__name__)
        
        self.log_info("Transformer setup complete",
                     total_contracts=len(self.config.contracts),
                     transformers_registered=len(self._transformers))

    def _setup_patterns(self):
        """Setup pattern processors"""
        if not self._pattern_classes:
            self.log_error("No pattern classes available for setup")
            return
        
        self.log_debug("Setting up patterns", pattern_count=len(self._pattern_classes))
        
        setup_stats = {'successful': 0, 'failed': 0}
        
        for name, pattern_class in self._pattern_classes.items():
            try:
                self.log_debug("Creating pattern instance", pattern_name=name)
                
                pattern_instance = pattern_class()
                
                # Validate pattern instance
                if not hasattr(pattern_instance, 'produce_events'):
                    self.log_error("Pattern missing required method 'produce_events'",
                                  pattern_name=name)
                    setup_stats['failed'] += 1
                    continue
                
                self._patterns[name] = pattern_instance
                
                self.log_debug("Pattern registered successfully", 
                              pattern_name=name,
                              pattern_class=pattern_class.__name__)
                setup_stats['successful'] += 1
                
            except Exception as e:
                self.log_error("Failed to create pattern", 
                              pattern_name=name, 
                              error=str(e),
                              exception_type=type(e).__name__)
                setup_stats['failed'] += 1
        
        if setup_stats['failed'] > 0:
            self.log_error("Pattern setup completed with failures", **setup_stats)
        else:
            self.log_info("Pattern setup completed successfully", **setup_stats)

    def register_contract(self, contract_address: EvmAddress, instance: BaseTransformer):
        """Register a transformer instance for a contract"""
        if not contract_address:
            raise ValueError("Contract address cannot be empty")
        if not instance:
            raise ValueError("Transformer instance cannot be None")
        if not isinstance(instance, BaseTransformer):
            raise TypeError("Instance must be a BaseTransformer")
        
        try:
            self._transformers[contract_address.lower()] = ContractTransformer(instance=instance)
            
            self.log_debug("Contract transformer registered",
                          contract_address=contract_address,
                          transformer_name=type(instance).__name__)
            
        except Exception as e:
            self.log_error("Failed to register contract transformer",
                          contract_address=contract_address,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def get_transformer(self, contract_address: EvmAddress) -> Optional[BaseTransformer]:
        """Get transformer instance for a contract"""
        if not contract_address:
            self.log_warning("Empty contract address provided")
            return None
        
        try:
            transformer = self._transformers.get(contract_address.lower())
            
            if not transformer:
                self.log_debug("No transformer found for contract",
                              contract_address=contract_address)
                return None
            
            if not transformer.active:
                self.log_debug("Transformer exists but is inactive",
                              contract_address=contract_address)
                return None
            
            return transformer.instance
            
        except Exception as e:
            self.log_error("Error retrieving transformer",
                          contract_address=contract_address,
                          error=str(e),
                          exception_type=type(e).__name__)
            return None

    def get_pattern(self, pattern_name: str) -> Optional[TransferPattern]:
        """Get pattern processor by name"""
        if not pattern_name:
            self.log_warning("Empty pattern name provided")
            return None
        
        try:
            pattern = self._patterns.get(pattern_name)
            
            if not pattern:
                self.log_warning("Pattern not found",
                                pattern_name=pattern_name,
                                available_patterns=list(self._patterns.keys()))
                return None
            
            return pattern
            
        except Exception as e:
            self.log_error("Error retrieving pattern",
                          pattern_name=pattern_name,
                          error=str(e),
                          exception_type=type(e).__name__)
            return None

    def get_all_contracts(self) -> Dict[EvmAddress, ContractTransformer]:
        """Get all registered contract transformers"""
        try:
            return self._transformers.copy()
        except Exception as e:
            self.log_error("Error getting all contracts",
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}

    def get_contracts_with_transformers(self) -> Dict[EvmAddress, BaseTransformer]:
        """Get all active contract transformers"""
        try:
            result = {
                address: transformer.instance 
                for address, transformer in self._transformers.items() 
                if transformer.active
            }
            
            self.log_debug("Retrieved active transformers",
                          active_count=len(result),
                          total_count=len(self._transformers))
            
            return result
            
        except Exception as e:
            self.log_error("Error getting contracts with transformers",
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}

    def get_setup_summary(self) -> Dict[str, Any]:
        """Get summary of transformer setup for debugging"""
        try:
            summary = {
                "total_contracts": len(self._transformers),
                "active_transformers": sum(1 for t in self._transformers.values() if t.active),
                "inactive_transformers": sum(1 for t in self._transformers.values() if not t.active),
                "transformer_types": {},
                "contracts_by_transformer": {},
                "available_patterns": list(self._patterns.keys()),
                "pattern_count": len(self._patterns)
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
            
            self.log_debug("Generated setup summary", **summary)
            
            return summary
            
        except Exception as e:
            self.log_error("Error generating setup summary",
                          error=str(e),
                          exception_type=type(e).__name__)
            return {
                "error": f"Failed to generate summary: {str(e)}",
                "total_contracts": len(self._transformers) if hasattr(self, '_transformers') else 0,
                "pattern_count": len(self._patterns) if hasattr(self, '_patterns') else 0
            }

    def deactivate_transformer(self, contract_address: EvmAddress) -> bool:
        """Deactivate a transformer without removing it"""
        if not contract_address:
            return False
        
        try:
            transformer = self._transformers.get(contract_address.lower())
            if transformer:
                transformer.active = False
                self.log_info("Transformer deactivated",
                             contract_address=contract_address)
                return True
            else:
                self.log_warning("Cannot deactivate - transformer not found",
                               contract_address=contract_address)
                return False
        except Exception as e:
            self.log_error("Error deactivating transformer",
                          contract_address=contract_address,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False

    def reactivate_transformer(self, contract_address: EvmAddress) -> bool:
        """Reactivate a deactivated transformer"""
        if not contract_address:
            return False
        
        try:
            transformer = self._transformers.get(contract_address.lower())
            if transformer:
                transformer.active = True
                self.log_info("Transformer reactivated",
                             contract_address=contract_address)
                return True
            else:
                self.log_warning("Cannot reactivate - transformer not found",
                               contract_address=contract_address)
                return False
        except Exception as e:
            self.log_error("Error reactivating transformer",
                          contract_address=contract_address,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False