import json
import os
from typing import Dict, Any, Optional
from pathlib import Path
import importlib


class ConfigLoader:
    """Loads and manages transformer configuration from JSON file."""
    
    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            config_path = Path(__file__).parent / "transformer_config.json"
        
        self.config_path = Path(config_path)
        self._config: Optional[Dict[str, Any]] = None
        self._transformer_classes: Dict[str, type] = {}
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        with open(self.config_path, 'r') as f:
            self._config = json.load(f)
        return self._config
    
    def get_config(self) -> Dict[str, Any]:
        """Get configuration, loading if not already loaded."""
        if self._config is None:
            self.load_config()
        return self._config
       
    def get_contract_types(self) -> Dict[str, Dict[str, Any]]:
        """Get contract type definitions."""
        return self.get_config().get("contract_types", {})
    
    def get_contracts(self) -> Dict[str, Dict[str, Any]]:
        """Get contract instance configurations."""
        return self.get_config().get("contracts", {})
    
    def get_transformation_rules(self) -> list:
        """Get transformation rule definitions."""
        return self.get_config().get("transformation_rules", [])
    
    def get_active_contracts(self) -> Dict[str, Dict[str, Any]]:
        contracts = self.get_contracts()
        return {
            address: config for address, config in contracts.items()
            if config.get("active", True)
        }
    
    def get_active_rules(self) -> list:
        rules = self.get_transformation_rules()
        return [rule for rule in rules if rule.get("active", True)]
    
    def get_transformer_class(self, contract_type: str) -> Optional[type]:
        if contract_type in self._transformer_classes:
            return self._transformer_classes[contract_type]
        
        contract_types = self.get_contract_types()
        if contract_type not in contract_types:
            return None
        
        type_config = contract_types[contract_type]
        module_path = type_config.get("module")
        class_name = type_config.get("transformer_class")
        
        if not module_path or not class_name:
            return None
        
        try:
            module = importlib.import_module(module_path)
            transformer_class = getattr(module, class_name)
            self._transformer_classes[contract_type] = transformer_class
            return transformer_class
        except (ImportError, AttributeError) as e:
            raise ImportError(f"Could not import {class_name} from {module_path}: {e}")
    
    def get_contract_config(self, contract_address: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific contract."""
        contracts = self.get_contracts()
        return contracts.get(contract_address.lower())


# Global config loader instance
config_loader = ConfigLoader()