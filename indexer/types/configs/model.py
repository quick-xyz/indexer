# indexer/types/configs/model.py

from typing import Dict, Optional, List, Any
from pathlib import Path

from msgspec import Struct

from ..new import EvmAddress


class ModelConfig(Struct):
    name: str
    version: str = 'v1'
    database_name: str
    status: str = 'active'
    description: Optional[str] = None
    target_asset: Optional[str] = None
    
    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'ModelConfig':
        """Create ModelConfig from YAML dictionary with validation"""
        # Handle legacy field names or nested structures
        if 'model' in data:
            # Handle nested structure: {'model': {...}}
            model_data = data['model']
        else:
            # Handle flat structure
            model_data = data
        
        return cls(**model_data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for database Model creation"""
        return {
            'name': self.name,
            'version': self.version,
            'description': self.description,
            'database_name': self.database_name,
            'target_asset': self.target_asset.lower() if self.target_asset else None,
            'status': self.status,
        }