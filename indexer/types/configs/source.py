# indexer/types/configs/source.py

from typing import Dict, Optional, List, Any
from pathlib import Path

from msgspec import Struct

from ..new import EvmAddress


class SourceConfig(Struct):
    name: str           
    path: str                  
    source_type: str = 'quicknode_stream'    
    status: str = 'active'    
    format: Optional[str] = None        
    description: Optional[str] = None 
    configuration: Optional[Dict[str, Any]] = None                   
    
    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'SourceConfig':
        """Create SourceConfig from YAML dictionary with validation"""
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for database Source creation"""
        return {
            'name': self.name,
            'path': self.path,
            'source_type': self.source_type,
            'format': self.format,
            'description': self.description,
            'configuration': self.configuration,
            'status': self.status,
        }