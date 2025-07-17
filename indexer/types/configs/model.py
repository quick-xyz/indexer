# indexer/types/configs/model.py

from typing import Dict, Optional, Any

from msgspec import Struct

from ..new import EvmAddress


class ModelConfig(Struct):
    id: str
    name: str
    version: str = 'v1'
    network: str = 'avalanche'
    shared_db: str
    indexer_db: str
    status: str = 'active'
    description: Optional[str] = None
    model_token: Optional[EvmAddress] = None
    
    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'ModelConfig':
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for database Model creation"""
        return {
            'name': self.name,
            'version': self.version,
            'network': self.network,
            'shared_db': self.shared_db,
            'indexer_db': self.indexer_db,
            'description': self.description,
            'model_token': self.model_token.lower() if self.model_token else None,
            'status': self.status,
        }