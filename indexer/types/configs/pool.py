# indexer/types/configs/pool.py

from typing import Dict, Optional, List, Any
from pathlib import Path

from msgspec import Struct

from ..new import EvmAddress


class PoolConfig(Struct):
    address: EvmAddress
    base_token: EvmAddress
    status: str = 'active' 
    quote_token: Optional[EvmAddress] = None
    pricing_default: Optional[str] = None
    pricing_start: Optional[int] = None
    pricing_end: Optional[int] = None

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'PoolConfig':
        """Create PoolConfig from YAML dictionary with validation"""
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for database Pool creation"""
        return {
            'base_token': self.base_token.lower() if self.base_token else None,
            'quote_token': self.quote_token.lower() if self.quote_token else None,
            'pricing_default': self.pricing_default,
            'pricing_start': self.pricing_start,
            'pricing_end': self.pricing_end,
            'status': self.status,
        }