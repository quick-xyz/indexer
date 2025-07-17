# indexer/types/configs/token.py

from typing import Dict, Optional, List, Any
from pathlib import Path

from msgspec import Struct

from ..new import EvmAddress


class TokenConfig(Struct):
    address: EvmAddress
    symbol: str
    decimals: int = 18
    status: str = 'active'

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'TokenConfig':
        """Create TokenConfig from YAML dictionary with validation"""
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for database Token creation"""
        return {
            'symbol': self.symbol,
            'decimals': self.decimals,
            'status': self.status,
        }