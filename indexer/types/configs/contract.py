# indexer/types/config/contract.py

from typing import Dict, Optional, List, Any
from pathlib import Path

from msgspec import Struct

from ..new import EvmAddress
from .token import TokenConfig


class ContractConfig(Struct):
    address: EvmAddress
    status: str = 'active'
    block_created: Optional[int] = None 
    abi_dir: Optional[str] = None 
    abi_file: Optional[str] = None
    abi: Optional[List[Dict[str, Any]]] = None
    transformer: Optional[str] = None 
    transform_init: Optional[Dict[str, Any]] = None

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'ContractConfig':
        """Create ContractConfig from YAML dictionary with validation"""
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        """Convert to dictionary suitable for database Contract creation"""
        return {
            'block_created': self.block_created,
            'abi_dir': self.abi_dir,
            'abi_file': self.abi_file,
            'transformer': self.transformer,
            'transform_init': self.transform_init,
            'status': self.status,
        }