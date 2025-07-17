# indexer/types/configs/pool.py

from typing import Dict, Optional, Any, Literal

from msgspec import Struct

from ..new import EvmAddress


class PoolConfig(Struct):
    address: EvmAddress
    base_token: EvmAddress
    status: str = 'active' 
    pricing_default: Literal['direct_avax', 'direct_usd', 'global'] = 'global'
    quote_token: Optional[EvmAddress] = None

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'PoolConfig':
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        return {
            'base_token': self.base_token.lower() if self.base_token else None,
            'quote_token': self.quote_token.lower() if self.quote_token else None,
            'pricing_default': self.pricing_default,
            'status': self.status,
        }