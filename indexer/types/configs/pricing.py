# indexer/types/configs/pricing.py

from typing import Dict, Optional, Any, Literal

from msgspec import Struct

from ..new import EvmAddress


class PricingConfig(Struct):
    model: str
    pool_address: EvmAddress
    pricing_method: Literal['direct_avax', 'direct_usd', 'global'] = 'global'
    price_feed: bool = False
    pricing_start: int 
    status: str = 'active' 
    pricing_end: Optional[int] = None

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'PricingConfig':
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        return {
            'model': self.model,
            'pool_address': self.pool_address.lower() if self.pool_address else None,
            'pricing_method': self.pricing_method,
            'price_feed': self.price_feed,
            'pricing_start': self.pricing_start,
            'pricing_end': self.pricing_end,
            'status': self.status,
        }