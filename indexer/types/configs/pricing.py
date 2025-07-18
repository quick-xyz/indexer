# indexer/types/configs/pricing.py

from typing import Optional, Literal
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