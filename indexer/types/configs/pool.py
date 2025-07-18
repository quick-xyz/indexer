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