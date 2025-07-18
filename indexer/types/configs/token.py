# indexer/types/configs/token.py

from typing import Dict, Any
from msgspec import Struct
from ..new import EvmAddress


class TokenConfig(Struct):
    address: EvmAddress
    symbol: str
    decimals: int = 18
    status: str = 'active'