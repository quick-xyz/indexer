from datetime import datetime
from typing import Literal, Optional
from msgspec import Struct

from ...decode.model.evm import EvmAddress,EvmHash

class LiquidityPosition(Struct, tag=True):
    position_id: int
    pool: EvmAddress
    provider: EvmAddress
    receipt_token: EvmAddress
    receipt_id: int
    amount_receipt: str
    created_at: EvmHash
    created_on: datetime
    closed_at: Optional[EvmHash] = None
    closed_on: Optional[datetime] = None
    custodian: Optional[EvmAddress] = None

class PositionBalance(Struct, tag=True):
    timestamp: datetime
    pool: EvmAddress
    provider: EvmAddress
    position_id: int
    base_token: EvmAddress
    amount_base: int
    quote_token: EvmAddress
    amount_quote: int
    value_avax: Optional[int] = None
    value_usd: Optional[int] = None
    custodian: Optional[EvmAddress] = None

class LiquidityBalance(Struct, tag=True):
    timestamp: datetime
    pool: EvmAddress
    provider: EvmAddress
    base_token: EvmAddress
    amount_base: int
    quote_token: EvmAddress
    amount_quote: int
    value_avax: Optional[int] = None
    value_usd: Optional[int] = None
    custodian: Optional[EvmAddress] = None