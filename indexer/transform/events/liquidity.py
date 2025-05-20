from datetime import datetime
from typing import Literal

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent

class Liquidity(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: str
    pool: str
    custodian: EvmAddress
    provider: EvmAddress
    base_token: str
    amount_base: str
    quote_token: str
    amount_quote: str
    event_tag: Literal["add_lp","remove_lp"]

class LiquidityDetailed(Liquidity, tag=True):
    value_avax: int
    value_usd: int

class PositionUpdate(Struct, tag=True):
    timestamp: datetime
    tx_hash: str
    pool: str
    custodian: EvmAddress
    provider: EvmAddress
    receipt_token: EvmAddress
    receipt_id: int
    amount_receipt: str
    event_tag: Literal["add","remove"]