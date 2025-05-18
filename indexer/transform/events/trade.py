from datetime import datetime
from typing import Literal

from ...decode.model.evm import EvmAddress
from .base import DomainEvent

class Trade(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: str
    pool: str
    taker: EvmAddress
    direction: str
    base_token: str
    base_amount: int
    quote_token: str
    quote_amount: int
    event_tag: Literal["buy","sell","arbitrage","trade"] = "trade"

class TradeDetailed(Trade, tag=True):
    price_native: int
    price_usd: int
    value_native: int
    value_usd: int
    price_method: str
    bool_arbitrage: bool