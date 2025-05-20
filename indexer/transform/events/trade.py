from typing import Literal, Optional, List

from ...decode.model.evm import EvmAddress
from .base import DomainEvent
from .swap import Swap, PoolSwap

class Trade(DomainEvent, tag=True):
    '''Top level trade event. Net buy/sell.'''
    taker: EvmAddress
    direction: Literal["buy","sell"]
    base_token: EvmAddress
    base_amount: int
    quote_token: EvmAddress
    quote_amount: int
    trade_type: Literal["arbitrage","trade"] = "trade"
    router: Optional[EvmAddress] = None
    swaps: Optional[List[Swap|PoolSwap]] = None

'''
class TradeDetailed(Trade, tag=True):
    price_native: int
    price_usd: int
    value_native: int
    value_usd: int
    price_method: str
    bool_arbitrage: bool
'''