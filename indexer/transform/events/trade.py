from typing import Literal, Optional, List

from ...decode.model.evm import EvmAddress
from .base import DomainEvent
from .transfer import Transfer
from .auction import Auction

class Swap(DomainEvent, tag=True):
    '''Unknown swap event.'''
    taker: EvmAddress
    direction: Literal["buy","sell"]
    base_token: EvmAddress
    base_amount: int
    quote_token: EvmAddress
    quote_amount: int
    transfers: Optional[List[Transfer]] = None

class PoolSwap(Swap, tag=True):
    '''Pool swap event.'''
    pool: EvmAddress

class Trade(DomainEvent, tag=True):
    '''Top level trade event. Net buy/sell.'''
    taker: EvmAddress
    direction: Literal["buy","sell"]
    base_token: EvmAddress
    base_amount: int
    quote_token: EvmAddress
    quote_amount: int
    trade_type: Literal["arbitrage","trade","auction"] = "trade"
    router: Optional[EvmAddress] = None
    swaps: Optional[List[Swap|PoolSwap|Auction]] = None
