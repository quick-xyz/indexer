from typing import Literal, Optional, List

from ...decode.model.evm import EvmAddress
from .base import DomainEvent
from .transfer import Transfer


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

'''
class SwapValued(Swap, tag=True):
    price_native: int
    price_usd: int
    value_native: int
    value_usd: int
    price_method: str
    bool_arbitrage: bool
'''