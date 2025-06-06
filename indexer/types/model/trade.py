# indexer/types/model/trade.py

from typing import Literal, Optional, List, Dict

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId
from .transfer import Transfer
from .auction import AuctionPurchase


class Swap(DomainEvent, tag=True, kw_only=True):
    '''Unknown swap event.'''
    taker: EvmAddress
    direction: Literal["buy","sell"]
    base_token: EvmAddress
    base_amount: str
    quote_token: EvmAddress
    quote_amount: str
    transfers: Optional[Dict[DomainEventId,Transfer]] = None
    batch: Optional[Dict[int,Dict[str,str]]] = None

    def _get_identifying_content(self):
        return {
            "event_type": "swap",
            "tx_salt": self.tx_hash,
            "taker": self.taker,
            "direction": self.direction,
            "base_amount": self.base_amount,
            "quote_amount": self.quote_amount,
        }

class PoolSwap(Swap, tag=True, kw_only=True):
    '''Pool swap event.'''
    pool: EvmAddress

    def _get_identifying_content(self):
        return {
            "event_type": "pool_swap",
            "tx_salt": self.tx_hash,
            "pool": self.pool,
            "taker": self.taker,
            "direction": self.direction,
            "base_amount": self.base_amount,
            "quote_amount": self.quote_amount,
        }

class Trade(DomainEvent, tag=True, kw_only=True):
    '''Top level trade event. Net buy/sell.'''
    taker: EvmAddress
    direction: Literal["buy","sell"]
    base_token: EvmAddress
    base_amount: str
    quote_token: EvmAddress
    quote_amount: str
    trade_type: Literal["arbitrage","trade","auction"] = "trade"
    router: Optional[EvmAddress] = None
    swaps: Optional[Dict[DomainEventId,Swap|PoolSwap|AuctionPurchase]] = None

    def _get_identifying_content(self):
        return {
            "event_type": "trade",
            "tx_salt": self.tx_hash,
            "taker": self.taker,
            "base_token": self.base_token,
            "base_amount": self.base_amount,
            "quote_token": self.quote_token,
            "quote_amount": self.quote_amount,
            "direction": self.direction,
            "trade_type": self.trade_type,
        }