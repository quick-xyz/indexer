# indexer/types/model/trade.py

from typing import Literal, Optional, Dict, Tuple

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId, Signal
from .auction import AuctionPurchase


class SwapBatchSignal(Signal, tag=True):
    pool: EvmAddress
    to: EvmAddress
    id: int
    base_amount: str
    quote_amount: str
    sender: Optional[EvmAddress] = None

class SwapSignal(Signal, tag=True):
    pool: EvmAddress
    base_amount: str
    base_token: EvmAddress
    quote_amount: str
    quote_token: EvmAddress
    to: EvmAddress
    sender: Optional[EvmAddress] = None
    batch: Optional[Dict[str,Dict[str,str]]] = None

class RouterSignal(Signal, tag=True):
    pool: EvmAddress
    base_amount: str
    base_token: EvmAddress
    quote_amount: str
    quote_token: EvmAddress
    to: EvmAddress
    sender: Optional[EvmAddress] = None
    batch: Optional[Dict[str,Dict[str,str]]] = None

class PoolSwap(DomainEvent, tag=True):
    pool: EvmAddress
    taker: EvmAddress
    direction: Literal["buy","sell"]
    base_token: EvmAddress
    base_amount: str
    quote_token: EvmAddress
    quote_amount: str
    batch: Optional[Dict[int,Dict[str,str]]] = None
    signals: Optional[Dict[int,Signal]] = None

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

class Trade(DomainEvent, tag=True):
    '''Top level trade event '''
    taker: EvmAddress
    direction: Literal["buy","sell"]
    base_token: EvmAddress
    base_amount: str
    quote_token: EvmAddress
    quote_amount: str
    trade_type: Literal["arbitrage","trade","auction"] = "trade"
    router: Optional[EvmAddress] = None
    swaps: Optional[Dict[DomainEventId,PoolSwap|AuctionPurchase]] = None
    signals: Optional[Dict[int,Signal]] = None

    def _get_identifying_content(self):
        return {
            "event_type": "trade",
            "tx_salt": self.tx_hash,
            "taker": self.taker,
            "direction": self.direction,
            "base_token": self.base_token,
            "base_amount": self.base_amount,
            "quote_token": self.quote_token,
            "quote_amount": self.quote_amount,
            "trade_type": self.trade_type,
        }