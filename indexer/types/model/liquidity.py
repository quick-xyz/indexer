# indexer/types/model/liquidity.py

from typing import Literal, Optional, Dict, Tuple

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId, Signal
from .positions import Position

class LiquiditySignal(Signal, tag=True):
    pool: EvmAddress
    base_amount: str
    base_token: EvmAddress
    quote_amount: str
    quote_token: EvmAddress
    action: Literal["add","remove","update"] = "update"
    receipt_amount: Optional[str] = None
    batch: Optional[Dict[str,Dict[str,str]]] = None
    sender: Optional[EvmAddress] = None
    owner: Optional[EvmAddress] = None

class Liquidity(DomainEvent, tag=True):
    pool: EvmAddress
    provider: EvmAddress
    base_token: EvmAddress
    base_amount: str
    quote_token: EvmAddress
    quote_amount: str
    action: Literal["add","remove","update"]
    positions: Dict[DomainEventId,Position]
    signals: Dict[int,Signal]
    batch: Optional[Dict[str,Dict[str,str]]] = None

    def _get_identifying_content(self):
        return {
            "event_type": "liquidity",
            "tx_salt": self.tx_hash,
            "pool": self.pool,
            "provider": self.provider,
            "base_amount": self.base_amount,
            "quote_amount": self.quote_amount,
            "action": self.action,
        }