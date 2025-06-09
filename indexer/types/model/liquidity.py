# indexer/types/model/liquidity.py

from typing import Literal, Optional, Dict, Tuple

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId, Signal


class LiquiditySignal(Signal, tag=True):
    pool: EvmAddress
    base_amount: str
    base_token: EvmAddress
    quote_amount: str
    quote_token: EvmAddress
    receipt_amount: Optional[str] = None
    batch: Optional[Dict[int,Tuple[int,str]]] = None
    sender: Optional[EvmAddress] = None
    to: Optional[EvmAddress] = None

class Position(DomainEvent, tag=True):
    receipt_token: EvmAddress
    receipt_id: int
    base_amount: str
    quote_amount: str
    signals: Dict[int,Signal]
    receipt_amount: Optional[str] = None
    custodian: Optional[EvmAddress] = None

    def _get_identifying_content(self):
        return {
            "event_type": "liquidity",
            "tx_salt": self.tx_hash,
            "receipt_token": self.receipt_token,
            "receipt_id": self.receipt_id,
            "base_amount": self.base_amount,
            "quote_amount": self.quote_amount,
        }

class Liquidity(DomainEvent, tag=True):
    pool: EvmAddress
    provider: EvmAddress
    base_token: EvmAddress
    base_amount: str
    quote_token: EvmAddress
    quote_amount: str
    action: Literal["add","remove","update"]
    signals: Dict[int,Signal]
    positions: Optional[Dict[DomainEventId,Position]] = None

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