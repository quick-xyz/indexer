# indexer/types/model/transfer.py

from typing import Literal, Optional, Dict

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId, Signal
from .positions import Position


class TransferSignal(Signal, tag=True):
    token: EvmAddress
    from_address: EvmAddress
    to_address: EvmAddress
    amount: str
    token_id: Optional[int] = None
    batch: Optional[Dict[str,str]] = None
    sender: Optional[EvmAddress] = None

class Transfer(DomainEvent, tag=True):
    token: EvmAddress
    from_address: EvmAddress
    to_address: EvmAddress
    amount: str
    positions: Dict[DomainEventId,Position]
    signals: Dict[int,Signal]
    
    def _get_identifying_content(self):
        return {
            "event_type": "transfer",
            "tx_salt": self.tx_hash,
            "token": self.token,
            "from_address": self.from_address,
            "to_address": self.to_address,
            "amount": self.amount,
            "signals": sorted(self.signals.keys()),
        }