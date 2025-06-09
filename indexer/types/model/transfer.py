# indexer/types/model/transfer.py

from typing import Literal, Optional, Dict

from ..new import EvmAddress
from .base import DomainEvent, Signal


class TransferSignal(Signal, tag=True):
    token: EvmAddress
    from_address: EvmAddress
    to_address: EvmAddress
    amount: str
    batch: Optional[Dict[str,str]] = None
    sender: Optional[EvmAddress] = None

class Transfer(DomainEvent, tag=True):
    token: EvmAddress
    from_address: EvmAddress
    to_address: EvmAddress
    amount: str
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

class TransferLedger(DomainEvent, tag=True, kw_only=True):
    token: EvmAddress
    address: EvmAddress
    amount: str
    direction: Literal["out","in"]
    signals: Dict[int,Signal]
    desc: Optional[str] = None

    def _get_identifying_content(self):
        return {
            "event_type": "transfer_ledger",
            "tx_salt": self.tx_hash,
            "token": self.token,
            "address": self.address,
            "amount": self.amount,
            "direction": self.direction,
            "signals": sorted(self.signals.keys()),
        }