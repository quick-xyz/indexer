# indexer/types/model/transfer.py

from typing import Literal, List, Optional, Dict

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId


class Transfer(DomainEvent, tag=True):
    token: EvmAddress
    amount: int
    from_address: EvmAddress
    to_address: EvmAddress
    transfer_type: Literal["transfer","transfer_batch"] = "transfer"
    batch: Optional[Dict[int,int]] = None # {id: amount}
    
    def _get_identifying_content(self):
        return {
            "event_type": "transfer",
            "tx_salt": self.tx_hash,
            "token": self.token,
            "from_address": self.from_address,
            "to_address": self.to_address,
            "amount": self.amount,
            "transfer_type": self.transfer_type,
        }

class UnmatchedTransfer(Transfer, tag=True):
    pass

class MatchedTransfer(Transfer, tag=True):
    pass

class TransferLedger(DomainEvent, tag=True):
    token: EvmAddress
    address: EvmAddress
    amount: int
    action: Literal["sent","received"]
    transfers: Optional[Dict[DomainEventId,Transfer]] = None
    desc: Optional[str] = None

    
    def _get_identifying_content(self):
        return {
            "event_type": "transfer_ledger",
            "tx_salt": self.tx_hash,
            "token": self.token,
            "address": self.from_address,
            "amount": self.to_address,
            "action": self.total_amount,
            "transfers": [transfer.content_id for transfer in self.transfers],
        }