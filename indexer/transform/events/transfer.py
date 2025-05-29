from typing import Literal, List, Optional
from msgspec import Struct

from ...decode.model.evm import EvmAddress
from .base import DomainEvent

class TransferIds(Struct, tag=True):
    id: int
    amount: int

class Transfer(DomainEvent, tag=True):
    token: EvmAddress
    amount: int
    from_address: EvmAddress
    to_address: EvmAddress
    transfer_type: Literal["transfer","transfer_batch"] = "transfer"
    matched: bool = False
    batch: Optional[List[TransferIds]] = None
    
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