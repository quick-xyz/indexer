# indexer/types/model/positions.py

from typing import Optional

from ..new import EvmAddress
from .base import DomainEvent


class Position(DomainEvent, tag=True):
    user: EvmAddress
    token: EvmAddress
    amount: str
    custodian: Optional[EvmAddress] = None
    token_id: Optional[int] = None

    def _get_identifying_content(self):
        return {
            "event_type": "position",
            "tx_salt": self.tx_hash,
            "user": self.user,
            "token": self.token,
            "amount": self.amount,
        }