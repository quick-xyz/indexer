# indexer/types/model/positions.py

from typing import Optional

from ..new import EvmAddress
from .base import DomainEvent


class Position(DomainEvent, tag=True):
    user: EvmAddress
    custodian: EvmAddress = user
    token: EvmAddress
    amount: str
    token_id: Optional[int] = None

    def _get_identifying_content(self):
        return {
            "event_type": "liquidity",
            "tx_salt": self.tx_hash,
            "user": self.user,
            "custodian": self.custodian,
            "token": self.token,
            "amount": self.amount,
            "token_id": self.token_id,
        }