from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash
from typing import Optional
from .base import DomainEvent

class Fee(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    pool: EvmAddress
    fee_type: str
    token: EvmAddress
    fee_amount: int
    payer: Optional[EvmAddress]

    def _get_identifying_content(self):
        return {
            "event_type": "fee",
            "tx_salt": self.tx_hash,
            "pool": self.pool,
            "fee_type": self.fee_type,
            "payer": self.payer,
            "token": self.token,
            "fee_amount": self.fee_amount,
        }