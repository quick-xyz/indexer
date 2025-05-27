from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash

from .base import DomainEvent

class Fee(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    pool: EvmAddress
    fee_type: str
    payer: EvmAddress
    token: EvmAddress
    fee_amount: int

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