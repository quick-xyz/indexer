from typing import Literal, Optional, List

from ..new import EvmAddress
from .base import DomainEvent
from .transfer import Transfer


class Staking(DomainEvent, tag=True):
    contract: EvmAddress
    staker: EvmAddress
    token: EvmAddress
    amount: int
    action: Literal["deposit","withdraw"]
    staking_id: Optional[int] = None
    receipt_token: Optional[EvmAddress] = None
    receipt_id: Optional[int] = None
    amount_receipt: Optional[str] = None
    transfers: Optional[List[Transfer]] = None

    def _get_identifying_content(self):
        return {
            "event_type": "staking",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "staker": self.staker,
            "token": self.token,
            "amount": self.amount,
            "action": self.action,
        }