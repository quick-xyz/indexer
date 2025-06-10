# indexer/types/model/staking.py

from typing import Literal, Optional, Dict

from ..new import EvmAddress
from .base import DomainEvent, Signal


class Staking(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    staker: EvmAddress
    token: EvmAddress
    amount: str
    action: Literal["deposit","withdraw"]
    signals: Dict[int,Signal]
    staking_id: Optional[int] = None

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