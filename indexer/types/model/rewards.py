# indexer/types/model/rewards.py

from typing import Literal, Optional, Dict

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId


class Reward(DomainEvent, tag=True, kw_only=True):
    reward_token: EvmAddress
    amount: str
    reward_type: Literal["claim_rewards","claim_fees"]

class RewardSet(DomainEvent, tag=True, kw_only=True):
    contract: EvmAddress
    recipient: EvmAddress
    token: EvmAddress
    amount: str
    rewards: Optional[Dict[DomainEventId,Reward]] = None

    def _get_identifying_content(self):
        return {
            "event_type": "rewards",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "recipient": self.recipient,
            "amount": self.amount,
            "token": self.token,
        }