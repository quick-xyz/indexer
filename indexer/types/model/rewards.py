# indexer/types/model/rewards.py

from typing import Literal, Optional, List

from ..new import EvmAddress
from .base import DomainEvent


class Reward(DomainEvent, tag=True):
    reward_token: EvmAddress
    amount: int
    reward_type: Literal["claim_rewards","claim_fees"]

class RewardSet(DomainEvent, tag=True):
    contract: EvmAddress
    recipient: EvmAddress
    token: EvmAddress
    amount: int
    rewards: Optional[List[Reward]] = None

    def _get_identifying_content(self):
        return {
            "event_type": "rewards",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "recipient": self.recipient,
            "amount": self.amount,
            "token": self.token,
        }