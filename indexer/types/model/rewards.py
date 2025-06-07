# indexer/types/model/rewards.py

from typing import Literal, Optional, Dict

from ..new import EvmAddress
from .base import DomainEvent, Signal


class RewardSignal(Signal, tag=True):
    contract: EvmAddress
    recipient: EvmAddress
    token: EvmAddress
    amount: str
    reward_type: Literal["rewards","fees"]
    contract_id: Optional[int] = None
    batch: Optional[Dict[int,str]] = None
    sender: Optional[EvmAddress] = None

class Reward(DomainEvent, tag=True):
    contract: EvmAddress
    recipient: EvmAddress
    token: EvmAddress
    amount: str
    reward_type: Literal["rewards","fees"]
    signals: Dict[int,Signal]

    def _get_identifying_content(self):
        return {
            "event_type": "rewards",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "recipient": self.recipient,
            "amount": self.amount,
            "token": self.token,
            "reward_type": self.reward_type,
            "signals": sorted(self.signals.keys()),
        }