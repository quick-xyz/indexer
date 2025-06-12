# indexer/types/model/rewards.py

from typing import Literal, Optional, Dict

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId, Signal
from .positions import Position

class CollectSignal(Signal, tag=True):
    contract: EvmAddress
    recipient: EvmAddress
    base_amount: str
    base_token: EvmAddress
    quote_amount: str
    quote_token: EvmAddress
    reward_type: Literal["rewards","fees"] = "fees"
    owner: Optional[EvmAddress] = None
    sender: Optional[EvmAddress] = None

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
    positions: Dict[DomainEventId,Position]
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