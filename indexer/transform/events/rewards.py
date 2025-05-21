from typing import Literal, Optional, List
from msgspec import Struct

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent

class Reward(Struct, tag=True):
    reward_token: EvmAddress
    amount: int
    reward_type: Literal["claim_rewards","claim_fees"]

class Rewards(DomainEvent, tag=True):
    contract: EvmAddress
    recipient: EvmAddress
    token: EvmAddress
    amount: int
    rewards: Optional[List[Reward]] = None

class RewardsDetailed(Rewards, tag=True):
    value_native: int
    value_usd: int