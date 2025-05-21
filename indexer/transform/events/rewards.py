from datetime import datetime
from typing import Literal
from msgspec import Struct

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent

class Rewards(DomainEvent, tag=True):
    contract: EvmAddress
    recipient: EvmAddress
    token: EvmAddress
    amount: int
    reward_type: Literal["claim_rewards","claim_fees"]

class RewardsDetailed(Rewards, tag=True):
    value_avax: int
    value_usd: int