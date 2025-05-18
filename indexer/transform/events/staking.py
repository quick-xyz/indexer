from datetime import datetime
from typing import Literal
from msgspec import Struct

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent

class Staking(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    contract: EvmAddress
    staker: EvmAddress
    token: EvmAddress
    amount: int
    value_avax: int
    value_usd: int
    event_tag: Literal["stake","unstake"]