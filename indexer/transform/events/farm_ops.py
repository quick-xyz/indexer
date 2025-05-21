from typing import List, Optional

from msgspec import Struct
from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash

from .base import DomainEvent

class Farm(DomainEvent, tag=True):
    contract: EvmAddress
    farm_id: int
    deposit_token: EvmAddress
    reward_token: EvmAddress
    reward_rate: int
    rewarder: Optional[str] = None