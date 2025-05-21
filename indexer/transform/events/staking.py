from typing import Literal, Optional

from ...decode.model.evm import EvmAddress
from .base import DomainEvent

class Staking(DomainEvent, tag=True):
    contract: EvmAddress
    staker: EvmAddress
    token: EvmAddress
    amount: int
    event_tag: Literal["stake","unstake"]
    receipt_token: Optional[EvmAddress] = None
    receipt_id: Optional[int] = None
    amount_receipt: Optional[str] = None

class StakingDetailed(Staking, tag=True):
    value_native: int
    value_usd: int