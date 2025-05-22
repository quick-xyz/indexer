from typing import Literal, Optional, List

from ...decode.model.evm import EvmAddress
from .base import DomainEvent
from .transfer import Transfer

class Staking(DomainEvent, tag=True):
    contract: EvmAddress
    staker: EvmAddress
    token: EvmAddress
    amount: int
    action: Literal["deposit","withdraw"]
    staking_id: Optional[int] = None
    receipt_token: Optional[EvmAddress] = None
    receipt_id: Optional[int] = None
    amount_receipt: Optional[str] = None
    transfers: Optional[List[Transfer]] = None

class StakingDetailed(Staking, tag=True):
    value_native: int
    value_usd: int