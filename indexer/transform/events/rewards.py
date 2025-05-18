from datetime import datetime
from typing import Literal
from msgspec import Struct

from ...decode.model.evm import EvmAddress,EvmHash

class Rewards(Struct, tag=True):
    timestamp: datetime
    tx_hash: str
    contract: str
    address: EvmAddress
    token: EvmAddress
    amount: int
    event_tag: Literal["claim_rewards","claim_fees"]

class RewardsDetailed(Rewards, tag=True):
    value_avax: int
    value_usd: int