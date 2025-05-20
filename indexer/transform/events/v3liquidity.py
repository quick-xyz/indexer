from datetime import datetime
from typing import Literal

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent

class V3Liquidity(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: str
    pool: str
    router: EvmAddress
    nft_id: int
    provider: EvmAddress
    amount_base: str
    amount_quote: str
    amount_receipt: str
    event_tag: Literal["add_lp","remove_lp"]

class V3LiquidityDetailed(V3Liquidity, tag=True):
    value_avax: int
    value_usd: int