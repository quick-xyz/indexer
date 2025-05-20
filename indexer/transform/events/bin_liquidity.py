from datetime import datetime
from typing import Literal

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent

class BinLiquidity(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    pool: EvmAddress
    bin: int
    provider: EvmAddress
    amount_base: int
    amount_quote: int
    event_tag: Literal["add_lp","remove_lp"]

class Position(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    pool: EvmAddress
    bin: int
    provider: EvmAddress
    amount_base: int
    amount_quote: int
    event_tag: Literal["add_lp","remove_lp"]


class BinLiquidityDetailed(BinLiquidity, tag=True):
    value_avax: int
    value_usd: int