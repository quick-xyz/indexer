from typing import Literal, List, Optional

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent
from .position import Position
from .transfer import Transfer

class Liquidity(DomainEvent, tag=True):
    pool: EvmAddress
    provider: EvmAddress
    base_token: EvmAddress
    amount_base: int
    quote_token: EvmAddress
    amount_quote: int
    liquidity_type: Literal["add_lp","remove_lp","update_lp"]
    positions: Optional[List[Position]] = None
    transfers: Optional[List[Transfer]] = None
    custodian: Optional[EvmAddress] = None

class LiquidityDetailed(Liquidity, tag=True):
    value_avax: int
    value_usd: int