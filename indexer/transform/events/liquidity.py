from datetime import datetime
from typing import Literal
from msgspec import Struct

from ...decode.model.evm import EvmAddress,EvmHash

class Liquidity(Struct, tag=True):
    timestamp: datetime
    tx_hash: str
    pool: str
    provider: EvmAddress
    amount_smol: str
    amount_avax: str
    amount_receipt: str
    event_tag: Literal["add_lp","remove_lp"]

class LiquidityDetailed(Liquidity, tag=True):
    value_avax: int
    value_usd: int