# indexer/types/model/nfp.py

from typing import Literal, Optional, Dict, Tuple

from ..new import EvmAddress
from .base import DomainEvent, DomainEventId, Signal


class NfpCollectSignal(Signal, tag=True):
    contract: EvmAddress
    token_id: int
    recipient: EvmAddress
    amount0: str
    amount1: str

class NfpLiquiditySignal(Signal, tag=True):
    contract: EvmAddress
    token_id: int
    liquidity: str
    amount0: str
    amount1: str