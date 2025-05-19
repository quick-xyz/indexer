from msgspec import Struct
from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash

from .base import DomainEvent

class Fee(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    taker: EvmAddress
    amount_base: int
    amount_quote: int