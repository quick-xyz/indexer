from msgspec import Struct
from datetime import datetime
from ...decode.model.evm import EvmAddress,EvmHash

from .base import DomainEvent

class Fee(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    pool: EvmAddress
    fee_type: str
    payer: EvmAddress
    token: EvmAddress
    fee_amount: int