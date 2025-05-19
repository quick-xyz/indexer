from datetime import datetime
from typing import Literal

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent

class BinTransfer(DomainEvent, tag=True):
    timestamp: datetime
    tx_hash: EvmHash
    pool: EvmAddress
    bin: int
    token: EvmAddress
    amount: EvmAddress
    from_address: EvmAddress
    to_address: EvmAddress
    event_tag: Literal["transfer_out","transfer_in","transfer"] = "transfer"

class BinTransferDetailed(BinTransfer, tag=True):
    value_avax: int
    value_usd: int