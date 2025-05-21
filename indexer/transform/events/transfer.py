from datetime import datetime
from typing import Literal

from ...decode.model.evm import EvmAddress
from .base import DomainEvent

class Transfer(DomainEvent, tag=True):
    token: EvmAddress
    amount: EvmAddress
    from_address: EvmAddress
    to_address: EvmAddress
    event_tag: Literal["transfer_out","transfer_in","transfer"] = "transfer"

class TransferDetailed(Transfer, tag=True):
    value_avax: int
    value_usd: int