from datetime import datetime
from typing import Literal, Optional

from ...decode.model.evm import EvmAddress,EvmHash
from .base import DomainEvent


class Position(DomainEvent, tag=True):
    pool: EvmAddress
    provider: EvmAddress
    direction: Literal["increase","decrease"]
    receipt_token: EvmAddress
    receipt_id: int
    amount_receipt: int
    amount_base: int
    amount_quote: int
    custodian: Optional[EvmAddress] = None