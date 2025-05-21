from typing import Literal, List, Optional
from msgspec import Struct

from ...decode.model.evm import EvmAddress
from .base import DomainEvent

class TransferIds(Struct, tag=True):
    id: int
    amount: int

class Transfer(DomainEvent, tag=True):
    token: EvmAddress
    amount: int
    from_address: EvmAddress
    to_address: EvmAddress
    transfer_type: Literal["transfer","transfer_batch"] = "transfer"
    batch: Optional[List[TransferIds]] = None
    
class TransferDetailed(Transfer, tag=True):
    value_avax: int
    value_usd: int