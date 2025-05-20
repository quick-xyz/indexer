from datetime import datetime
from msgspec import Struct
from typing import Optional

from ...decode.model.types import EvmHash, EvmAddress
from ...decode.model.block import DecodedMethod

class DomainEvent(Struct):
    ''' Base class for domain events. '''
    timestamp: datetime
    tx_hash: EvmHash

class TransactionContext(Struct):
    timestamp: datetime
    tx_hash: EvmHash
    sender: EvmAddress
    contract: Optional[EvmAddress]
    function: Optional[DecodedMethod]
    value: int