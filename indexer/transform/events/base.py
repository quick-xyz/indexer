from datetime import datetime
from msgspec import Struct

from ...decode.model.types import EvmHash

class DomainEvent(Struct):
    ''' Base class for domain events. '''
    timestamp: datetime
    tx_hash: EvmHash