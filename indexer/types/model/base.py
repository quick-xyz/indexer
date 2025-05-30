# indexer/types/model/base.py

from typing import Optional
import hashlib
import msgspec
from msgspec import Struct

from ..new import EvmHash, DomainEventId


class DomainEvent(Struct):
    ''' Base class for domain events. '''
    content_id: Optional[DomainEventId] = None
    timestamp: int
    tx_hash: EvmHash
    log_index: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.content_id:
            self.content_id = self._generate_content_id()
    
    def _generate_content_id(self) -> str:
        content_struct = self._get_identifying_content()
        content_bytes = msgspec.msgpack.encode(content_struct)
        hash_hex = hashlib.sha256(content_bytes).hexdigest()
        self.content_id = hash_hex[:12]

        return hash_hex[:12]
    
    def _get_identifying_content(self):
        raise NotImplementedError
