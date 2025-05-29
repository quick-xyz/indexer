from typing import Optional
import hashlib
import msgspec
from msgspec import Struct

from ..new import EvmHash


class DomainEvent(Struct):
    ''' Base class for domain events. '''
    timestamp: int
    tx_hash: EvmHash
    content_id: str = ""
    log_index: Optional[int] = None
    
    def generate_content_id(self) -> str:
        content_struct = self._get_identifying_content()
        content_bytes = msgspec.msgpack.encode(content_struct)
        hash_hex = hashlib.sha256(content_bytes).hexdigest()
        self.content_id = hash_hex[:12]

        return hash_hex[:12]
    
    def _get_identifying_content(self):
        raise NotImplementedError

class ProcessingError(Struct, tag=True):
    stage: str
    error: str
    desc: str