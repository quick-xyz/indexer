from msgspec import Struct
import msgspec
import hashlib

from ...decode.model.types import EvmHash

class DomainEvent(Struct):
    ''' Base class for domain events. '''
    timestamp: int
    tx_hash: EvmHash
    content_id: str = ""

    def generate_content_id(self) -> str:
        content_struct = self._get_identifying_content()
        content_bytes = msgspec.msgpack.encode(content_struct)
        hash_hex = hashlib.sha256(content_bytes).hexdigest()
        
        return hash_hex[:12]
    
    def _get_identifying_content(self):
        raise NotImplementedError