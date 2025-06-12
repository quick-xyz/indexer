# indexer/types/model/base.py

from typing import Dict, Any
import hashlib
import msgspec
from msgspec import Struct

from ..new import EvmHash, DomainEventId


class Signal(Struct):
    log_index: int
    pattern: str 
    
    @property
    def signal_type(self) -> str:
        return self.__class__.__name__
    
    def to_dict(self) -> Dict[str, Any]:
        return msgspec.structs.asdict(self)

class DomainEvent(Struct):
    timestamp: int
    tx_hash: EvmHash

    @property
    def content_id(self) -> DomainEventId:
        if not hasattr(self, '_content_id'):
            self._content_id = self._generate_content_id()
        return self._content_id

    def _generate_content_id(self) -> str:
        content_struct = self._get_identifying_content()
        content_bytes = msgspec.msgpack.encode(content_struct)
        hash_hex = hashlib.sha256(content_bytes).hexdigest()
        self._content_id = hash_hex[:12]
        return hash_hex[:12]
    
    def _get_identifying_content(self):
        raise NotImplementedError

    def to_dict(self) -> Dict[str, Any]:
        return msgspec.structs.asdict(self)
