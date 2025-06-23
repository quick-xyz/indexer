# indexer/database/models/types.py

from typing import Optional
from sqlalchemy.dialects.postgresql import VARCHAR
from ...types.new import EvmAddress, EvmHash, DomainEventId


class EvmAddressType(VARCHAR):
    def __init__(self):
        super().__init__(42)
    
    def process_bind_param(self, value: Optional[EvmAddress], dialect) -> Optional[str]:
        return str(value).lower() if value else None
    
    def process_result_value(self, value: Optional[str], dialect) -> Optional[EvmAddress]:
        return EvmAddress(value) if value else None


class EvmHashType(VARCHAR):
    def __init__(self):
        super().__init__(66)
    
    def process_bind_param(self, value: Optional[EvmHash], dialect) -> Optional[str]:
        return str(value).lower() if value else None
    
    def process_result_value(self, value: Optional[str], dialect) -> Optional[EvmHash]:
        return EvmHash(value) if value else None


class DomainEventIdType(VARCHAR):
    def __init__(self):
        super().__init__(12)
    
    def process_bind_param(self, value: Optional[DomainEventId], dialect) -> Optional[str]:
        return str(value) if value else None
    
    def process_result_value(self, value: Optional[str], dialect) -> Optional[DomainEventId]:
        return DomainEventId(value) if value else None