# indexer/types/configs/label.py

from typing import Optional, Dict, Any

from msgspec import Struct

from ..new import EvmAddress
from ...database.shared.tables.config.config import Label

class Label(Struct):
    address: EvmAddress
    value: str
    created_by: str
    status: str = 'active'
    type: Optional[str] = None
    subtype: Optional[str] = None

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'Label':
        """Create Label from YAML dictionary with validation"""
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        return {
            'address': self.address.lower(),
            'value': self.value,
            'created_by': self.created_by,
            'type': self.type,
            'subtype': self.subtype,
            'status': self.status,
        }