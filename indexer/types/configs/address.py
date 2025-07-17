# indexer/types/configs/address.py

from typing import Dict, Optional, Any, Literal

from msgspec import Struct

from ..new import EvmAddress
from ...database.shared.tables.config.config import Address


class AddressConfig(Struct):
    address: EvmAddress
    name: str
    type: str
    status: Literal['active','inactive'] = 'active'
    description: Optional[str] = None
    project: Optional[str] = None
    subtype: Optional[str] = None

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'AddressConfig':
        """Create AddressConfig from YAML dictionary with validation"""
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        return {
            'address': self.address.lower(),
            'name': self.name,
            'project': self.project,
            'description': self.description,
            'type': self.type,
            'subtype': self.subtype,
            'status': self.status,
        }

    @classmethod
    def from_database(cls, address: Address) -> 'AddressConfig':
        """Convert database Address back to AddressConfig"""
        return cls(
            address=address.address,
            name=address.name,
            type=address.type,
            status=address.status,
            description=address.description,
            project=address.project,
            subtype=address.subtype,
        )
