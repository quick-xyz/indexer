# indexer/types/configs/address.py

from typing import Dict, Optional, List, Any, Literal

import msgspec
from msgspec import Struct

from ..new import EvmAddress
from ...database.shared.tables.config import Address

class Label(Struct):
    value: str
    type: str
    subtype: str
    created_by: str
    created_at: Any

class AddressConfig(Struct):
    network: str = 'avalanche'
    address: EvmAddress
    name: str
    type: str
    status: Literal['active','inactive'] = 'active'
    description: Optional[str] = None
    project: Optional[str] = None
    subtype: Optional[str] = None
    tags: Optional[List[Label]] = None

    @classmethod
    def from_yaml_dict(cls, data: Dict[str, Any]) -> 'AddressConfig':
        """Create AddressConfig from YAML dictionary with validation"""
        return cls(**data)
    
    def to_database_dict(self) -> Dict[str, Any]:
        tags_json = None
        if self.tags:
            tags_json = [msgspec.structs.asdict(tag) for tag in self.tags]
        
        return {
            'network': self.network,
            'address': self.address.lower(),
            'name': self.name,
            'project': self.project,
            'description': self.description,
            'type': self.type,
            'subtype': self.subtype,
            'tags': tags_json,
            'status': self.status,
        }

    @classmethod
    def from_database(cls, address: Address) -> 'AddressConfig':
        """Convert database Address back to AddressConfig"""
        tags = None
        if address.tags:
            tags = [Label(**tag_dict) for tag_dict in address.tags]
        
        return cls(
            network=address.network,
            address=address.address,
            name=address.name,
            type=address.type,
            status=address.status,
            description=address.description,
            project=address.project,
            subtype=address.subtype,
            tags=tags
        )
