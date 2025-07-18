# indexer/types/configs/address.py

from typing import Dict, Optional
from msgspec import Struct
from ..new import EvmAddress


class AddressConfig(Struct):
    address: EvmAddress
    name: str
    type: str
    status: str = 'active'
    description: Optional[str] = None
    project: Optional[str] = None
    subtype: Optional[str] = None