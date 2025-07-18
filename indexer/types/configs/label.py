# indexer/types/configs/label.py

from typing import Optional
from msgspec import Struct

from ..new import EvmAddress


class LabelConfig(Struct):
    address: EvmAddress
    value: str
    created_by: str
    status: str = 'active'
    type: Optional[str] = None
    subtype: Optional[str] = None