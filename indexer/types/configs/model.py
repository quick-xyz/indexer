# indexer/types/configs/model.py

from typing import Optional
from msgspec import Struct
from ..new import EvmAddress


class ModelConfig(Struct):
    id: str
    name: str
    version: str = 'v1'
    network: str = 'avalanche'
    shared_db: str
    model_db: str
    status: str = 'active'
    description: Optional[str] = None
    model_token: Optional[EvmAddress] = None