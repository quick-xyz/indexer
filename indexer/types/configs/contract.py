# indexer/types/config/contract.py

from typing import Dict, Optional, List, Any
from msgspec import Struct
from ..new import EvmAddress


class ContractConfig(Struct):
    address: EvmAddress
    status: str = 'active'
    block_created: Optional[int] = None 
    abi_dir: Optional[str] = None 
    abi_file: Optional[str] = None
    abi: Optional[List[Dict[str, Any]]] = None
    transformer: Optional[str] = None 
    transform_init: Optional[Dict[str, Any]] = None