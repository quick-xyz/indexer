from msgspec import Struct
from typing import Optional

from ..decode.model.types import EvmAddress

class StorageConfig(Struct):
    local_dir: str
    storage_rpc_prefix: str
    storage_decoded_prefix: str
    storage_rpc_format: str
    storage_block_format: str

class TokenConfig(Struct):
    name: str
    symbol: str
    type: str
    decimals: int

class AddressConfig(Struct):
    name: str
    type: str
    description: str
    grouping: Optional[str] = None
    tags: Optional[list[str]] = None  

class ABIConfig(Struct):
    abi: list

class TransformerConfig(Struct):
    name: str
    priorities: Optional[dict[str,int]] = None
    description: Optional[str] = None

class ContractConfig(Struct):
    name: str
    project: str
    type: str
    abi_dir: str
    abi: str
    transformer: Optional[TransformerConfig] = None
    description: Optional[str] = None
    version: Optional[str] = None
    implementation: Optional[EvmAddress] = None

class ContractWithABI(Struct):
    contract_info: ContractConfig
    abi: list