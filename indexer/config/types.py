from msgspec import Struct
from typing import Optional, Dict, List

from ..decode.model.types import EvmAddress


class StorageConfig(Struct):
    rpc_prefix: str
    decoded_prefix: str
    rpc_format: str
    decoded_format: str


class TokenConfig(Struct):
    symbol: str
    decimals: int


class AddressConfig(Struct):
    name: str
    type: str
    description: str
    grouping: Optional[str] = None
    tags: Optional[List[str]] = None  


class ABIConfig(Struct):
    abi: list


class TransformerConfig(Struct):
    name: str
    instantiate: Dict[str,any]
    transfers: Optional[Dict[str,int]] = None
    logs: Optional[Dict[str,int]] = None


class DecodeConfig(Struct):
    abi_dir: str
    abi: str


class ContractConfig(Struct):
    name: str
    project: str
    type: str
    decode: DecodeConfig
    transform: TransformerConfig


class ContractWithABI(Struct):
    contract_info: ContractConfig
    abi: list