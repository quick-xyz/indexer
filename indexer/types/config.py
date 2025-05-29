from typing import Dict, Optional, List, Any
from pathlib import Path

from msgspec import Struct


class StorageConfig(Struct, frozen=True):
    rpc_prefix: str
    decoded_prefix: str
    rpc_format: str
    decoded_format: str

class TokenConfig(Struct, frozen=True):
    symbol: str
    decimals: int

class AddressConfig(Struct, frozen=True):
    name: str
    type: str
    description: str
    grouping: Optional[str] = None
    tags: Optional[List[str]] = None

class DecoderConfig(Struct, frozen=True):
    abi_dir: str
    abi: str

class TransformerConfig(Struct, frozen=True):
    name: str
    instantiate: Dict[str, Any]
    transfers: Optional[Dict[str, int]] = None
    logs: Optional[Dict[str, int]] = None

class ContractConfig(Struct, frozen=True):
    name: str
    project: str
    type: str
    decode: DecoderConfig
    transform: Optional[TransformerConfig] = None

class ABIConfig(Struct, frozen=True):
    abi: List[Dict[str, Any]]

class ContractWithABI(Struct, frozen=True):
    contract_info: ContractConfig
    abi: List[Dict[str, Any]]

class DatabaseConfig(Struct, frozen=True):
    url: str
    pool_size: int = 5
    max_overflow: int = 10

class RPCConfig(Struct, frozen=True):
    endpoint_url: str
    timeout: int = 30
    max_retries: int = 3

class PathsConfig(Struct, frozen=True):
    project_root: Path
    indexer_root: Path
    config_dir: Path
    data_dir: Path
    log_dir: Path
    abi_dir: Path