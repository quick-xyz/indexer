# indexer/types/config.py

from typing import Dict, Optional, List, Any
from pathlib import Path

from msgspec import Struct


class GCSConfig(Struct):
    project_id: str
    bucket_name: str
    credentials_path: Optional[str] = None

class StorageConfig(Struct):
    processing_prefix: str
    complete_prefix: str
    processing_format: str
    complete_format: str

class TokenConfig(Struct):
    symbol: Optional[str] = None
    decimals: Optional[int] = None

class AddressConfig(Struct):
    name: str
    type: str
    description: Optional[str] = None
    project: Optional[str] = None
    grouping: Optional[str] = None
    tags: Optional[List[str]] = None

class DecoderConfig(Struct):
    abi_dir: str
    abi: str

class TransformerConfig(Struct):
    name: str
    instantiate: Dict[str, Any]

class ABIConfig(Struct):
    abi: List[Dict[str, Any]]

class ContractConfig(Struct):
    name: str
    project: str
    type: str
    decode: DecoderConfig
    transform: Optional[TransformerConfig] = None
    token: Optional[TokenConfig] = None
    abi: Optional[List[Dict[str, Any]]] = None

class DatabaseConfig(Struct):
    url: str
    pool_size: int = 5
    max_overflow: int = 10

class RpcConfig(Struct):
    endpoint_url: str
    timeout: int = 30
    max_retries: int = 3

class PathsConfig(Struct):
    project_root: Path
    indexer_root: Path
    config_dir: Path
    data_dir: Path
    log_dir: Path
    abi_dir: Path