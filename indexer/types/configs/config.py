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