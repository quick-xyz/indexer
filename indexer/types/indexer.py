# indexer/types/indexer.py

from typing import Optional, Dict, Literal
from msgspec import Struct

from .new import HexStr,EvmAddress,EvmHash, DomainEventId, ErrorId
from .model.errors import ProcessingError
from .model.base import DomainEvent, Signal


BlockStatus = Literal["rpc", "processing", "complete", "error"]
TransactionStatus = Literal["decoded", "transformed", "error"]

class ProcessingMetadata(Struct, kw_only=True):
    error_count: int = 0
    retry_count: int = 0
    last_error: Optional[str] = None
    started_at: Optional[str] = None  # ISO timestamp
    completed_at: Optional[str] = None
    error_stage: Optional[str] = None  # "decode", "transform"

class EncodedLog(Struct, tag=True):
    index: int
    removed: bool
    contract: EvmAddress
    signature: EvmHash
    topics: list[EvmHash]
    data: HexStr

class DecodedLog(Struct, tag=True):
    index: int
    removed: bool
    contract: EvmAddress
    signature: EvmHash
    name: str
    attributes: dict

class DecodedMethod(Struct, tag=True):
    selector: Optional[HexStr] = None
    name: Optional[str] = None
    args: Optional[dict] = None

class EncodedMethod(Struct, tag=True):
    data: HexStr

class Transaction(Struct):
    block: int
    timestamp: int
    tx_hash: EvmHash
    index: int
    origin_from: EvmAddress
    function: EncodedMethod | DecodedMethod
    value: str  # Changed from int to str
    tx_success: bool
    logs: Dict[int,EncodedLog|DecodedLog]  # keyed by log index
    origin_to: Optional[EvmAddress] = None
    signals: Optional[Dict[int,Signal]] = None
    events: Optional[Dict[DomainEventId,DomainEvent]] = None
    errors: Optional[Dict[ErrorId,ProcessingError]] = None
    indexing_status: Optional[TransactionStatus] = None
    processing_metadata: Optional[ProcessingMetadata] = None

class Block(Struct):
    block_number: int
    timestamp: int
    transactions: Optional[Dict[EvmHash,Transaction]] = None # keyed by transaction hash
    indexing_status: Optional[str] = None
    processing_metadata: Optional[ProcessingMetadata] = None