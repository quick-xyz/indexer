# indexer/types/indexer.py

from typing import Optional, Dict, List
from msgspec import Struct
from datetime import datetime

from .new import HexStr,EvmAddress,EvmHash, DomainEventId, ErrorId
from .model.errors import ProcessingError
from .model.base import DomainEvent
from .model.transfer import Transfer



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
    origin_to: Optional[EvmAddress]
    function: EncodedMethod | DecodedMethod
    value: int
    tx_success: bool
    logs: Dict[int,EncodedLog|DecodedLog]  # keyed by log index
    transfers: Optional[Dict[DomainEventId,Transfer]] = None
    events: Optional[Dict[DomainEventId,DomainEvent]] = None
    errors: Optional[Dict[ErrorId,ProcessingError]] = None
    indexing_status: Optional[str] = None

class Block(Struct):
    block_number: int
    timestamp: int
    transactions: Optional[Dict[EvmHash,Transaction]] = None # keyed by transaction hash
    indexing_status: Optional[str] = None