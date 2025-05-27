from typing import Optional
from msgspec import Struct
from datetime import datetime

from .types import HexStr,EvmAddress,EvmHash
from ...transform.events.base import DomainEvent
from ...transform.events.transfer import Transfer

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
    logs: dict[int,EncodedLog|DecodedLog]  # keyed by log index
    transfers: Optional[dict[str,Transfer]] = None # keyed by indexer hashing function
    events: Optional[dict[str,DomainEvent]] = None # keyed by indexer hashing function
    errors: Optional[list] = None

class Block(Struct):
    block_number: int
    timestamp: int
    transactions: Optional[dict[EvmHash,Transaction]] = None # keyed by transaction hash