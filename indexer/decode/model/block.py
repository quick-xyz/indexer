from typing import Optional
from msgspec import Struct
from datetime import datetime

from .types import HexStr,EvmAddress,EvmHash


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
    tx_hash: EvmHash
    index: int
    origin_from: EvmAddress
    origin_to: Optional[EvmAddress]
    function: EncodedMethod | DecodedMethod
    tx_success: bool
    logs: dict[str,EncodedLog|DecodedLog]  # key: "{tx_hash}_{log_index}" aka log_id
    events: Optional[list[dict]] = None

class Block(Struct):
    block_number: int
    timestamp: datetime
    transactions: Optional[list[Transaction]] = None