# indexer/types/errors.py

from typing import Optional, Dict, Any, Literal
import hashlib
import msgspec
from msgspec import Struct

from ..new import ErrorId, EvmAddress, EvmHash


class ProcessingError(Struct):
    stage: str  # "rpc", "decode", "transform", "storage"
    error_type: str  # "missing_data", "decode_failed", "validation_failed"
    message: str
    status: Literal["unresolved", "resolved"] = "unresolved"
    attempts: int = 0
    error_id: Optional[ErrorId] = None
    context: Optional[Dict[str, Any]] = None  # tx_hash, log_index, contract_address, etc.

    def __post_init__(self) -> None:
        if not self.error_id:
            self.error_id = self.generate_error_id()

    def mark_resolved(self) -> None:
        self.status = "resolved"

    def add_attempt(self) -> None:
        self.attempts += 1
    
    def generate_error_id(self) -> ErrorId:
        content_struct = {
            "stage": self.stage,
            "error_type": self.error_type,
            "message": self.message,
            "context": self.context or {},
        }
        content_bytes = msgspec.msgpack.encode(content_struct)
        hash_hex = hashlib.sha256(content_bytes).hexdigest()

        return ErrorId(hash_hex[:12])


'''
Helper functions to create specific error types
'''
def create_decode_error(
    error_type: str,
    message: str,
    tx_hash: Optional[EvmHash] = None,
    log_index: Optional[int] = None,
    contract_address: Optional[EvmAddress] = None
) -> ProcessingError:
    context = {}
    if tx_hash:
        context["tx_hash"] = tx_hash
    if log_index is not None:
        context["log_index"] = log_index
    if contract_address:
        context["contract_address"] = contract_address
    
    return ProcessingError(
        stage="decode",
        error_type=error_type,
        message=message,
        context=context if context else None
    )


def create_transform_error(
    error_type: str,
    message: str,
    tx_hash: Optional[EvmHash] = None,
    contract_address: Optional[EvmAddress] = None,
    transformer_name: Optional[str] = None,
    log_index: Optional[int] = None
) -> ProcessingError:
    context = {}
    if tx_hash:
        context["tx_hash"] = tx_hash
    if contract_address:
        context["contract_address"] = contract_address
    if transformer_name:
        context["transformer_name"] = transformer_name
    if log_index:
        context["log_index"] = log_index

    return ProcessingError(
        stage="transform",
        error_type=error_type,
        message=message,
        context=context if context else None
    )


def create_rpc_error(
    error_type: str,
    message: str,
    block_number: Optional[int] = None,
    tx_hash: Optional[EvmHash] = None
) -> ProcessingError:
    context = {}
    if block_number is not None:
        context["block_number"] = block_number
    if tx_hash:
        context["tx_hash"] = tx_hash
    
    return ProcessingError(
        stage="rpc",
        error_type=error_type,
        message=message,
        context=context if context else None
    )