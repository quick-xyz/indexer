# indexer/transform/transformers/base.py

from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict, Tuple
import msgspec

from ...types import (
    ZERO_ADDRESS,
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    Signal,
    ProcessingError,
    Transfer,
    DomainEventId,
    ErrorId,
    create_transform_error,
    EvmHash,
)
from ...utils.amounts import amount_to_int, amount_to_str, is_positive, is_zero, compare_amounts
from ...core.mixins import LoggingMixin


class BaseTransformer(ABC, LoggingMixin):
    def __init__(self, contract_address: Optional[str] = None):
        self.contract_address = EvmAddress(contract_address.lower()) if contract_address else None
        self.name = self.__class__.__name__
        
        # Log initialization
        self.log_info("Transformer initialized", 
                     contract_address=self.contract_address,
                     transformer_name=self.name)
    
    @abstractmethod
    def process_logs(self, logs: List[DecodedLog]) -> Tuple[
        Optional[Dict[int, Signal]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        raise NotImplementedError

    # ERROR HANDLING METHODS
    def _create_transform_error(self, error_type: str, message: str, 
                               log_index: Optional[int] = None, 
                               tx_hash: Optional[EvmHash] = None) -> ProcessingError:
        return create_transform_error(
            error_type=error_type,
            message=message,
            tx_hash=tx_hash,
            contract_address=self.contract_address,
            transformer_name=self.name,
            log_index=log_index
        )

    def _create_log_exception(self, e: Exception, log_index: int, 
                             errors: Dict[ErrorId, ProcessingError],tx_hash: Optional[EvmHash] = None) -> None:
        self.log_error("Log processing exception",
                      error=str(e),
                      exception_type=type(e).__name__,
                      tx_hash=tx_hash,
                      contract_address=self.contract_address,
                      transformer_name=self.name,
                      log_index=log_index
                      )
        
        error = self._create_transform_error(
            error_type="processing_exception",
            message=f"Log processing exception: {str(e)}",
            log_index=log_index,
        )
        errors[error.error_id] = error

    def _create_attr_error(self, log_index: int,errors: Dict[ErrorId, ProcessingError]) -> None:
        error = self._create_transform_error(
            error_type="missing_attributes",
            message="Transformer missing required attributes in log",
            log_index=log_index
        )
        errors[error.error_id] = error

    def _validate_null_attr(self, values: List[Any], log_index: int, 
                      errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not all(value is not None for value in values):
            self.log_warning("Attribute validation failed",
                           log_index=log_index,
                           null_values=sum(1 for v in values if v is None),
                           total_values=len(values),
                           )
            
            self._create_attr_error(log_index, errors)
            return False
            
        self.log_debug("Attribute validation passed",log_index=log_index)
        return True
    
    def _get_swap_direction(self, base_amount: str) -> str:
        return "buy" if is_positive(base_amount) else "sell"

    def _get_base_quote_amounts(self, amount0: str, amount1: str, token0: EvmAddress, 
                               token1: EvmAddress, base_token: EvmAddress) -> Tuple[str, str]:
        if token0 == base_token:
            return amount_to_str(abs(amount_to_int(amount0))), amount_to_str(abs(amount_to_int(amount1)))
        elif token1 == base_token:
            return amount_to_str(abs(amount_to_int(amount1))), amount_to_str(abs(amount_to_int(amount0)))
        else:
            return None, None

    def _validate_addresses(self, *addresses: str) -> bool:
        for addr in addresses:
            if not addr or addr == ZERO_ADDRESS or len(addr) != 42:
                return False
        return True

    def _validate_amounts(self, *amounts: str) -> bool:
        for amount in amounts:
            if not isinstance(amount, str) or not is_positive(amount):
                return False
        return True

