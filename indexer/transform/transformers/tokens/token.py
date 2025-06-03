from typing import List, Optional, Dict, Tuple, Any

from ..base import BaseTransformer
from ....types import (
    ProcessingError,    
    DecodedLog,
    Transaction, 
    EvmHash,   
    EvmAddress,    
    Transfer,
    UnmatchedTransfer,
    DomainEventId,
    ErrorId,
    create_transform_error,
)

class TokenTransformer(BaseTransformer):
    def __init__(self, contract: str):
        super().__init__(contract_address=EvmAddress(str(contract).lower()))

    def _create_log_exception(self, e, tx_hash: EvmHash, log_index: int, transformer_name: str, error_dict: Dict[ErrorId,ProcessingError]) -> None:
        """ Create a ProcessingError for exceptions """
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Log processing exception: {str(e)}",
            tx_hash=tx_hash,
            log_index=log_index,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
        return None

    def _validate_attr(self, values: List[Any],tx_hash: EvmHash, log_index: int, error_dict: Dict[ErrorId,ProcessingError]) -> bool:
        """ Validate that all required attributes are present """
        if not all(value is not None for value in values):
            error = create_transform_error(
                error_type="missing_attributes",
                message=f"Transformer missing required attributes in log",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False
        return True

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]],Optional[Dict[ErrorId,ProcessingError]]]:
        transfers = {}
        errors = {}
    
        for log in logs:
            try:
                if log.name == "Transfer":
                    from_addr = EvmAddress(str(log.attributes.get("from")).lower())
                    to_addr = EvmAddress(str(log.attributes.get("to")).lower())
                    value = int(log.attributes.get("value"))   
                                    
                    if not self._validate_attr([from_addr, to_addr, value], tx.tx_hash, log.index, errors):
                        continue

                    transfer = UnmatchedTransfer(
                        timestamp=tx.timestamp,
                        tx_hash=tx.tx_hash,
                        from_address=from_addr,
                        to_address=to_addr,
                        token=log.contract,
                        amount=value,
                        log_index=log.index
                    )
                    transfers[transfer.content_id] = transfer

            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None
    