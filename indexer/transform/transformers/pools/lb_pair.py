from typing import List, Dict, Tuple, Optional, Any
import msgspec

from ..base import BaseTransformer
from ....types import (
    ZERO_ADDRESS,
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    ProcessingError,
    Transfer,
    UnmatchedTransfer,
    MatchedTransfer,
    Liquidity,
    Position,
    Fee,
    PoolSwap,
    DomainEventId,
    ErrorId,
    create_transform_error,
    EvmHash,
    TransferLedger,
    TransferIds,
)
from ....utils.lb_byte32_decoder import decode_amounts


class LbPairTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress, token_x: EvmAddress, token_y: EvmAddress, base_token: EvmAddress):
        super().__init__(contract_address=contract.lower())
        self.token_x = token_x.lower()
        self.token_y = token_y.lower()
        self.base_token = base_token.lower()
        self.quote_token = self.token_y if self.token_x == self.base_token else self.token_x

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
    
    def _create_tx_exception(self, e, tx_hash: EvmHash, transformer_name: str, error_dict: Dict[ErrorId,ProcessingError]) -> None:
        """ Create a ProcessingError for exceptions """
        error = create_transform_error(
            error_type="processing_exception",
            message=f"Transaction processing exception: {str(e)}",
            tx_hash=tx_hash,
            transformer_name=transformer_name
        )
        error_dict[error.error_id] = error
        return None
    
    def _unpack_amounts(self, bytes: bytes) -> tuple[Optional[int], Optional[int]]:
        try:
            amounts_x, amounts_y = decode_amounts(bytes)

            if self.token_x == self.base_token:
                return amounts_x, amounts_y
            else:
                return amounts_y, amounts_x
        except Exception:
            return None, None
        
    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId,Transfer]],Optional[Dict[ErrorId,ProcessingError]]]:
        transfers = {}
        errors = {}

        for log in logs:
            try:
                if log.name == "TransferBatch":
                    from_addr = log.attributes.get("from")
                    to_addr = log.attributes.get("to")
                    amounts = log.attributes.get("amounts")
                    bins = log.attributes.get("ids")
                    
                    if not len(amounts) == len(bins):
                        error = create_transform_error(
                            error_type= "invalid_lb_transfer",
                            message = f"LB Transfer Batch: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                            tx_hash = tx.tx_hash,
                            log_index = log.index
                        )
                        errors[error.id] = error
                        continue
                    if not self._validate_attr([from_addr, to_addr, amounts, bins], tx.tx_hash, log.index, errors):
                        continue

                    transferids = []
                    sum_transfers = 0
                    
                    for i in bins:    
                        trf = TransferIds(
                            id=i,
                            amount=amounts[i]
                        )
                        sum_transfers += amounts[i]
                        transferids.append(trf)  

                    transfer = UnmatchedTransfer(
                        timestamp=tx.timestamp,
                        tx_hash=tx.tx_hash,
                        from_address=from_addr.lower(),
                        to_address=to_addr.lower(),
                        token=log.contract,
                        amount=value,
                        log_index=log.index
                    )
                    transfers[transfer.content_id] = transfer

            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None