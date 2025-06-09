# indexer/transform/transformers/tokens/token_base.py

from typing import List, Optional, Dict, Tuple

from ..base import BaseTransformer
from ....types import (
    ProcessingError,    
    DecodedLog,
    Signal,
    TransferSignal,
    ErrorId,
    EvmAddress,
)
from ....utils.amounts import amount_to_str, is_zero

class TokenTransformer(BaseTransformer):   
    def __init__(self, contract: str):
        super().__init__(contract_address=contract)
        self.handler_map = {
            "Transfer": self._handle_transfer,
        }

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        from_addr = str(log.attributes.get("from", ""))
        to_addr = str(log.attributes.get("to", ""))
        value = amount_to_str(log.attributes.get("value", 0))
        sender = str(log.attributes.get("sender", ""))
        return from_addr, to_addr, value, sender

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(trf, log.index, errors):
            return False
        if is_zero(trf[2]):
            self.log_warning("Transfer amount is zero",log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        return True

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling transfer log", log_index=log.index)

        trf = self._get_transfer_attributes(log)
        if not self._validate_transfer_data(log, trf, errors):
            return
        
        signals[log.index] = TransferSignal(
            log_index=log.index,
            token=self.contract_address,
            from_address=EvmAddress(trf[0].lower()),
            to_address=EvmAddress(trf[1].lower()),
            amount=trf[2],
            sender=EvmAddress(trf[3].lower()) if trf[3] else None
        )
        self.log_debug("Transfer signal created", log_index=log.index)

