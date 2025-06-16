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
        from_addr = log.attributes.get("from", "")
        to_addr = log.attributes.get("to", "")
        value = log.attributes.get("value", 0)  # Keep as int initially
        sender = log.attributes.get("sender", "")
        
        # Convert addresses to strings if they're not already
        from_addr = str(from_addr) if from_addr is not None else ""
        to_addr = str(to_addr) if to_addr is not None else ""
        sender = str(sender) if sender is not None else ""
        
        return from_addr, to_addr, value, sender 

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        # Check for None/null values explicitly
        if trf[0] is None or trf[1] is None or trf[2] is None:
            self.log_warning("Transfer has null attributes", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        # Check for empty strings (but allow integers)
        if (isinstance(trf[0], str) and trf[0] == "") or \
        (isinstance(trf[1], str) and trf[1] == "") or \
        (isinstance(trf[2], str) and trf[2] == ""):
            self.log_warning("Transfer has empty string attributes", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        # Check if amount is zero (handle both int and string)
        amount_value = trf[2]
        if isinstance(amount_value, (int, float)) and amount_value == 0:
            self.log_warning("Transfer amount is zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        elif isinstance(amount_value, str) and (amount_value == "0" or amount_value == ""):
            self.log_warning("Transfer amount is zero or empty", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        return True

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling transfer log", log_index=log.index)

        trf = self._get_transfer_attributes(log)
        if not self._validate_transfer_data(log, trf, errors):
            print("FAILED VALIDATION")
            self.log_warning("Transfer validation failed", log_index=log.index)
            return
        
        # Convert amount to string only at signal creation
        signals[log.index] = TransferSignal(
            log_index=log.index,
            pattern="Transfer",
            token=self.contract_address,
            from_address=EvmAddress(trf[0].lower()),
            to_address=EvmAddress(trf[1].lower()),
            amount=str(trf[2]),  # Convert to string here
            sender=EvmAddress(trf[3].lower()) if trf[3] else None
        )
        self.log_debug("Transfer signal created", log_index=log.index)

