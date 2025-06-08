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

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        from_addr = str(log.attributes.get("from", ""))
        to_addr = str(log.attributes.get("to", ""))
        value = amount_to_str(log.attributes.get("value", 0))
        sender = str(log.attributes.get("sender", ""))
        return from_addr, to_addr, value, sender

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple, errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(trf, log.index, errors):
            return False
        if is_zero(trf[2]):
            self.log_warning("Transfer amount is zero",log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        return True
    
    def _create_transfer_signal(self, log: DecodedLog, from_addr: str, to_addr: str, value: str, sender: str) -> TransferSignal:
        return TransferSignal(
            log_index=log.index,
            token=log.contract,
            from_address=EvmAddress(from_addr.lower()),
            to_address=EvmAddress(to_addr.lower()),
            amount=value,
            sender=EvmAddress(sender.lower()) if sender else None  # Keep this one - sender can be optional
        )

    def process_logs(self, logs: List[DecodedLog]) -> Tuple[
        Optional[Dict[int, Signal]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        signals = {}
        errors = {}

        for log in logs:
            try:
                if log.name == "Transfer":
                    trf = self._get_transfer_attributes(log)
                    if not self._validate_transfer_data(log,trf, errors):
                        continue
                    signals[log.index] = self._create_transfer_signal(log, *trf)

            except Exception as e:
                self._create_log_exception(e, log.index, errors)

        return signals if signals else None, errors if errors else None