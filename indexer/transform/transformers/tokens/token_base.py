# indexer/transform/transformers/tokens/token_base.py

from typing import List, Optional, Dict, Tuple

from ..base import BaseTransformer
from ....types import (
    ProcessingError,    
    DecodedLog,
    Transaction,   
    Transfer,
    DomainEventId,
    ErrorId,
)

class TokenTransformer(BaseTransformer):   
    def __init__(self, contract: str):
        super().__init__(contract_address=contract)

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        transfers = {}
        errors = {}

        for log in logs:
            try:
                if log.name == "Transfer":
                    transfer = self._build_transfer_from_log(log, tx)
                    if transfer:
                        transfers[transfer.content_id] = transfer
                        
            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None