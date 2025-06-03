# indexer/transform/transformers/tokens/wavax.py

from typing import List, Optional, Dict, Tuple

from ..base import BaseTransformer
from ....types import (
    ProcessingError,    
    DecodedLog,
    Transaction,   
    Transfer,
    DomainEventId,
    ErrorId,
    EvmAddress,
    UnmatchedTransfer
)

class WavaxTransformer(BaseTransformer):   
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
                    from_addr = EvmAddress(str(log.attributes.get("src", "")).lower())
                    to_addr = EvmAddress(str(log.attributes.get("dst", "")).lower())
                    value = int(log.attributes.get("wad", 0))
                    
                    if not from_addr or not to_addr or value <= 0:
                        return None
                        
                    transfer = UnmatchedTransfer(
                        timestamp=tx.timestamp,
                        tx_hash=tx.tx_hash,
                        log_index=log.index,
                        token=log.contract.lower(),
                        amount=value,
                        from_address=from_addr,
                        to_address=to_addr
                    )
            
                    transfers[transfer.content_id] = transfer
                        
            except Exception as e:
                self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)
                
        return transfers if transfers else None, errors if errors else None