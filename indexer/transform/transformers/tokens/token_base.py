# indexer/transform/transformers/tokens/token_base.py

from typing import List, Optional, Dict, Tuple

from ..base import BaseTransformer
from ....types import (
    ProcessingError,    
    DecodedLog,
    Transaction,   
    Transfer,
    UnmatchedTransfer,
    DomainEvent,
    DomainEventId,
    ErrorId,
    EvmAddress,
)

class TokenTransformer(BaseTransformer):   
    def __init__(self, contract: str):
        super().__init__(contract_address=contract)

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, int]:
        """
        Extract transfer attributes from log. 
        Override in subclasses for different attribute naming.
        
        Returns:
            Tuple of (from_address, to_address, amount)
        """
        from_addr = str(log.attributes.get("from", ""))
        to_addr = str(log.attributes.get("to", ""))
        value = int(log.attributes.get("value", 0))
        return from_addr, to_addr, value

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        transfers = {}
        errors = {}

        for log in logs:
            try:
                if log.name == "Transfer":
                    from_addr, to_addr, value = self._get_transfer_attributes(log)
                    
                    # Validate and normalize addresses
                    from_addr = EvmAddress(from_addr.lower()) if from_addr else ""
                    to_addr = EvmAddress(to_addr.lower()) if to_addr else ""
                    
                    if not from_addr or not to_addr or value <= 0:
                        continue
                        
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

    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[
        Optional[Dict[DomainEventId, Transfer]], 
        Optional[Dict[DomainEventId, DomainEvent]], 
        Optional[Dict[ErrorId, ProcessingError]]
    ]:
        """
        Token contracts typically don't produce domain events beyond transfers.
        This method returns empty results since transfers are handled in process_transfers().
        """
        return None, None, None