from typing import List, Dict, Tuple, Optional

from ....utils.logger import get_logger
from ....decode.model.block import DecodedLog, Transaction
from ..base import BaseTransformer
from ...events.transfer import Transfer,UnmatchedTransfer
from ...events.base import DomainEvent, ProcessingError


class WavaxTransformer(BaseTransformer):
    def __init__(self, contract):
        self.logger = get_logger(__name__)
        self.contract = contract

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Dict[str,Transfer],Optional[List[ProcessingError]]]:
        transfers = {}

        for log in logs:
            if log.name == "Transfer":
                transfer = UnmatchedTransfer(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    from_address=log.attributes.get("src").lower(),
                    to_address=log.attributes.get("dst").lower(),
                    token=log.contract,
                    amount=log.attributes.get("wad"),
                    log_index=log.index
                )
                key = transfer.generate_content_id()
                
                transfers[key] = transfer
                
        return transfers, None