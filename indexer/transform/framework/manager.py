"""
Transformation manager implementation.
"""
from typing import Dict, Any, List, Optional
from msgspec import Struct
from datetime import datetime
from ..events.base import BusinessEvent
from ..interfaces import TransformationManager, EventTransformer
from .context import TransactionContext
from ...utils.logger import get_logger

from ...decode.model.block import Block,Transaction, DecodedLog
from ...decode.model.types import EvmHash
from ..events.base import DomainEvent,TransactionContext


class TransformationManager:
    def __init__(self):
        """Initialize the transformation manager."""
        self.transformers = []
        self.logger = get_logger(__name__)













    def get_transformer(self, contract: str)


    def transform_single_logs(self, tx: Transaction,context: DomainEvent):
        for log in tx.logs.values():
            if isinstance(log, DecodedLog):
                transformer = self.get_transformer(log.contract)
                domain_event = transformer.transform_log(log,context)



    def transform_multi_logs(self, tx: Transaction):

    def transform_multi_events(self, tx: Transaction):

    def transform_transaction(self, tx: Transaction, time: datetime):   
        context = TransactionContext(
            timestamp=time,
            tx_hash=tx.tx_hash,
            sender=tx.origin_from,
            contract=tx.origin_to,
            function=tx.function,
            value=tx.value
        )

        self.transform_single_logs(tx,context)
        self.transform_multi_logs(tx,context)
        self.transform_multi_events(tx,context)





    def transform_block(self, block: Block):  
        for tx in block.transactions:
            self.transform_transaction(tx, block.timestamp)

    def process_tx_method(self, block: Block):
        self.transform_block(block)

