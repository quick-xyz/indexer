from typing import List, Any
from .registry import registry

from ..decode.model.block import Block,Transaction, DecodedLog
from .events.base import TransactionContext, DomainEvent


class TransformationManager:

    def process_transaction(self, transaction: Transaction) -> tuple[bool,Transaction]:       
        if not self.has_decoded_logs(transaction) or not transaction.tx_success:
            return False, transaction

        decoded_logs = self.get_decoded_logs(transaction)

        # PHASE 1: Transfers Only
        transfers = registry.get_transfers_by_contract(decoded_logs)
        transfer_events = {}

        for key, log in transfers.items():
            transformer = registry.get_transformer(log.contract)
            if transformer:
                transfer_events = transformer.process_transfers(log, transaction)
                if transfer_events:
                    transaction.add_events(transfer_events)






    def get_decoded_logs(transaction: Transaction) -> dict[int, DecodedLog]:
        decoded_logs = {}
        for index, log in transaction.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def has_decoded_logs(transaction: Transaction) -> bool:
        """Check if transaction has any decoded logs."""
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())

    def _validate_transfer_amounts(self, business_log, related_transfers):
        """Validate that transfer amounts balance correctly."""
        pass
    