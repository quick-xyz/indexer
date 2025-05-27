from typing import List, Any
from .registry import registry

from ..decode.model.block import Block,Transaction, DecodedLog
from .events.base import TransactionContext, DomainEvent


class TransformationManager:
    def process_block(self, block: Block) -> Block:       
        for tx_hash, transaction in block.transactions.items():    
            transformed_tx = self.process_transaction(transaction)
            block[tx_hash] = transformed_tx
        #TODO: add logic to flag if errors occurred
        return block
    
    def process_transaction(self, transaction: Transaction) -> Transaction:       
        if not self.has_decoded_logs(transaction) or not transaction.tx_success:
            return transaction

        context = self._create_context(transaction)

        decoded_logs = self.get_decoded_logs(transaction)
        ordered_logs = registry.get_logs_by_priority(decoded_logs)

        for index, log in ordered_logs:
            transformer_class = registry.get_transformer_class(log.contract)
            if not transformer_class:
                continue
            
            transformer = transformer_class()
            domain_events = transformer.process_log(log, transaction, block)
            
            if domain_events:
                all_domain_events.extend(domain_events)
        
        return all_domain_events


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
    