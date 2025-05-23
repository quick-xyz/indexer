from typing import List, Dict, Any, Union

from ..decode.model.block import Block,Transaction, DecodedLog
from .events.base import TransactionContext, DomainEvent


class TransformationManager:
    def process_block(self, block: Block) -> Block:       
        for tx_hash, transaction in block.transactions.items():    
            transformed_tx = self.process_transaction(transaction)
            block[tx_hash] = transformed_tx
        #TODO: add logic to flag if errors occurred
        return block
    
    def process_transaction(self, transaction: Transaction, context: TransactionContext) -> Transaction:       
        if not self.has_decoded_logs(transaction):
            return transaction
        
        if not transaction.tx_success:
            return transaction

        context = TransactionContext(
            timestamp=transaction.timestamp,
            tx_hash=transaction.tx_hash,
            sender=transaction.origin_from,
            contract=transaction.origin_to,
            function=transaction.function,
            value=transaction.value,
        )

        decoded_logs = self.get_decoded_logs(transaction)
        
        ops_logs, transfers_temp, events_temp, events, errors = [], [], [], [], []

        for log_id, log in decoded_logs.items(): 
            transformer = registry.get_log_transformer(log.contract)
            if not transformer:
                continue

            rules = registry.get_triggered_rules(log.signature)
            applicable_rules = [
                rule for rule in rules
                if rule.contract_address is None or rule.contract_address.lower() == log.contract.lower()
            ]
            if not applicable_rules:
                continue
 
            transformer = transformer()
            # Process the log
            for rule in applicable_rules:
                # Get the transformer for this log
                result = transformer.transform(rule, log, context)
                if isinstance(result, list):
                    ops_logs.extend(result)
                elif isinstance(result, DomainEvent):
                    events_temp.append(result)
                elif isinstance(result, Transfer):
                    transfers_temp.append(result)
                else:
                    errors.append(f"Unknown result type: {type(result)}")

        # Store the results in the transaction
        transaction.ops_logs = ops_logs
        transaction.transfers_temp = transfers_temp
        transaction.events_temp = events_temp
        transaction.events = events
        transaction.errors = errors

        
        return transformed_tx



    def get_decoded_logs(transaction: Transaction) -> dict[str, DecodedLog]:
        """Get all decoded logs from transaction."""
        decoded_logs = {}
        for key, log in transaction.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[key] = log
        return decoded_logs

    def has_decoded_logs(transaction: Transaction) -> bool:
        """Check if transaction has any decoded logs."""
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
