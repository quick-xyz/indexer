from typing import List, Any, Dict
from collections import defaultdict
from .registry import transformer_registry

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
        
        # Check if transaction has decoded logs and if it is successful
        if not self.has_decoded_logs(transaction):
            return transaction
        
        if not transaction.tx_success:
            return transaction

        # Build context to pass to transformers (TODO: deprecate?)
        context = self._create_context(transaction)

        # Assemble all decoded logs
        decoded_logs = self.get_decoded_logs(transaction)
        
        transfers_temp, logs_temp = {}, {}

        contracts_in_tx = set()
        for key, log in decoded_logs.items():
            contracts_in_tx.add(log.contract.lower())
            if transformer_registry.is_transfer_event(log.name):
                transfers_temp[key] = log
            else:
                logs_temp[key] = log

        ops_logs, events, errors = [], [], []
        domain_events = []

        for log_id, log in logs_temp.items(): 
            mapping = registry.get_contract_mapping(log.contract)
            if not mapping:
                continue
            
            applicable_rules = [
                rule for rule in mapping.business_event_rules 
                if log.name in rule.source_events
            ]
            
            if not applicable_rules:
                continue
            
            transformer = mapping.transformer_class()
            
            
            
            
            mapping = registry.get_contract_mapping(log.contract)
            if not mapping:
                continue
            
            # Get applicable rules for this business event
            applicable_rules = [
                rule for rule in mapping.business_event_rules 
                if log.name in rule.source_events
            ]
            
            if not applicable_rules:
                continue
            
            # Create transformer instance
            transformer = mapping.transformer_class()
            
            # Process each rule with transfer correlation
            for rule in applicable_rules:
                if rule.requires_transfers:
                    # Find related transfers for this business event
                    related_transfers = self._find_related_transfers(
                        log, transfer_logs, rule.transfer_validation
                    )
                    
                    # Create enriched domain event with transfers
                    result = transformer.transform_with_transfers(
                        log, related_transfers, self._create_context(transaction, block)
                    )
                else:
                    # Simple transformation without transfers
                    result = transformer.transform_log(
                        log, self._create_context(transaction, block)
                    )
                
                domain_events.extend(result)
        
        return domain_events


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
    
    def _validate_transfer_amounts(self, business_log, related_transfers):
        """Validate that transfer amounts balance correctly."""
        pass
    
    def _create_context(self, transaction: Transaction) -> TransactionContext:
        return TransactionContext(
            timestamp=transaction.timestamp,
            tx_hash=transaction.tx_hash,
            sender=transaction.origin_from,
            contract=transaction.origin_to,
            function=transaction.function,
            value=transaction.value,
        )