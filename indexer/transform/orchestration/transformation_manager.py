from typing import List, Dict, Any, Union
from ..context.transaction_context import TransactionContext, DecodedEvent

from ...decode.model.block import Block,Transaction

class TransformationManager:
    def process_block(self, block: Block) -> Block:       
        for tx in block.transactions:
            transformed_tx = self.process_transaction(tx)

            if domain_events:  # Only store non-empty results
                block_results[tx_hash] = domain_events
        
        return block_results
    
    def process_transaction(self, transaction: Transaction) -> Transaction:
        if not decoded_events:
            return []
        
        # Create context
        context = TransactionContext(transaction_hash, block_number, timestamp)
        
        # Add events to context
        for event in decoded_events:
            context.add_decoded_event(event)
        
        # Transform events
        domain_events = []
        
        for event in decoded_events:
            # Get transformer for this contract
            transformer_class = registry.get_contract_transformer(event.contract_address)
            if not transformer_class:
                continue
            
            # Get applicable transformation rules for this event
            rules = registry.get_triggered_rules(event.event_name)
            
            # Filter rules that apply to this contract
            applicable_rules = [
                rule for rule in rules
                if rule.contract_address is None or rule.contract_address.lower() == event.contract_address.lower()
            ]
            
            if not applicable_rules:
                continue
            
            # Create transformer instance once per contract
            transformer = transformer_class()
            
            # Process each applicable rule
            for rule in applicable_rules:
                events_for_rule = [event]  # Simple: one event per rule for now
                result = transformer.transform(rule, events_for_rule, context)
                domain_events.extend(result)
        
        return domain_events
    
    def process_events_batch(self, events_batch: List[DecodedEvent]) -> List[Any]:
        # Group events by transaction
        events_by_tx = {}
        for event in events_batch:
            tx_hash = event.transaction_hash
            if tx_hash not in events_by_tx:
                events_by_tx[tx_hash] = []
            events_by_tx[tx_hash].append(event)
        
        # Process each transaction
        all_domain_events = []
        for tx_hash, tx_events in events_by_tx.items():
            # Use block info from first event
            block_number = tx_events[0].block_number
            timestamp = tx_events[0].timestamp
            
            domain_events = self.process_transaction(tx_hash, block_number, tx_events, timestamp)
            all_domain_events.extend(domain_events)
        
        return all_domain_events