# transformer/transformation_manager.py

from typing import List, Any
from .registry import registry


class TransformationManager:
    """Log-priority based transformation manager."""

    
    def process_transaction(self, transaction, block) -> List[Any]:
        """Process transaction by processing logs in priority order."""
        decoded_logs = self.get_decoded_logs(transaction)
        if not decoded_logs:
            return []
        
        # Sort logs by contract + event priority
        logs_by_priority = registry.get_logs_by_priority(decoded_logs)
        
        all_domain_events = []
        
        # Process each log in priority order (may revisit contracts)
        for log_key, log in logs_by_priority:
            transformer_class = registry.get_transformer_class(log.contract)
            if not transformer_class:
                continue
            
            # Create transformer and process this specific log
            transformer = transformer_class()
            domain_events = transformer.process_log(log, transaction, block)
            
            if domain_events:
                all_domain_events.extend(domain_events)
        
        return all_domain_events
    