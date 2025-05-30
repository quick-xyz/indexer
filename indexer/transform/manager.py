# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional

from .registry import TransformerRegistry
from ..types import (
    Transaction, 
    DecodedLog,
    DomainEvent,
    Transfer,
    ProcessingError,
)

'''
TODO:
- handle transfers with no transformer 
- Handle unmatched transfers and errors
- Add logging for unmatched transfers and errors
'''
class TransformationManager:
    """Manages the transformation of decoded transactions into domain events"""
    
    def __init__(self, registry: TransformerRegistry):
        self.registry = registry

    def process_transaction(self, transaction: Transaction) -> Tuple[bool, Transaction]:
        """Process a transaction through the transformation pipeline"""
        if not self._has_decoded_logs(transaction) or not transaction.tx_success:
            return False, transaction

        updated_tx = transaction.copy(deep=True)
        decoded_logs = self._get_decoded_logs(transaction)

        unmatched_transfers,error_list = [], []
        if not updated_tx.transfers:
            updated_tx.transfers = []
        if not updated_tx.events:
            updated_tx.events = []

        # PHASE 1: TRANSFERS
        transfers_by_contract = self.registry.get_transfers_ordered(decoded_logs)

        for contract_address, transfer_logs in transfers_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            
            if transformer:
                for priority, log_list in sorted(transfer_logs.items()):
                    transfers, errors = transformer.process_transfers(log_list, updated_tx)
                    if transfers:
                        unmatched_transfers.extend(transfers)
                    if errors:
                        error_list.extend(errors)

        updated_tx.transfers = unmatched_transfers

        # PHASE 2: EVENTS
        logs_by_priority_contract = self.registry.get_remaining_logs_ordered(decoded_logs)

        for priority in sorted(logs_by_priority_contract.keys()):
            for contract_address, log_list in logs_by_priority_contract[priority].items():
                transformer = self.registry.get_transformer(contract_address)
                if transformer:
                    transfers, events, errors = transformer.process_logs(log_list,updated_tx)
                    if self._validate_transformer_results(transfers, events, errors, updated_tx):
                        updated_tx.transfers.extend(transfers)
                        updated_tx.events.extend(events)
                    
                    if errors:
                        error_list.extend(errors)

        return True, updated_tx

    def _get_decoded_logs(self, transaction: Transaction) -> Dict[int, DecodedLog]:
        """Extract decoded logs from transaction"""
        decoded_logs = {}
        for index, log in transaction.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        """Check if transaction has any decoded logs"""
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def _validate_transformer_results(
        self, 
        transfers: Optional[List[Transfer]], 
        events: Optional[List[DomainEvent]], 
        errors: Optional[List[ProcessingError]], 
        transaction: Transaction
    ) -> bool:
        pass
    )