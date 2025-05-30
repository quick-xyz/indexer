# indexer/transform/manager.py

from typing import Tuple, Dict

from .registry import TransformerRegistry
from ..types import (
    Transaction, 
    DecodedLog, 
)


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
        trf_dict, event_dict, error_list = {}, {}, []
        # TODO: handle reprocessing / existing transfers and events

        # PHASE 1: TRANSFERS
        transfers_by_contract = self.registry.get_transfers_ordered(decoded_logs)

        for contract_address, transfer_logs in transfers_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            
            if transformer:
                logs_only = [log for _, log in transfer_logs.items()]
                result, errors = transformer.process_transfers(logs_only, updated_tx)
                trf_dict[contract_address] = result or {}
                if errors:
                    error_list.extend(errors)
            # TODO: handle transfers with no transformer  

        updated_tx.transfers = trf_dict

        # PHASE 2: EVENTS
        logs_by_priority_contract = self.registry.get_remaining_logs_ordered(decoded_logs)

        for priority in sorted(logs_by_priority_contract.keys()):
            contracts_at_priority = logs_by_priority_contract[priority]

            for contract_address, remaining_logs in contracts_at_priority.items():
                transformer = self.registry.get_transformer(contract_address)
                if transformer:
                    transfers, events, errors = transformer.process_logs(remaining_logs, event_dict, updated_tx)
                    event_dict = events
                    updated_tx.transfers.update(transfers)
                    if errors:
                        error_list.extend(errors)

        updated_tx.events = event_dict
        # TODO: handle unmatched transfers and errors
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