from typing import List, Any
from .registry import registry

from ..decode.model.block import Block,Transaction, DecodedLog
from .events.base import TransactionContext, DomainEvent

'''
Method: groups events by contract address and event priority. event lists are passed to transformers
'''
#TODO: add error handling and logging

class TransformationManager:

    def process_transaction(self, transaction: Transaction) -> tuple[bool,Transaction]:       
        if not self.has_decoded_logs(transaction) or not transaction.tx_success:
            return False, transaction

        updated_tx = transaction.copy(deep=True)

        decoded_logs = self.get_decoded_logs(transaction)
        trf_dict, event_dict, error_list = {}, {}, []
        # TODO: handle reprocessing / existing transfers and events

        # PHASE 1: TRANSFERS
        transfers_by_contract = registry.get_transfers_ordered(decoded_logs)

        for contract_address, transfer_logs in transfers_by_contract.items():
            transformer = registry.get_transformer(contract_address)
                        
            if transformer:     
                logs_only = [log for _, log in transfer_logs]
                result, errors = transformer.process_transfers(logs_only, updated_tx)
                trf_dict[contract_address] = {} if not result else result
                if errors:
                    error_list.extend(errors)
            # TODO: handle transfers with no transformer        
        
        updated_tx.transfers = trf_dict

        # PHASE 2: EVENTS
        logs_by_priority_contract = registry.get_remaining_logs_ordered(decoded_logs)

        for priority in sorted(logs_by_priority_contract.keys()):
            contracts_at_priority = logs_by_priority_contract[priority]

            for contract_address, remaining_logs in contracts_at_priority.items():
                transformer = registry.get_transformer(contract_address)
                if transformer:
                    logs_only = [log for _, log in remaining_logs]
                    transfers, events, errors = transformer.process_logs(logs_only, event_dict, updated_tx)
                    event_dict = events
                    updated_tx.transfers.update(transfers)
                    if errors:
                        error_list.extend(errors)

        updated_tx.events = event_dict
        # TODO: handle unmatched transfers and errors
  
        return True, updated_tx

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
    