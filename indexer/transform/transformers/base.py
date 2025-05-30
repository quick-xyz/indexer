from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict, Tuple

from ...types import (
    DomainEvent,
    ProcessingError,    
    DecodedLog,
    Transaction,    
    EvmAddress,    
    Transfer,
    UnmatchedTransfer,
)


class BaseTransformer(ABC):
    def __init__(self):
        self.contract_address: Optional[EvmAddress] = None
        self.name = self.__class__.__name__
    
    @abstractmethod
    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Dict[str,Transfer],Optional[List[ProcessingError]]]:
        ''' Returns (transfers, errors) '''
        pass

    @abstractmethod
    def process_logs(self, logs: List[DecodedLog], events: Dict[str,DomainEvent], tx: Transaction) -> Tuple[Dict[str,Transfer],Dict[str,DomainEvent],Optional[List[ProcessingError]]]:
        ''' Returns (transfers, events, errors) '''
        pass

    def _get_unmatched_transfers(self, tx: Transaction) -> Dict[str,Dict[str, Transfer]]:
        unmatched_transfers = {}
    
        for key, transfer in tx.transfers.items():
            if isinstance(transfer, UnmatchedTransfer):
                unmatched_transfers[transfer.token][key] = transfer

        return unmatched_transfers


    def get_related_transfers(self, transaction, token_address: Optional[EvmAddress] = None) -> List[Any]:
        decoded_logs = self.get_decoded_logs(transaction)
        transfers = []
        
        for key, log in decoded_logs.items():
            if log.name in ["Transfer", "TransferBatch", "TransferSingle"]:
                if token_address is None or log.contract.lower() == token_address.lower():
                    transfers.append(log)
        
        return transfers
    
    def get_decoded_logs(transaction: Transaction) -> dict[int, DecodedLog]:
        decoded_logs = {}
        for index, log in transaction.logs.items():
            if isinstance(log, DecodedLog):
                decoded_logs[index] = log
        return decoded_logs

    def has_decoded_logs(transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())

    def get_related_events(self, transaction, event_names: List[str], contract_address: Optional[EvmAddress] = None) -> List[Any]:
        """Find specific events in the transaction, optionally filtered by contract."""
        decoded_logs = self.get_decoded_logs(transaction)
        events = []
        
        for key, log in decoded_logs.items():
            if log.name in event_names:
                if contract_address is None or log.contract.lower() == contract_address.lower():
                    events.append(log)
        
        return events
    
    def get_events_involving_contract(self, transaction, contract_address: EvmAddress) -> List[Any]:
        """Get all events that involve a specific contract (as source or in data)."""
        decoded_logs = self.get_decoded_logs(transaction)
        events = []
        
        for key, log in decoded_logs.items():
            # Event from this contract
            if log.contract.lower() == contract_address.lower():
                events.append(log)
                continue
            
            # Event that references this contract in its data
            if hasattr(log, 'data') and log.data:
                for value in log.data.values():
                    if isinstance(value, str) and value.lower() == contract_address.lower():
                        events.append(log)
                        break
        
        return events
    
    def validate_amounts(self, expected_total: int, actual_amounts: List[int], tolerance: float = 0.001) -> bool:
        """Validate that amounts sum correctly within tolerance."""
        actual_total = sum(actual_amounts)
        if expected_total == 0:
            return actual_total == 0
        
        difference = abs(expected_total - actual_total)
        relative_error = difference / abs(expected_total)
        return relative_error <= tolerance


class TokenTransformer(BaseTransformer):
    def __init__(self, contract):
        self.contract = contract

    def process_transfers(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Dict[str,Transfer],Optional[List[ProcessingError]]]:
        transfers = {}

        for log in logs:
            if log.name == "Transfer":
                transfer = UnmatchedTransfer(
                    timestamp=tx.timestamp,
                    tx_hash=tx.tx_hash,
                    from_address=log.attributes.get("from").lower(),
                    to_address=log.attributes.get("to").lower(),
                    token=log.contract,
                    amount=log.attributes.get("value"),
                    log_index=log.index
                )
                key = transfer.generate_content_id()
                
                transfers[key] = transfer
                
        return transfers, None