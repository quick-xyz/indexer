# indexer/transform/context.py

from typing import Dict, List, Set, Optional, Type, TypeVar
from collections import defaultdict
import msgspec

from ..core.config import IndexerConfig
from ..types import (
    Transaction,
    DecodedLog,
    Signal,
    TransferSignal,
    DomainEvent,
    EvmAddress,
    TokenConfig,
    AddressConfig,
)


class TransformerContext:
    def __init__(self, transaction: Transaction, config: IndexerConfig):
        self.transaction = transaction
        self.config = config

        self.all_signals: Dict[int, Signal] = {}
        self.consumed_signals: Set[int] = set()
        self.transfer_signals: Dict[int, TransferSignal] = {}
        self.matched_transfers: Set[int] = set()

    def add_signals(self, signals: Dict[int, Signal]) -> None:
        for log_index, signal in signals.items():
            self.all_signals[log_index]= signal

            match signal:
                case TransferSignal() if signal.token in self.config.tokens:
                    self.transfer_signals[log_index]= signal

    def get_signals_by_type(self, signal_type: type) -> Dict[int, Signal]:
        return {idx: s for idx, s in self.all_signals.items() if isinstance(s, signal_type)}

    def mark_signal_consumed(self, log_index: int) -> None:
        self.consumed_signals.add(log_index)

    def is_signal_consumed(self, log_index: int) -> bool:
        return log_index in self.consumed_signals

    def match_transfer(self, transfer_signal: TransferSignal) -> None:
        self.matched_transfers.add(transfer_signal.log_index)

    def get_unmatched_transfers(self) -> Dict[int, TransferSignal]:
        return {idx: t for idx, t in self.transfer_signals.items() 
                if idx not in self.matched_transfers}

    def group_logs_by_contract(self, decoded_logs: Dict[int, DecodedLog]) -> Dict[EvmAddress, List[DecodedLog]]:
        logs_by_contract = defaultdict(list)
        for log_index, log in decoded_logs.items():
            contract = log.contract
            logs_by_contract[contract].append(log)
        return dict(logs_by_contract)
    
    def get_address_token_deltas(self, token: EvmAddress) -> Dict[EvmAddress, int]:
        deltas = defaultdict(int)
        
        for transfer in self.transfer_signals.values():
            if transfer.token != token:
                continue
                
            amount = int(transfer.amount)
            deltas[transfer.from_address] -= amount
            deltas[transfer.to_address] += amount
            
        return dict(deltas)

    def get_address_signals(self, address: EvmAddress) -> Dict[int, Signal]:
        address_signals = {}
        
        for idx, signal in self.all_signals.items():
            if self._signal_involves_address(signal, address):
                address_signals[idx] = signal
                    
        return address_signals

    def _is_valid_address(self, value) -> bool:
        return isinstance(value, str) and len(value) == 42 and value.startswith('0x')

    def _signal_involves_address(self, signal: Signal, address: str) -> bool:
        target_address = address.lower()
        signal_dict = msgspec.structs.asdict(signal)
        
        for value in signal_dict.values():
            if isinstance(value, list):
                for item in value:
                    if self._is_valid_address(item) and item.lower() == target_address:
                        return True
            else:
                if self._is_valid_address(value) and value.lower() == target_address:
                    return True
        
        return False

    '''
    def reconcile_event_transfers(self, event: DomainEvent) -> None:
        if hasattr(event, 'signals'):
            for signal in event.signals.values():
                if isinstance(signal, TransferSignal):
                    self.match_transfer(signal)
    '''

    def get_reconciliation_summary(self) -> Dict[str, int]:
        total_transfers = len(self.transfer_signals)
        matched_transfers = len(self.matched_transfers)
        
        return {
            "total_transfer_signals": total_transfers,
            "matched_transfers": matched_transfers,
            "unmatched_transfers": total_transfers - matched_transfers,
            "total_signals": sum(len(signals) for signals in self.signals_by_type.values()),
            "consumed_signals": len(self.consumed_signals)
        }