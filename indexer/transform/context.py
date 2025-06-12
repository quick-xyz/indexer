# indexer/transform/context.py

from typing import Dict, List, Set, Optional, Type, TypeVar, Union
from collections import defaultdict
import msgspec

from ..types import (
    Transaction,
    DecodedLog,
    Signal,
    TransferSignal,
    DomainEvent,
    EvmAddress,
    TokenConfig,
    AddressConfig,
    DomainEventId,
    ErrorId,
    ProcessingError,
)

SignalDict = Dict[int, Signal]
EventDict = Dict[DomainEventId, DomainEvent]
TransferList = Dict[int, TransferSignal]
AddressTransfers = Dict[EvmAddress, TransferList]
DirectionDict = Dict[str, AddressTransfers]
TransfersDict = Dict[EvmAddress, DirectionDict]


class TransformContext:
    def __init__(self, transaction: Transaction, tokens_of_interest: Set[EvmAddress]):
        self._og_tx = transaction
        self.tokens_of_interest = tokens_of_interest 

        self.signals: Dict[int, Signal] = {}
        self.events: Dict[DomainEventId, DomainEvent] = {}
        self.errors: Dict[ErrorId, ProcessingError] = {}

        self._transfer_signals: Dict[int, TransferSignal] = {}
        self.matched_transfers: Set[int] = set()
        self._event_signals: Dict[int, Signal] = {}
        self.consumed_signals: Set[int] = set()

        self._trf_dict = None

    # Read-only access to original transaction
    @property 
    def transaction(self) -> Transaction:
        return self._og_tx

    @property
    def trf_dict(self) -> TransfersDict:
        if self._trf_dict is None:
            self._build_trf_dict()
        return self._trf_dict

    def add_signals(self, signals: Dict[int, Signal]):
        self.signals.update(signals)
        self._transfer_signals = None
    
    def add_events(self, events: Dict[DomainEventId, DomainEvent]):
        self.events.update(events)
    
    def add_errors(self, errors: Dict[ErrorId, ProcessingError]):
        self.errors.update(errors)

    def group_logs_by_contract(self, decoded_logs: Dict[int, DecodedLog]) -> Dict[EvmAddress, List[DecodedLog]]:
        logs_by_contract = defaultdict(list)
        for log_index, log in decoded_logs.items():
            contract = log.contract
            logs_by_contract[contract].append(log)
        return dict(logs_by_contract)
    
    def finalize_to_transaction(self) -> Transaction:
        return Transaction(
            block=self._og_tx.block,
            timestamp=self._og_tx.timestamp,
            tx_hash=self._og_tx.tx_hash,
            index=self._og_tx.index,
            origin_from=self._og_tx.origin_from,
            origin_to=self._og_tx.origin_to,
            function=self._og_tx.function,
            value=self._og_tx.value,
            tx_success=self._og_tx.tx_success,
            logs=self._og_tx.logs,
            signals=self.signals if self.signals else None,
            events=self.events if self.events else None,
            errors=self.errors if self.errors else None
        )

    def _init_signals(self) -> None:
        if not self.signals:
            return
        self._build_transfer_signals()
        self._build_event_signals()
        self._build_trf_dict

    def _build_transfer_signals(self) -> Dict[int, TransferSignal]:
        if self._transfer_signals is None:
            self._transfer_signals = {idx: signal for idx, signal in self.signals.items() if isinstance(signal, TransferSignal)}

    def _build_event_signals(self) -> Dict[int, Signal]:
        if self._event_signals is None:
            self._event_signals = {idx: signal for idx, signal in self.signals.items() if not isinstance(signal, TransferSignal)}

    def _build_trf_dict(self) -> None:
        trf_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        for idx, transfer in self._transfer_signals.items():
            trf_dict[transfer.token]["out"][transfer.from_address][idx] = transfer
            trf_dict[transfer.token]["in"][transfer.to_address][idx] = transfer
        self._trf_dict = trf_dict
    
    def _filter_trf_dict(self, matched: set) -> TransfersDict:
        filtered_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        
        for token, directions in self.trf_dict.items():
            for direction, addresses in directions.items():
                for address, transfers in addresses.items():
                    for idx, transfer in transfers.items():
                        if idx not in matched:
                            filtered_dict[token][direction][address][idx] = transfer
        
        return filtered_dict
    
    # =============================================================================
    # HELPER METHODS
    # =============================================================================

    def get_signals_by_type(self, signal_types: Union[type, List[type]]) -> Dict[int, Signal]:
        if isinstance(signal_types, type):
            return {idx: s for idx, s in self.all_signals.items() if isinstance(s, signal_types)}
        else:
            signal_tuple = tuple(signal_types)
            return {idx: s for idx, s in self.all_signals.items() if isinstance(s, signal_tuple)}
    
    def mark_signal_consumed(self, log_index: int) -> None:
        self.consumed_signals.add(log_index)

    def is_signal_consumed(self, log_index: int) -> bool:
        return log_index in self.consumed_signals

    def match_transfer(self, log_index: int) -> None:
        self.matched_transfers.add(log_index)

    def get_unmatched_transfers(self) -> Dict[int, TransferSignal]:
        return {idx: t for idx, t in self._transfer_signals.items() 
                if idx not in self.matched_transfers}

    def get_remaining_signals(self) -> Dict[int, Signal]:
        return {idx: signal for idx, signal in self._event_signals.items()
                if idx not in self.consumed_signals}
    
    def get_unmatched_trf_dict(self) -> TransfersDict:
        return self._filter_trf_dict(self.matched_transfers)

    def match_all_signals(self, signals: Dict[int, Signal]) -> None:
        for idx, signal in signals.items():
            if isinstance(signal, TransferSignal):
                self.match_transfer(idx)
            else:
                self.mark_signal_consumed(idx)

    def get_address_deltas_for_token(self, token: EvmAddress) -> Dict[EvmAddress, int]:
        deltas = defaultdict(int)
        
        for transfer in self.transfer_signals.values():
            if transfer.token != token:
                continue
                
            amount = int(transfer.amount)
            deltas[transfer.from_address] -= amount
            deltas[transfer.to_address] += amount
            
        return dict(deltas)

