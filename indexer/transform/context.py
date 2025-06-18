# indexer/transform/context.py

from typing import Dict, List, Set, Optional, Type, Union, Tuple
from collections import defaultdict

from ..types import (
    Transaction,
    DecodedLog,
    Signal,
    TransferSignal,
    DomainEvent,
    EvmAddress,
    DomainEventId,
    ErrorId,
    ProcessingError,
    PoolSwap,
    SwapSignal,
    SwapBatchSignal,
    Position,
)

SignalDict = Dict[int, Signal]
EventDict = Dict[DomainEventId, DomainEvent]
TransferDict = Dict[int, TransferSignal]
AddrTrf = Dict[EvmAddress, TransferDict]  # Transfers grouped by address
TokenTrf = Dict[EvmAddress, TransferDict] # Transfers grouped by token
TrfDict = Dict[EvmAddress, TokenTrf] # {address: {token: {log_index: TransferSignal}}}




class TransformContext:
    def __init__(self, transaction: Transaction, indexer_tokens: Set[EvmAddress]):
        self._og_tx = transaction
        self.indexer_tokens = indexer_tokens 

        self.signals: Dict[int, Signal] = {}
        self.events: Dict[DomainEventId, DomainEvent] = {}
        self.errors: Dict[ErrorId, ProcessingError] = {}
        self.positions: Dict[DomainEventId, Position] = {}

        # Initialize as None instead of empty dicts
        self._transfer_signals: Optional[Dict[int, TransferSignal]] = None
        self.matched_transfers: Set[int] = set()
        self._event_signals: Optional[Dict[int, Signal]] = None
        self.consumed_signals: Set[int] = set()

        self._trf_dict: Dict[str,TrfDict] = None
        self._trf_summary: Dict[EvmAddress, Dict[EvmAddress, Dict[str, int]]] = None


    # Read-only access to original transaction
    @property 
    def transaction(self) -> Transaction:
        return self._og_tx

    @property
    def trf_dict(self) -> Dict[str,TrfDict]:
        if self._trf_dict is None:
            self._build_trf_dict()
        return self._trf_dict

    def add_signals(self, signals: Dict[int, Signal]):
        self.signals.update(signals)
        # Reset cached computations when signals are added
        self._transfer_signals = None
        self._event_signals = None
        self._trf_dict = None
    
    def add_events(self, events: Dict[DomainEventId, DomainEvent]):
        self.events.update(events)
    
    def remove_events(self, event_ids: List[DomainEventId]):
        for event_id in event_ids:
            if event_id in self.events:
                del self.events[event_id]

    def add_positions(self, positions: Dict[DomainEventId, Position]):
        self.positions.update(positions)

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
        self._build_trf_dict()

    def _build_transfer_signals(self) -> None:
        self._transfer_signals = {idx: signal for idx, signal in self.signals.items() if isinstance(signal, TransferSignal)}

    def _build_event_signals(self) -> None:
        self._event_signals = {idx: signal for idx, signal in self.signals.items() 
                              if not isinstance(signal, TransferSignal)}

    def _build_trf_dict(self) -> None:
        if self._transfer_signals is None: 
            self._build_transfer_signals()
            
        trf_out = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        trf_in = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
        for idx, transfer in self._transfer_signals.items():
            trf_out[transfer.from_address][transfer.token][idx] = transfer
            trf_in[transfer.to_address][transfer.token][idx] = transfer
            

        self._trf_dict = {
            "trf_out": trf_out,
            "trf_in": trf_in,
        }
    
    def _get_trf_in(self) -> TrfDict:
        return self._trf_dict["trf_in"]

    def _get_trf_out(self) -> TrfDict:
        return self._trf_dict["trf_out"]
    

    # =============================================================================
    # HELPER METHODS
    # =============================================================================

    def get_signals_by_type(self, signal_types: Union[type, List[type]]) -> Dict[int, Signal]:
        if isinstance(signal_types, type):
            return {idx: s for idx, s in self.signals.items() if isinstance(s, signal_types)}
        else:
            signal_tuple = tuple(signal_types)
            return {idx: s for idx, s in self.signals.items() if isinstance(s, signal_tuple)}
    
    def get_events_by_type(self, event_types: Union[Type[DomainEvent], List[Type[DomainEvent]]]) -> Dict[DomainEventId, DomainEvent]:
        if isinstance(event_types, type):
            return {eid: e for eid, e in self.events.items() if isinstance(e, event_types)}
        else:
            event_tuple = tuple(event_types)
            return {eid: e for eid, e in self.events.items() if isinstance(e, event_tuple)}
    
    def mark_signal_consumed(self, log_index: int) -> None:
        self.consumed_signals.add(log_index)

    def mark_signals_consumed(self, log_indices: List[int]) -> None:
        for log_index in log_indices:
            self.mark_signal_consumed(log_index)

    def is_signal_consumed(self, log_index: int) -> bool:
        return log_index in self.consumed_signals

    def match_transfer(self, log_index: int) -> None:
        self.matched_transfers.add(log_index)

    def get_unmatched_transfers(self) -> Dict[int, TransferSignal]:
        if self._transfer_signals is None:
            self._build_transfer_signals()
        
        unmatched_transfers = {idx: t for idx, t in self._transfer_signals.items() 
                if idx not in self.matched_transfers}
        return unmatched_transfers if unmatched_transfers else {}

    def get_remaining_signals(self) -> Dict[int, Signal]:
        # Always rebuild to ensure we have the latest signals
        self._build_event_signals()
        
        # Show what signals we have by type
        signal_types = {}
        for idx, signal in self.signals.items():
            signal_type = type(signal).__name__
            signal_types[signal_type] = signal_types.get(signal_type, 0) + 1
        
        # Show event signals before filtering
        if self._event_signals:
            event_signal_types = {}
            for idx, signal in self._event_signals.items():
                signal_type = type(signal).__name__
                event_signal_types[signal_type] = event_signal_types.get(signal_type, 0) + 1
        
        result = {idx: signal for idx, signal in self._event_signals.items()
                  if idx not in self.consumed_signals}
        
        return result
    

    def match_all_signals(self, signals: Dict[int, Signal]) -> None:
        for idx, signal in signals.items():
            if isinstance(signal, TransferSignal):
                self.match_transfer(idx)
            else:
                self.mark_signal_consumed(idx)

    def group_swap_events(self, events: List[DomainEventId]) -> None:
        for idx in events:
            if isinstance(self.events[idx], PoolSwap):
                self.events[idx].grouped = True

    def get_batch_swap_signals(self) -> Dict[int, SwapBatchSignal]:
        return self.get_signals_by_type([SwapBatchSignal])

    def get_swap_signals(self) -> Dict[int, SwapSignal]:
        return self.get_signals_by_type([SwapSignal])
    
    def get_swap_events(self) -> Tuple[Dict[DomainEventId, PoolSwap],Dict[DomainEventId, PoolSwap]]:
        all_swaps = self.get_events_by_type([PoolSwap])
        buy_swaps = {eid: swap for eid, swap in all_swaps.items() if swap.direction == "buy"}
        sell_swaps = {eid: swap for eid, swap in all_swaps.items() if swap.direction == "sell"}
        
        return buy_swaps, sell_swaps
    
    def get_contract_transfers(self, contract: EvmAddress) -> Tuple[TokenTrf, TokenTrf]:
        if self._trf_dict is None:
            self._build_trf_dict()
        return self._trf_dict.get("trf_in", {}).get(contract, {}), self._trf_dict.get("trf_out", {}).get(contract, {})
    
    def get_token_transfers(self, token: EvmAddress) -> Tuple[AddrTrf, AddrTrf]:
        if self._trf_dict is None:
            self._build_trf_dict()
        in_trf = {contract: trf for contract, trf in self._get_trf_in().items() if token in trf}
        out_trf = {contract: trf for contract, trf in self._get_trf_out().items() if token in trf}
        return in_trf, out_trf

    def get_unmatched_contract_transfers(self, contract: EvmAddress) -> Tuple[TokenTrf, TokenTrf]:
        in_trf, out_trf = self.get_contract_transfers(contract)
        unmatched_in = {idx: trf for idx, trf in in_trf.items() if idx not in self.matched_transfers}
        unmatched_out = {idx: trf for idx, trf in out_trf.items() if idx not in self.matched_transfers}
        return unmatched_in, unmatched_out
    
    def get_unmatched_token_transfers(self, token: EvmAddress) -> Tuple[AddrTrf, AddrTrf]:
        in_trf, out_trf = self.get_token_transfers(token)
        in_trf = {contract: {idx: trf for idx, trf in trf.items() if idx not in self.matched_transfers} for contract, trf in in_trf.items()}
        out_trf = {contract: {idx: trf for idx, trf in trf.items() if idx not in self.matched_transfers} for contract, trf in out_trf.items()}
        return in_trf, out_trf