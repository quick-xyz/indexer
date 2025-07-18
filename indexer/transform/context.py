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
from ..core.logging import LoggingMixin

TrfDict = Dict[EvmAddress, Dict[EvmAddress, Dict[int, TransferSignal]]] # {address: {token: {log_index: TransferSignal}}}


class TransformContext(LoggingMixin):
    def __init__(self, transaction: Transaction, tracked_tokens: Set[EvmAddress]):
        if not transaction:
            raise ValueError("Transaction cannot be None")
        if not tracked_tokens:
            self.log_warning("No tracked tokens provided to context")

        self._og_tx = transaction
        self.tracked_tokens = tracked_tokens

        self.signals: Dict[int, Signal] = {}
        self.events: Dict[DomainEventId, DomainEvent] = {}
        self.errors: Dict[ErrorId, ProcessingError] = {}
        self.positions: Dict[DomainEventId, Position] = {}

        # Track consumed signals for non-transfer signals
        self.consumed_signals: Set[int] = set()

        # Transfer tracking - trf_dict contains only UNMATCHED transfers
        self._transfer_signals: Optional[Dict[int, TransferSignal]] = None
        self._trf_dict: Optional[Dict[str, TrfDict]] = None
        self._trf_dict_built = False

        self.log_debug("Transform context initialized",
                      tx_hash=transaction.tx_hash,
                      tracked_tokens_count=len(tracked_tokens))

    # Read-only access to original transaction
    @property 
    def transaction(self) -> Transaction:
        return self._og_tx

    @property
    def trf_dict(self) -> Dict[str, TrfDict]:
        """Get the unmatched transfers dictionary"""
        if not self._trf_dict_built:
            self._build_trf_dict()
        return self._trf_dict

    def add_signals(self, signals: Dict[int, Signal]) -> None:
        """Add signals to context with validation"""
        if not signals:
            self.log_debug("No signals to add", tx_hash=self.transaction.tx_hash)
            return
        
        if not isinstance(signals, dict):
            raise TypeError("Signals must be a dictionary")
        
        # Validate signal indices
        for idx, signal in signals.items():
            if not isinstance(idx, int):
                raise TypeError(f"Signal index must be integer, got {type(idx)}")
            if not isinstance(signal, Signal):
                raise TypeError(f"Signal must be Signal instance, got {type(signal)}")
        
        self.signals.update(signals)
        
        # Reset cached computations when signals are added
        self._transfer_signals = None
        self._trf_dict = None
        self._trf_dict_built = False
        
        self.log_debug("Signals added to context",
                      tx_hash=self.transaction.tx_hash,
                      new_signals=len(signals),
                      total_signals=len(self.signals))
    
    def add_events(self, events: Dict[DomainEventId, DomainEvent]) -> None:
        """Add events to context with validation"""
        if not events:
            self.log_debug("No events to add", tx_hash=self.transaction.tx_hash)
            return
        
        if not isinstance(events, dict):
            raise TypeError("Events must be a dictionary")
        
        self.events.update(events)
        
        self.log_debug("Events added to context",
                      tx_hash=self.transaction.tx_hash,
                      new_events=len(events),
                      total_events=len(self.events))
    
    def remove_events(self, event_ids: List[DomainEventId]) -> None:
        """Remove events from context"""
        if not event_ids:
            return
        
        removed_count = 0
        for event_id in event_ids:
            if event_id in self.events:
                del self.events[event_id]
                removed_count += 1
        
        self.log_debug("Events removed from context",
                      tx_hash=self.transaction.tx_hash,
                      removed_count=removed_count,
                      remaining_events=len(self.events))

    def add_positions(self, positions: Dict[DomainEventId, Position]) -> None:
        """Add positions to context with validation"""
        if not positions:
            self.log_debug("No positions to add", tx_hash=self.transaction.tx_hash)
            return
        
        if not isinstance(positions, dict):
            raise TypeError("Positions must be a dictionary")
        
        self.positions.update(positions)
        
        self.log_debug("Positions added to context",
                      tx_hash=self.transaction.tx_hash,
                      new_positions=len(positions),
                      total_positions=len(self.positions))

    def add_errors(self, errors: Dict[ErrorId, ProcessingError]) -> None:
        """Add errors to context with validation"""
        if not errors:
            return
        
        if not isinstance(errors, dict):
            raise TypeError("Errors must be a dictionary")
        
        self.errors.update(errors)
        
        self.log_error("Errors added to context",
                      tx_hash=self.transaction.tx_hash,
                      new_errors=len(errors),
                      total_errors=len(self.errors))

    def group_logs_by_contract(self, decoded_logs: Dict[int, DecodedLog]) -> Dict[EvmAddress, List[DecodedLog]]:
        """Group decoded logs by contract address"""
        if not decoded_logs:
            self.log_debug("No decoded logs to group", tx_hash=self.transaction.tx_hash)
            return {}
        
        try:
            logs_by_contract = defaultdict(list)
            for log_index, log in decoded_logs.items():
                if not hasattr(log, 'contract') or not log.contract:
                    self.log_warning("Log missing contract address",
                                    tx_hash=self.transaction.tx_hash,
                                    log_index=log_index)
                    continue
                
                contract = log.contract
                logs_by_contract[contract].append(log)
            
            result = dict(logs_by_contract)
            
            self.log_debug("Logs grouped by contract",
                          tx_hash=self.transaction.tx_hash,
                          total_logs=len(decoded_logs),
                          contracts=len(result))
            
            return result
            
        except Exception as e:
            self.log_error("Failed to group logs by contract",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise
    
    def finalize_to_transaction(self) -> Transaction:
        """Create final transaction with all accumulated data"""
        try:
            result = Transaction(
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
                positions=self.positions if self.positions else None,
                errors=self.errors if self.errors else None
            )
            
            self.log_debug("Transaction finalized",
                          tx_hash=self.transaction.tx_hash,
                          signals=len(self.signals),
                          events=len(self.events),
                          positions=len(self.positions),
                          errors=len(self.errors))
            
            return result
            
        except Exception as e:
            self.log_error("Failed to finalize transaction",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def _build_transfer_signals(self) -> None:
        """Build transfer signals dictionary"""
        try:
            self._transfer_signals = {
                idx: signal for idx, signal in self.signals.items() 
                if isinstance(signal, TransferSignal)
            }
            
            self.log_debug("Transfer signals built",
                          tx_hash=self.transaction.tx_hash,
                          transfer_count=len(self._transfer_signals))
            
        except Exception as e:
            self.log_error("Failed to build transfer signals",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def _build_trf_dict(self) -> None:
        """Build transfer dictionary containing only UNMATCHED transfers"""
        if self._transfer_signals is None:
            self._build_transfer_signals()
        
        try:
            trf_out = defaultdict(lambda: defaultdict(dict))
            trf_in = defaultdict(lambda: defaultdict(dict))
            
            for idx, transfer in self._transfer_signals.items():
                if not transfer.from_address or not transfer.to_address:
                    self.log_warning("Transfer missing address information",
                                    tx_hash=self.transaction.tx_hash,
                                    transfer_index=idx)
                    continue
                
                trf_out[transfer.from_address][transfer.token][idx] = transfer
                trf_in[transfer.to_address][transfer.token][idx] = transfer

            self._trf_dict = {
                "trf_out": dict(trf_out),
                "trf_in": dict(trf_in),
            }
            self._trf_dict_built = True
            
            self.log_debug("Transfer dictionary built (unmatched only)",
                          tx_hash=self.transaction.tx_hash,
                          transfers_processed=len(self._transfer_signals))
            
        except Exception as e:
            self.log_error("Failed to build transfer dictionary",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def _remove_transfer_from_dict(self, log_index: int) -> None:
        """Remove a matched transfer from the trf_dict"""
        if not self._trf_dict_built or not self._transfer_signals:
            return
        
        transfer = self._transfer_signals.get(log_index)
        if not transfer:
            return
        
        try:
            # Remove from trf_out
            if (transfer.from_address in self._trf_dict["trf_out"] and 
                transfer.token in self._trf_dict["trf_out"][transfer.from_address] and
                log_index in self._trf_dict["trf_out"][transfer.from_address][transfer.token]):
                
                del self._trf_dict["trf_out"][transfer.from_address][transfer.token][log_index]
                
                # Clean up empty dictionaries
                if not self._trf_dict["trf_out"][transfer.from_address][transfer.token]:
                    del self._trf_dict["trf_out"][transfer.from_address][transfer.token]
                    if not self._trf_dict["trf_out"][transfer.from_address]:
                        del self._trf_dict["trf_out"][transfer.from_address]
            
            # Remove from trf_in
            if (transfer.to_address in self._trf_dict["trf_in"] and 
                transfer.token in self._trf_dict["trf_in"][transfer.to_address] and
                log_index in self._trf_dict["trf_in"][transfer.to_address][transfer.token]):
                
                del self._trf_dict["trf_in"][transfer.to_address][transfer.token][log_index]
                
                # Clean up empty dictionaries
                if not self._trf_dict["trf_in"][transfer.to_address][transfer.token]:
                    del self._trf_dict["trf_in"][transfer.to_address][transfer.token]
                    if not self._trf_dict["trf_in"][transfer.to_address]:
                        del self._trf_dict["trf_in"][transfer.to_address]
            
            self.log_debug("Transfer removed from trf_dict",
                          tx_hash=self.transaction.tx_hash,
                          log_index=log_index)
                          
        except Exception as e:
            self.log_error("Failed to remove transfer from trf_dict",
                          tx_hash=self.transaction.tx_hash,
                          log_index=log_index,
                          error=str(e),
                          exception_type=type(e).__name__)

    def rebuild_complete_trf_dict(self, include_matched: bool = True) -> Dict[str, TrfDict]:
        """Rebuild complete transfer dictionary with option to include matched transfers"""
        if self._transfer_signals is None:
            self._build_transfer_signals()
        
        try:
            trf_out = defaultdict(lambda: defaultdict(dict))
            trf_in = defaultdict(lambda: defaultdict(dict))
            
            for idx, transfer in self._transfer_signals.items():
                # Skip matched transfers if include_matched is False
                if not include_matched and self._is_transfer_matched(idx):
                    continue
                
                if not transfer.from_address or not transfer.to_address:
                    continue
                
                trf_out[transfer.from_address][transfer.token][idx] = transfer
                trf_in[transfer.to_address][transfer.token][idx] = transfer

            result = {
                "trf_out": dict(trf_out),
                "trf_in": dict(trf_in),
            }
            
            self.log_debug("Complete transfer dictionary rebuilt",
                          tx_hash=self.transaction.tx_hash,
                          include_matched=include_matched,
                          transfers_included=len([t for idx, t in self._transfer_signals.items() 
                                                if include_matched or not self._is_transfer_matched(idx)]))
            
            return result
            
        except Exception as e:
            self.log_error("Failed to rebuild complete transfer dictionary",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {"trf_out": {}, "trf_in": {}}

    def _is_transfer_matched(self, log_index: int) -> bool:
        """Check if a transfer signal has been matched by checking if it's in current trf_dict"""
        if not self._trf_dict_built or not self._transfer_signals:
            return False
        
        transfer = self._transfer_signals.get(log_index)
        if not transfer:
            return True  # If we can't find it, consider it matched
        
        # Check if it exists in the unmatched trf_dict
        return not (
            transfer.from_address in self._trf_dict["trf_out"] and
            transfer.token in self._trf_dict["trf_out"][transfer.from_address] and
            log_index in self._trf_dict["trf_out"][transfer.from_address][transfer.token]
        )

    # =============================================================================
    # HELPER METHODS
    # =============================================================================

    def get_signals_by_type(self, signal_types: Union[type, List[type]]) -> Dict[int, Signal]:
        """Get signals filtered by type"""
        if not self.signals:
            return {}
        
        try:
            if isinstance(signal_types, type):
                result = {idx: s for idx, s in self.signals.items() if isinstance(s, signal_types)}
            else:
                signal_tuple = tuple(signal_types)
                result = {idx: s for idx, s in self.signals.items() if isinstance(s, signal_tuple)}
            
            self.log_debug("Signals filtered by type",
                          tx_hash=self.transaction.tx_hash,
                          requested_types=str(signal_types),
                          matching_signals=len(result))
            
            return result
            
        except Exception as e:
            self.log_error("Failed to filter signals by type",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}
    
    def get_events_by_type(self, event_types: Union[Type[DomainEvent], List[Type[DomainEvent]]]) -> Dict[DomainEventId, DomainEvent]:
        """Get events filtered by type"""
        if not self.events:
            return {}
        
        try:
            if isinstance(event_types, type):
                result = {eid: e for eid, e in self.events.items() if isinstance(e, event_types)}
            else:
                event_tuple = tuple(event_types)
                result = {eid: e for eid, e in self.events.items() if isinstance(e, event_tuple)}
            
            self.log_debug("Events filtered by type",
                          tx_hash=self.transaction.tx_hash,
                          requested_types=str(event_types),
                          matching_events=len(result))
            
            return result
            
        except Exception as e:
            self.log_error("Failed to filter events by type",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}
    
    def mark_signal_consumed(self, log_index: int) -> None:
        """Mark a signal as consumed"""
        if not isinstance(log_index, int):
            raise TypeError("Log index must be an integer")
        
        self.consumed_signals.add(log_index)
        
        self.log_debug("Signal marked as consumed",
                      tx_hash=self.transaction.tx_hash,
                      log_index=log_index,
                      total_consumed=len(self.consumed_signals))

    def mark_signals_consumed(self, log_indices: List[int]) -> None:
        """Mark signals as consumed and automatically mark transfer signals as matched"""
        if not log_indices:
            return
        
        if not all(isinstance(idx, int) for idx in log_indices):
            raise TypeError("All log indices must be integers")
        
        transfer_count = 0
        signal_count = 0
        
        for log_index in log_indices:
            self.consumed_signals.add(log_index)
            
            # If this signal is a transfer, remove it from trf_dict (mark as matched)
            if (log_index in self.signals and 
                isinstance(self.signals[log_index], TransferSignal)):
                self._remove_transfer_from_dict(log_index)
                transfer_count += 1
            else:
                signal_count += 1
        
        self.log_debug("Signals marked as consumed, transfers marked as matched",
                      tx_hash=self.transaction.tx_hash,
                      total_processed=len(log_indices),
                      transfers_matched=transfer_count,
                      signals_consumed=signal_count,
                      total_consumed=len(self.consumed_signals))

    def is_signal_consumed(self, log_index: int) -> bool:
        """Check if a signal is already consumed"""
        return log_index in self.consumed_signals

    def get_unmatched_transfers(self) -> Dict[int, TransferSignal]:
        """Get all unmatched transfer signals"""
        if not self._trf_dict_built:
            self._build_trf_dict()
        
        try:
            # Collect all transfers from the unmatched trf_dict
            unmatched_transfers = {}
            
            for address_dict in self._trf_dict["trf_out"].values():
                for token_dict in address_dict.values():
                    unmatched_transfers.update(token_dict)
            
            self.log_debug("Retrieved unmatched transfers",
                          tx_hash=self.transaction.tx_hash,
                          total_transfers=len(self._transfer_signals) if self._transfer_signals else 0,
                          unmatched_transfers=len(unmatched_transfers))
            
            return unmatched_transfers
            
        except Exception as e:
            self.log_error("Failed to get unmatched transfers",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}

    def get_remaining_signals(self) -> Dict[int, Signal]:
        """Get signals that haven't been consumed (non-transfer signals only)"""
        try:
            result = {
                idx: signal for idx, signal in self.signals.items()
                if idx not in self.consumed_signals and not isinstance(signal, TransferSignal)
            }
            
            self.log_debug("Retrieved remaining signals",
                          tx_hash=self.transaction.tx_hash,
                          total_signals=len(self.signals),
                          consumed_signals=len(self.consumed_signals),
                          remaining_signals=len(result))
            
            return result
            
        except Exception as e:
            self.log_error("Failed to get remaining signals",
                          tx_hash=self.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}

    def match_all_signals(self, signals: Dict[int, Signal]) -> None:
        """Mark all provided signals as consumed/matched"""
        if not signals:
            return
        
        self.mark_signals_consumed(list(signals.keys()))

    def group_swap_events(self, events: List[DomainEventId]) -> None:
        """Mark swap events as grouped"""
        if not events:
            return
        
        grouped_count = 0
        for event_id in events:
            if event_id in self.events and isinstance(self.events[event_id], PoolSwap):
                self.events[event_id].grouped = True
                grouped_count += 1
        
        self.log_debug("Swap events grouped",
                      tx_hash=self.transaction.tx_hash,
                      grouped_count=grouped_count)

    def get_batch_swap_signals(self) -> Dict[int, SwapBatchSignal]:
        """Get batch swap signals"""
        return self.get_signals_by_type([SwapBatchSignal])

    def get_swap_signals(self) -> Dict[int, SwapSignal]:
        """Get swap signals"""
        return self.get_signals_by_type([SwapSignal])
    
    def get_swap_events(self) -> Tuple[Dict[DomainEventId, PoolSwap], Dict[DomainEventId, PoolSwap]]:
        """Get swap events categorized by direction"""
        all_swaps = self.get_events_by_type([PoolSwap])
        
        buy_swaps = {eid: swap for eid, swap in all_swaps.items() if swap.direction == "buy"}
        sell_swaps = {eid: swap for eid, swap in all_swaps.items() if swap.direction == "sell"}
        
        self.log_debug("Swap events categorized",
                      tx_hash=self.transaction.tx_hash,
                      total_swaps=len(all_swaps),
                      buy_swaps=len(buy_swaps),
                      sell_swaps=len(sell_swaps))
        
        return buy_swaps, sell_swaps
    
    def get_unmatched_contract_transfers(self, contract: EvmAddress) -> Tuple[Dict[EvmAddress, Dict[int, TransferSignal]], Dict[EvmAddress, Dict[int, TransferSignal]]]:
        """Get unmatched transfers for a specific contract - EFFICIENT VERSION"""
        if not contract:
            raise ValueError("Contract address cannot be empty")
        
        if not self._trf_dict_built:
            self._build_trf_dict()
        
        try:
            in_transfers = self._trf_dict["trf_in"].get(contract, {})
            out_transfers = self._trf_dict["trf_out"].get(contract, {})
            
            self.log_debug("Retrieved unmatched contract transfers",
                          tx_hash=self.transaction.tx_hash,
                          contract=contract,
                          in_tokens=len(in_transfers),
                          out_tokens=len(out_transfers))
            
            return in_transfers, out_transfers
            
        except Exception as e:
            self.log_error("Failed to get unmatched contract transfers",
                          tx_hash=self.transaction.tx_hash,
                          contract=contract,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}, {}
    
    def get_unmatched_token_transfers(self, token: EvmAddress) -> Tuple[Dict[EvmAddress, Dict[int, TransferSignal]], Dict[EvmAddress, Dict[int, TransferSignal]]]:
        """Get unmatched transfers of a specific token - EFFICIENT VERSION"""
        if not token:
            raise ValueError("Token address cannot be empty")
        
        if not self._trf_dict_built:
            self._build_trf_dict()
        
        try:
            in_trf = {
                address: trf[token] for address, trf in self._trf_dict["trf_in"].items() 
                if token in trf
            }
            out_trf = {
                address: trf[token] for address, trf in self._trf_dict["trf_out"].items() 
                if token in trf
            }
            
            self.log_debug("Retrieved unmatched token transfers",
                          tx_hash=self.transaction.tx_hash,
                          token=token,
                          in_addresses=len(in_trf),
                          out_addresses=len(out_trf))
            
            return in_trf, out_trf
            
        except Exception as e:
            self.log_error("Failed to get unmatched token transfers",
                          tx_hash=self.transaction.tx_hash,
                          token=token,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}, {}

    def get_contract_transfers(self, contract: EvmAddress) -> Tuple[Dict[EvmAddress, Dict[int, TransferSignal]], Dict[EvmAddress, Dict[int, TransferSignal]]]:
        """Get contract transfers (returns unmatched only for efficiency)"""
        return self.get_unmatched_contract_transfers(contract)
    
    def get_token_transfers(self, token: EvmAddress) -> Tuple[Dict[EvmAddress, Dict[int, TransferSignal]], Dict[EvmAddress, Dict[int, TransferSignal]]]:
        """Get token transfers (returns unmatched only for efficiency)"""
        return self.get_unmatched_token_transfers(token)