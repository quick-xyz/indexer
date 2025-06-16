# indexer/transform/manager.py

from typing import Tuple, Dict, List, Optional
from msgspec import Struct
from collections import defaultdict

from ..core.config import IndexerConfig
from .registry import TransformRegistry
from .context import TransformContext
from ..types import (
    Transaction, 
    DecodedLog,
    DomainEventId,
    ProcessingError,
    Signal,
    TransferSignal,
    RouteSignal,
    SwapBatchSignal,
    Trade,
    SwapSignal,
    MultiRouteSignal,
    UnknownTransfer,
    Position,
    ZERO_ADDRESS,
    create_transform_error
)
from ..core.mixins import LoggingMixin
from ..utils.amounts import amount_to_negative_str, amount_to_int, amount_to_str

TRADE_PATTERNS = ["Swap_A", "Route"]

class TransformManager(LoggingMixin):   
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config

    def _create_context(self, transaction: Transaction) -> TransformContext:
        return TransformContext(
            transaction=transaction,
            tokens_of_interest=self.config.get_indexer_tokens(),
        )

    def process_transaction(self, tx: Transaction) -> Tuple[bool, Transaction]:
        if not self._has_decoded_logs(tx) or not tx.tx_success:
            return False, tx

        context = self._create_context(tx)

        try:
            self._produce_signals(context)
            self._produce_events(context)
            self._reconcile_transfers(context)
            self._produce_net_positions(context)

            updated_tx = context.finalize_to_transaction()
            return True, updated_tx
        
        except Exception as e:
            self.log_error("Transaction processing failed", error=str(e))
            return False, tx 

    def _produce_signals(self, context: TransformContext) -> None:
        decoded_logs = self._get_decoded_logs(context.transaction)
        if not decoded_logs:
            return

        logs_by_contract = context.group_logs_by_contract(decoded_logs)

        for contract_address, log_list in logs_by_contract.items():
            transformer = self.registry.get_transformer(contract_address)
            if not transformer:
                continue
            
            try:
                signals, errors = transformer.process_logs(log_list)
                if signals:
                    context.add_signals(signals)
                if errors:
                    context.add_errors(errors)
                
            except Exception as e:
                error = self._create_transformer_error(e, context.transaction.tx_hash, contract_address)
                context.add_errors({error.error_id: error})

    def _produce_events(self, context: TransformContext) -> None:
        event_signals = context.get_remaining_signals()
        signal_len = len(event_signals)
        if not event_signals:
            return

        reprocess_queue = self._process_signals(event_signals, context)

        if reprocess_queue and signal_len > 1:
            self.log_debug("Reprocessing signals", reprocess_count=len(reprocess_queue))
            self._process_signals(reprocess_queue, context)


    def _process_signals(self, event_signals: Dict[int, Signal], context: TransformContext) -> Optional[Dict[int, Signal]]:
        reprocess_queue = {}
        for log_index, signal in event_signals.items():
            if log_index not in context.consumed_signals:
                pattern = self.registry.get_pattern(signal.pattern)
                if not pattern:
                    continue
                
                if pattern in TRADE_PATTERNS:
                    self.log_debug("Processing trade signals")
                    try:
                        trade_signals = {k: v for k, v in context.get_remaining_signals() if v.pattern in TRADE_PATTERNS}
                        if not self._process_trade(trade_signals, context):
                            reprocess_queue.update(trade_signals)
                    except Exception as e:
                        self.log_error("Signal processing failed", error=str(e))
                else:
                    try:
                        if not pattern.process_signal(signal, context):
                            reprocess_queue[log_index] = signal
                    except Exception as e:
                        self.log_error("Signal processing failed", error=str(e))
        
        return reprocess_queue

    def _process_trade(self, trade_signals: Dict[int,Signal], context: TransformContext) -> bool:
        if not trade_signals:
            return True  

        ''' Get user intent from Router Signals '''      
        tokens_in, tokens_out, tos, senders = [], [], [], []
        for log_index, signal in trade_signals.items():
            if isinstance(signal, [RouteSignal, MultiRouteSignal]):
                self.log_debug("Processing Route Pattern", log_index=log_index)
                tokens_in.extend(signal.tokens_in if isinstance(signal, MultiRouteSignal) else [signal.token_in])
                tokens_out.extend(signal.tokens_out if isinstance(signal, MultiRouteSignal) else [signal.token_out])
                tos.append(signal.to)
                senders.append(signal.sender)

        ''' Aggregate SwapBatchSignals into SwapSignals '''   
        batch_signals = context.get_batch_swap_signals()
        batch_dict, batch_components, signal_components = {}, {}, {}

        for log_index, signal in batch_signals.items():
            key = "_".join((str(signal.pool), str(signal.to)))  
            transformer = self.registry.get_transformer(signal.pool)

            if key not in batch_dict:
                batch_dict[key] = {
                    "index": 0,
                    "pool": signal.pool,
                    "to": signal.to,
                    "base_amount": 0,
                    "quote_amount": 0,
                    "base_token": transformer.base_token,
                    "quote_token": transformer.quote_token,
                    "batch": {},
                    "sender": signal.to if signal.to else None
                }
            
            batch_dict[key]["index"] += amount_to_int(signal.log_index)
            batch_dict[key]["base_amount"] += amount_to_int(signal.base_amount)
            batch_dict[key]["quote_amount"] += amount_to_int(signal.quote_amount)
            batch_components[key][str(signal.id)] = (signal.base_amount, signal.quote_amount)
            signal_components[key] = signal_components.update({log_index: signal})

        for key, data in batch_dict.items():
            swap_signal = SwapSignal(
                log_index=data["index"]*100,
                pattern="Swap_A",
                pool=data["pool"],
                base_amount=amount_to_str(data["base_amount"]),
                base_token=data["base_token"],
                quote_amount=amount_to_str(data["quote_amount"]),
                quote_token=data["quote_token"],
                to=data["to"],
                sender=data["sender"] if data["sender"] else None,
                batch=batch_components[key],
                signals= signal_components.get(key, {})
            )
            context.add_signals({swap_signal.log_index: swap_signal})

        ''' Process SwapSignals into PoolSwap Events '''  
        swap_signals = context.get_swap_signals([SwapSignal])
        for log_index, signal in swap_signals.items():
            pattern = self.registry.get_pattern(signal.pattern)
            if not pattern:
                continue
            try:
                if not pattern.process_signal(signal, context):
                    return False
            except Exception as e:
                self.log_error("Signal processing failed", error=str(e))
                return False
        
        # TODO: Validate Swaps against Router Signals. Generate UnknownTransfer Events if required.         ''' 
        
        ''' Aggregate PoolSwap Events into Trade Events '''
        buy_swaps, sell_swaps = context.get_swap_events()
        swaps = buy_swaps + sell_swaps
        if not swaps:
            self.log_warning("No Swaps found during Trade Processing",)
            return False

        trade_dict, trade_swaps = {}, {}
        for idx, swap in buy_swaps.items():
            if swap.taker not in trade_dict:
                key = "_".join((str(swap.taker), str(swap.base_token))) 
                trade_dict["buy"][key] = {
                    "index": 0,
                    "taker": swap.taker,
                    "direction": "buy",
                    "base_amount": 0,
                    "base_token": swap.base_token,
                    "swaps": {},
                }

            trade_dict["buy"][key]["index"] += amount_to_int(signal.log_index)
            trade_dict["buy"][key]["base_amount"] += amount_to_int(signal.base_amount)
            trade_swaps["buy"][key][str(idx)] = swap

        for idx, swap in sell_swaps.items():
            if swap.taker not in trade_dict:
                key = "_".join((str(swap.taker), str(swap.base_token))) 
                trade_dict["sell"][key] = {
                    "index": 0,
                    "taker": swap.taker,
                    "direction": "sell",
                    "base_amount": 0,
                    "base_token": swap.base_token,
                    "swaps": {},
                }

            trade_dict["sell"][key]["index"] += amount_to_int(signal.log_index)
            trade_dict["sell"][key]["base_amount"] += amount_to_int(signal.base_amount)
            trade_swaps["sell"][key][str(idx)] = swap


        for dir, keyed_data in trade_dict.items():
            for key, data in keyed_data.items():
                trade_event = Trade(
                    timestamp= context.transaction.timestamp,
                    tx_hash= context.transaction.tx_hash,
                    taker=data["taker"],
                    direction=data["direction"],
                    base_token=data["base_token"],
                    base_amount=amount_to_str(data["base_amount"]),
                    trade_type="trade",
                    swaps=trade_swaps[dir][key],
                )
                context.add_events({trade_event.content_id: trade_event})   
                context.group_swap_events(trade_event.swaps.keys())       
        
        '''
        if not (swap.base_token in context.tokens_of_interest and swap.quote_token in context.tokens_of_interest):
            self.log_warning("Swap tokens not in tokens of interest", 
                             base_token=swap.base_token, quote_token=swap.quote_token)
            return False
        if not (swap.base_amount and swap.quote_amount):
            self.log_warning("Swap amounts are zero", 
                             base_amount=swap.base_amount, quote_amount=swap.quote_amount)
            return False
        if not (swap.to and swap.sender):
            self.log_warning("Swap addresses are invalid", 
                             to=swap.to, sender=swap.sender)
            return False

        swap.positions = positions
        swap.signals = {signal.log_index: signal for signal in trade_signals.values()}
        context.add_events({swap._content_id: swap})
        self.log_debug("Trade signals processed successfully", swap=swap)
        if not (unmatched_transfers := context.get_unmatched_transfers()):
            self.log_debug("No unmatched transfers found after processing trade signals")
            return True
        self.log_debug("Unmatched transfers found after processing trade signals", count=len(unmatched_transfers))
        # Handle unmatched transfers
        for idx, trf in unmatched_transfers.items():
            if trf.token not in context.tokens_of_interest:
                continue
            
            positions = self._generate_positions(trf)
            self._produce_unknown_transfer(trf, context, positions)
        self.log_debug("Trade signals processed successfully, unmatched transfers handled") 
        '''

        ''' BUILD TRADE EVENTS '''



        return True

    def _reconcile_transfers(self, context: TransformContext) -> None:
        if not (unmatched_transfers := context.get_unmatched_transfers()):
            return True
        
        for idx, trf in unmatched_transfers:
            if trf.token not in context.tokens_of_interest:
                continue
            
            positions = self._generate_positions(trf)
            self._produce_unknown_transfer(trf, context, positions)
        
    def _produce_net_positions(self, context: TransformContext) -> None:
        deltas = {}
        for event in context.events.values():
            for id, position in event.positions.items():
                if position.token in context.tokens_of_interest:
                    deltas[position.user][position.token]["net_amount"] += int(position.amount)
                    deltas[position.user][position.token]["positions"].update({id: position})

        for user, token in deltas.items():
            if token["net_amount"] != 0:
                context.add_events(token["positions"])

    def _get_decoded_logs(self, transaction: Transaction) -> Optional[Dict[int, DecodedLog]]:
        if self._has_decoded_logs(transaction):
            return {index: log for index, log in transaction.logs.items() 
                   if isinstance(log, DecodedLog)}
        return None

    def _has_decoded_logs(self, transaction: Transaction) -> bool:
        return any(isinstance(log, DecodedLog) for log in transaction.logs.values())
    
    def _create_transformer_error(self, e: Exception, tx_hash: str, contract_address: str) -> ProcessingError:
        return create_transform_error(
            error_type="transformer_processing_exception",
            message=f"Exception in transformer processing: {str(e)}",
            tx_hash=tx_hash,
            contract_address=contract_address,
            transformer_name="TransformManager"
        )

    def _generate_positions(self, transfer: TransferSignal) -> Dict[DomainEventId, Position]:
        positions = {}

        position_in = Position(
            user=transfer.to_address,
            token=transfer.token,
            amount=transfer.amount,
        ) if transfer.to_address != ZERO_ADDRESS else None
        positions[position_in.content_id] = position_in

        position_out = Position(
            user=transfer.from_address,
            token=transfer.token,
            amount=amount_to_negative_str(transfer.amount),
        ) if transfer.from_address != ZERO_ADDRESS else None
        positions[position_out.content_id] = position_out

        return positions
    
    def _produce_unknown_transfer(transfer: TransferSignal, context: TransformContext, positions: Dict[DomainEventId, Position]) -> None:
        events = {}
        unknown_transfer = UnknownTransfer(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            token=transfer.token,
            from_address=transfer.from_address,
            to_address=transfer.to_address,
            amount=transfer.amount,
            positions=positions,
            signals={transfer.log_index: transfer}
        )
        events[unknown_transfer.content_id] = unknown_transfer
        context.add_events(events)
