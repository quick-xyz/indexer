# indexer/transform/processors/trade_processor.py

from typing import Dict, Optional, Any, List
from collections import defaultdict
from dataclasses import dataclass

from ..registry import TransformRegistry
from ..context import TransformContext
from ...core.config import IndexerConfig
from ...core.mixins import LoggingMixin
from ...types import (
    EvmAddress,
    Signal,
    RouteSignal,
    MultiRouteSignal,
    SwapBatchSignal,
    SwapSignal,
    ProcessingError,
    create_transform_error,
    DomainEventId,
    PoolSwap,
    Trade,
    ZERO_ADDRESS,
)
from ...utils.amounts import amount_to_int, amount_to_str


@dataclass
class RouteContext:
    taker: EvmAddress
    routers: List[EvmAddress]
    top_level_router: EvmAddress


class TradeProcessor(LoggingMixin):
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config
        
        self.log_info("TradeProcessor initialized", 
                     indexer_tokens=len(config.get_indexer_tokens()))
    
    def process_trade_signals(self, trade_signals: Dict[int, Signal], context: TransformContext) -> bool:
        if not trade_signals:
            self.log_debug("No trade signals to process", tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Starting trade processing pipeline",
                      tx_hash=context.transaction.tx_hash,
                      signal_count=len(trade_signals))
        
        try:
            # Stage 1: Aggregate batch signals → SwapSignals
            aggregated_signals = self._aggregate_batch_signals(trade_signals, context)
            
            # Stage 2: Process SwapSignals → PoolSwap events
            pool_swaps = self._process_swap_signals(aggregated_signals, context)

            # Stage 3: Extract route context
            route = self._build_route_context(trade_signals, context)
            
            # Stage 4: Aggregate PoolSwaps → Trade events
            trade_events = self._produce_trade_events(pool_swaps, route, context)
            
            # Stage 5: Add events to context and mark signals consumed
            if trade_events or pool_swaps:
                self._finalize_trade_processing(trade_events, pool_swaps, trade_signals, context)
                
            self.log_info("Trade processing completed",
                         tx_hash=context.transaction.tx_hash,
                         pool_swaps_created=len(pool_swaps),
                         trade_events_created=len(trade_events))
            
            return True
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_processing")
            context.add_errors({error.error_id: error})
            self.log_error("Trade processing failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False
    
    def _aggregate_batch_signals(self, trade_signals: Dict[int, Signal], 
                                context: TransformContext) -> Dict[int, Signal]:
        batch_signals = {
            idx: signal for idx, signal in trade_signals.items()
            if isinstance(signal, SwapBatchSignal)
        }
        
        if not batch_signals:
            self.log_debug("No batch signals to aggregate", tx_hash=context.transaction.tx_hash)
            return trade_signals
        
        self.log_debug("Aggregating batch signals",
                    tx_hash=context.transaction.tx_hash,
                    batch_signal_count=len(batch_signals))
        
        batch_groups = {}
        for idx, signal in batch_signals.items():
            batch_groups[signal.pool][signal.to][idx]= signal
        
        aggregated = trade_signals.copy()
        
        for pool, to_dict in batch_groups.items():
            for to, batch_dict in to_dict.items():
                transformer = self.registry.get_transformer(pool)
                token_tuple = (transformer.base_token, transformer.quote_token)

                pattern = self.registry.get_pattern(batch_dict[0].pattern)

                batch_swap_signal = pattern.aggregate_batch_swaps(batch_dict, token_tuple, context)
                if batch_swap_signal:
                    aggregated[batch_swap_signal.log_index] = batch_swap_signal
                    aggregated-= batch_dict.keys()
        
        return aggregated

    def _process_swap_signals(self, signals: Dict[int, Signal], 
                                context: TransformContext) -> Dict[DomainEventId, PoolSwap]:
        
        swap_signals = {idx: signal for idx, signal in signals.items() if isinstance(signal, SwapSignal)}

        if not swap_signals:
            self.log_debug("No swap signals to process", tx_hash=context.transaction.tx_hash)
            return {}
        
        self.log_debug("Processing swap signals into pool swaps",
                    tx_hash=context.transaction.tx_hash,
                    swap_signal_count=len(swap_signals))
        
        pool_swaps = {}
        try:
            for log_index, signal in swap_signals.items():
                pattern = self.registry.get_pattern(signal.pattern)
                if not pattern:
                    self.log_warning("No pattern found for swap signal",
                                    tx_hash=context.transaction.tx_hash,
                                    log_index=log_index,
                                    pattern_name=signal.pattern)
                    continue
                
                self.log_debug("Processing swap signal with pattern",
                              tx_hash=context.transaction.tx_hash,
                              log_index=log_index,
                              pattern_name=signal.pattern,
                              pool=signal.pool,
                              taker=signal.to)
                
                result = pattern.produce_swap_events(signal, context)
                if not result:
                    self.log_warning("Swap signal pattern processing failed",
                                    tx_hash=context.transaction.tx_hash,
                                    log_index=log_index,
                                    pattern_name=signal.pattern)
                    continue
                pool_swaps.update(result)
            
            return pool_swaps
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "swap_signal_processing")
            context.add_errors({error.error_id: error})
            self.log_error("Swap signal processing failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e))
            return pool_swaps

    def _build_route_context(self, trade_signals: Dict[int, Signal], 
                              context: TransformContext) -> Optional[RouteContext]:        
        route_signals = [
            signal for signal in trade_signals.values() 
            if isinstance(signal, (RouteSignal, MultiRouteSignal))
        ]
        
        if not route_signals:
            self.log_debug("No route signals found", tx_hash=context.transaction.tx_hash)
            return None

        top_route = max(route_signals, key=lambda s: s.log_index)
        
        known_routers = set()
        for route_signal in route_signals:
            known_routers.add(route_signal.contract)
        
        return RouteContext(
            taker=top_route.to or top_route.sender,
            routers=sorted(list(known_routers)),
            router0=top_route.contract,
            route=top_route
        )
    
    def _produce_trade_events(self, pool_swaps: Dict[DomainEventId, PoolSwap],
                              route: Optional[RouteContext],
                              context: TransformContext) -> Dict[DomainEventId, Trade]:
        if not pool_swaps:
            self.log_debug("No pool swaps to produce trade events", tx_hash=context.transaction.tx_hash)
            return {}
        
        self.log_debug("Producing trade events from pool swaps",
                      tx_hash=context.transaction.tx_hash,
                      pool_swap_count=len(pool_swaps))
        
        trade_events = {}
        try:
            for id, swap in pool_swaps.items():
                trade = Trade(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    taker=swap.taker,
                    direction=swap.direction,
                    base_token=swap.base_token,
                    base_amount=swap.base_amount,
                    swaps=pool_swaps,
                    trade_type="trade",
                )
                context.add_events({trade.content_id: trade})
                trade_events[trade.content_id] = trade
            
            return trade_events
        
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_event_production")
            context.add_errors({error.error_id: error})
            self.log_error("Trade event production failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e))
            return trade_events

    def _create_processing_error(self, e: Exception, tx_hash: str, stage: str) -> ProcessingError:
        """Create processing error for trade operations"""
        return create_transform_error(
            error_type="trade_processing_exception",
            message=f"Exception in {stage}: {str(e)}",
            tx_hash=tx_hash,
            contract_address=None,
            transformer_name="TradeProcessor"
        )