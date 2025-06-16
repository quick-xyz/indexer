# indexer/transform/processors/trade_processor.py

from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict

from ..registry import TransformRegistry
from ..context import TransformContext
from ...core.config import IndexerConfig
from ...core.mixins import LoggingMixin
from ...types import (
    Signal,
    RouteSignal,
    MultiRouteSignal,
    SwapBatchSignal,
    SwapSignal,
    ProcessingError,
    ErrorId,
    create_transform_error
)
from ...utils.amounts import amount_to_int, amount_to_str


class TradeProcessor(LoggingMixin):
    """Processor for trade-related signal processing and event generation"""
    
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        self.registry = registry
        self.config = config
        
        self.log_info("TradeProcessor initialized", 
                     indexer_tokens=len(config.get_indexer_tokens()))
    
    def process_trade_signals(self, trade_signals: Dict[int, Signal], context: TransformContext) -> bool:
        """Process all trade-related signals into events"""
        if not trade_signals:
            self.log_debug("No trade signals to process", tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Processing trade signals",
                      tx_hash=context.transaction.tx_hash,
                      signal_count=len(trade_signals))
        
        try:
            # Phase 1: Extract route context and override SwapSignal takers
            route_context = self._extract_route_context(trade_signals, context)
            corrected_signals = self._apply_route_corrections(trade_signals, route_context, context)
            
            # Phase 2: Aggregate SwapBatchSignals into SwapSignals
            success = self._aggregate_batch_signals(corrected_signals, context)
            if not success:
                return False
            
            # Phase 3: Process SwapSignals into PoolSwap Events
            success = self._process_swap_signals(context)
            if not success:
                return False
            
            # Phase 4: TODO - Validate Swaps against Router Signals and generate Trade Events
            # This would create higher-level Trade events that combine multiple swaps
            
            self.log_debug("Trade processing completed successfully",
                          tx_hash=context.transaction.tx_hash)
            return True
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_processing")
            context.add_errors({error.error_id: error})
            self.log_error("Trade processing failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False
    
    def _extract_route_context(self, trade_signals: Dict[int, Signal], context: TransformContext) -> Optional[Dict[str, Any]]:
        """Extract routing context from Route signals using log index order"""
        route_signals = {idx: signal for idx, signal in trade_signals.items() 
                        if isinstance(signal, (RouteSignal, MultiRouteSignal))}
        
        if not route_signals:
            self.log_debug("No route signals found", tx_hash=context.transaction.tx_hash)
            return None
        
        self.log_debug("Found route signals for context extraction",
                      tx_hash=context.transaction.tx_hash,
                      route_signal_count=len(route_signals),
                      route_indices=list(route_signals.keys()))
        
        # Find the RouteSignal with the highest log_index (top-level aggregator)
        top_level_idx = max(route_signals.keys())
        top_level_route = route_signals[top_level_idx]
        
        route_context = {
            'taker': top_level_route.to,
            'aggregator_contract': top_level_route.contract,
            'top_level_route': top_level_route,
            'all_routes': list(route_signals.values()),
            'route_count': len(route_signals)
        }
        
        self.log_debug("Route context extracted",
                      tx_hash=context.transaction.tx_hash,
                      top_level_index=top_level_idx,
                      aggregator_contract=route_context['aggregator_contract'],
                      taker=route_context['taker'],
                      total_routes=route_context['route_count'])
        
        return route_context
    
    def _apply_route_corrections(self, trade_signals: Dict[int, Signal], 
                               route_context: Optional[Dict[str, Any]], 
                               context: TransformContext) -> Dict[int, Signal]:
        """Apply route-based corrections to SwapSignals"""
        if not route_context:
            self.log_debug("No route context available - using original signals",
                          tx_hash=context.transaction.tx_hash)
            return trade_signals
        
        corrected_signals = trade_signals.copy()
        swap_corrections = 0
        
        # Extract swap signals for correction
        swap_signals = {idx: signal for idx, signal in trade_signals.items() 
                       if isinstance(signal, SwapSignal)}
        
        if not swap_signals:
            self.log_debug("No swap signals to correct",
                          tx_hash=context.transaction.tx_hash)
            return corrected_signals
        
        self.log_debug("Applying route corrections to swap signals",
                      tx_hash=context.transaction.tx_hash,
                      swap_signal_count=len(swap_signals),
                      original_taker_candidates=[s.to for s in swap_signals.values()],
                      route_taker=route_context['taker'])
        
        # Apply corrections to SwapSignals
        for idx, signal in swap_signals.items():
            if isinstance(signal, SwapSignal):
                original_to = signal.to
                
                # Create corrected signal with updated taker
                corrected_signal = SwapSignal(
                    log_index=signal.log_index,
                    pattern=signal.pattern,
                    pool=signal.pool,
                    base_amount=signal.base_amount,
                    base_token=signal.base_token,
                    quote_amount=signal.quote_amount,
                    quote_token=signal.quote_token,
                    to=route_context['taker'],  # Override with route taker
                    sender=signal.sender,
                    batch=signal.batch
                )
                
                corrected_signals[idx] = corrected_signal
                swap_corrections += 1
                
                self.log_debug("SwapSignal taker corrected",
                              tx_hash=context.transaction.tx_hash,
                              signal_index=idx,
                              pool=signal.pool,
                              original_to=original_to,
                              corrected_to=route_context['taker'])
        
        self.log_info("Route corrections applied",
                     tx_hash=context.transaction.tx_hash,
                     corrections_applied=swap_corrections,
                     route_context_used=route_context['aggregator_contract'])
        
        return corrected_signals
    
    def _aggregate_batch_signals(self, trade_signals: Dict[int, Signal], context: TransformContext) -> bool:
        """Aggregate SwapBatchSignals into SwapSignals"""
        batch_signals = {idx: signal for idx, signal in trade_signals.items() 
                        if isinstance(signal, SwapBatchSignal)}
        
        if not batch_signals:
            self.log_debug("No batch signals to aggregate", tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Processing batch swap signals",
                      tx_hash=context.transaction.tx_hash,
                      batch_signal_count=len(batch_signals))
        
        batch_dict = {}
        batch_components = {}
        
        try:
            for log_index, signal in batch_signals.items():
                key = "_".join((str(signal.pool), str(signal.to)))
                transformer = self.registry.get_transformer(signal.pool)
                
                if not transformer:
                    self.log_warning("No transformer found for swap pool",
                                    tx_hash=context.transaction.tx_hash,
                                    pool=signal.pool,
                                    log_index=log_index)
                    continue
                
                if key not in batch_dict:
                    batch_dict[key] = {
                        "index": 0,
                        "pool": signal.pool,
                        "to": signal.to,
                        "base_amount": 0,
                        "quote_amount": 0,
                        "base_token": transformer.base_token,
                        "quote_token": transformer.quote_token,
                        "sender": signal.to if signal.to else None
                    }
                    batch_components[key] = {}
                
                batch_dict[key]["index"] += amount_to_int(signal.log_index)
                batch_dict[key]["base_amount"] += amount_to_int(signal.base_amount)
                batch_dict[key]["quote_amount"] += amount_to_int(signal.quote_amount)
                batch_components[key][str(signal.id)] = (signal.base_amount, signal.quote_amount)
            
            # Create SwapSignals from aggregated batches
            for key, data in batch_dict.items():
                swap_signal = SwapSignal(
                    log_index=data["index"] * 100,  # Ensure unique index
                    pattern="Swap_A",
                    pool=data["pool"],
                    base_amount=amount_to_str(data["base_amount"]),
                    base_token=data["base_token"],
                    quote_amount=amount_to_str(data["quote_amount"]),
                    quote_token=data["quote_token"],
                    to=data["to"],
                    sender=data["sender"],
                    batch=batch_components[key],
                )
                context.add_signals({swap_signal.log_index: swap_signal})
                
                self.log_debug("SwapSignal created from batch",
                              tx_hash=context.transaction.tx_hash,
                              swap_signal_index=swap_signal.log_index,
                              pool=data["pool"],
                              base_amount=data["base_amount"],
                              quote_amount=data["quote_amount"])
            
            return True
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "batch_aggregation")
            context.add_errors({error.error_id: error})
            self.log_error("Batch signal aggregation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e))
            return False
    
    def _process_swap_signals(self, context: TransformContext) -> bool:
        """Process SwapSignals into PoolSwap Events"""
        swap_signals = context.get_swap_signals()
        
        if not swap_signals:
            self.log_debug("No swap signals to process", tx_hash=context.transaction.tx_hash)
            return True
        
        self.log_debug("Processing swap signals into events",
                      tx_hash=context.transaction.tx_hash,
                      swap_signal_count=len(swap_signals))
        
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
                
                if not pattern.process_signal(signal, context):
                    self.log_warning("Swap signal pattern processing failed",
                                    tx_hash=context.transaction.tx_hash,
                                    log_index=log_index)
                    return False
            
            return True
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "swap_signal_processing")
            context.add_errors({error.error_id: error})
            self.log_error("Swap signal processing failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e))
            return False
    
    def _create_processing_error(self, e: Exception, tx_hash: str, stage: str) -> ProcessingError:
        """Create processing error for trade operations"""
        return create_transform_error(
            error_type="trade_processing_exception",
            message=f"Exception in {stage}: {str(e)}",
            tx_hash=tx_hash,
            contract_address=None,
            transformer_name="TradeProcessor"
        )