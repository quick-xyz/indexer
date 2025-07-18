# indexer/transform/processors/trade_processor.py

from typing import Dict, Optional, Tuple, Literal
from collections import defaultdict
from dataclasses import dataclass
import msgspec

from ..registry import TransformRegistry
from ..context import TransformContext
from ...core.indexer_config import IndexerConfig
from ...core.logging import LoggingMixin
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
    TransferSignal,
    Position,
    Transfer,
)
from ...utils.amounts import amount_to_int, amount_to_str, amount_to_negative_str


@dataclass
class RouteContext:
    router: EvmAddress
    taker: EvmAddress
    direction: Literal["buy", "sell"]
    amount: str

'''
Trade definition:
- Unique (transaction, taker, direction, base_token)
- Can contain multiple swaps
- There can be multiple trades in a single transaction
'''
class TradeProcessor(LoggingMixin):
    def __init__(self, registry: TransformRegistry, config: IndexerConfig):
        if not registry:
            raise ValueError("TransformRegistry cannot be None")
        if not config:
            raise ValueError("IndexerConfig cannot be None")
        
        self.registry = registry
        self.config = config
        
        self.log_info("TradeProcessor initialized", 
                     tracked_tokens=len(config.tracked_tokens))
    
    def process_trade_signals(self, trade_signals: Dict[int, Signal], context: TransformContext) -> bool:
        """Main entry point for trade signal processing"""
        if not trade_signals:
            self.log_debug("No trade signals to process", tx_hash=context.transaction.tx_hash)
            return True
        
        if not context:
            self.log_error("TransformContext cannot be None")
            raise ValueError("TransformContext cannot be None")
        
        self.log_debug("Starting trade processing pipeline",
                      tx_hash=context.transaction.tx_hash,
                      signal_count=len(trade_signals))
        
        try:
            # Stage 1: Aggregate batch signals → SwapSignals
            self.log_debug("Starting batch signal aggregation", tx_hash=context.transaction.tx_hash)
            aggregated_signals = self._aggregate_batch_signals(trade_signals, context)
            
            # Extract route signals for later processing
            route_signals = {
                idx: signal for idx, signal in trade_signals.items()
                if isinstance(signal, (RouteSignal, MultiRouteSignal))
            }
            
            self.log_debug("Categorized trade signals",
                          tx_hash=context.transaction.tx_hash,
                          aggregated_signals=len(aggregated_signals),
                          route_signals=len(route_signals))

            # Stage 2: Process SwapSignals → PoolSwap events
            self.log_debug("Starting swap signal processing", tx_hash=context.transaction.tx_hash)
            pool_swaps = self._process_swap_signals(aggregated_signals, context)
            
            # Stage 3: Aggregate PoolSwaps → Trade events
            self.log_debug("Starting trade event production", tx_hash=context.transaction.tx_hash)
            trade_events = self._produce_trade_events(pool_swaps, route_signals, context)
            
            # Stage 4: Check for arbitrage
            self.log_debug("Checking for arbitrage opportunities", tx_hash=context.transaction.tx_hash)
            arbitrage_detected = self._check_arbitrage(trade_events, context)
                
            self.log_info("Trade processing completed successfully",
                         tx_hash=context.transaction.tx_hash,
                         pool_swaps_created=len(pool_swaps),
                         trade_events_created=len(trade_events),
                         arbitrage_detected=arbitrage_detected)
            
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
        """Aggregate batch swap signals into single swap signals"""
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
        
        try:
            # Group batch signals by pool and recipient
            batch_groups = defaultdict(lambda: defaultdict(dict))
            for idx, signal in batch_signals.items():
                batch_groups[signal.pool][signal.to][idx] = signal
            
            aggregated = trade_signals.copy()
            aggregated_count = 0
            
            for pool, to_dict in batch_groups.items():
                for to, batch_dict in to_dict.items():
                    try:
                        transformer = self.registry.get_transformer(pool)
                        if not transformer:
                            self.log_warning("No transformer found for pool",
                                           tx_hash=context.transaction.tx_hash,
                                           pool=pool)
                            continue
                        
                        if not hasattr(transformer, 'base_token') or not hasattr(transformer, 'quote_token'):
                            self.log_error("Transformer missing token information",
                                         tx_hash=context.transaction.tx_hash,
                                         pool=pool,
                                         transformer_name=type(transformer).__name__)
                            continue
                        
                        token_tuple = (transformer.base_token, transformer.quote_token)
                        
                        # Get the pattern from the first signal
                        first_signal = next(iter(batch_dict.values()))
                        pattern = self.registry.get_pattern(first_signal.pattern)
                        
                        if not pattern:
                            self.log_warning("No pattern found for batch aggregation",
                                           tx_hash=context.transaction.tx_hash,
                                           pattern_name=first_signal.pattern)
                            continue
                        
                        if not hasattr(pattern, 'aggregate_signals'):
                            self.log_warning("Pattern does not support batch aggregation",
                                           tx_hash=context.transaction.tx_hash,
                                           pattern_name=first_signal.pattern)
                            continue

                        batch_swap_signal = pattern.aggregate_signals(batch_dict, token_tuple, context)
                        if batch_swap_signal:
                            aggregated[batch_swap_signal.log_index] = batch_swap_signal
                            # Remove individual batch signals
                            for idx in batch_dict.keys():
                                del aggregated[idx]
                            aggregated_count += 1
                            
                            self.log_debug("Batch signals aggregated successfully",
                                          tx_hash=context.transaction.tx_hash,
                                          pool=pool,
                                          batch_size=len(batch_dict),
                                          new_signal_index=batch_swap_signal.log_index)
                        
                    except Exception as e:
                        self.log_error("Failed to aggregate batch for pool",
                                      tx_hash=context.transaction.tx_hash,
                                      pool=pool,
                                      error=str(e),
                                      exception_type=type(e).__name__)
                        continue
            
            self.log_debug("Batch aggregation completed",
                          tx_hash=context.transaction.tx_hash,
                          original_signals=len(trade_signals),
                          aggregated_signals=len(aggregated),
                          aggregations_performed=aggregated_count)
            
            return aggregated
            
        except Exception as e:
            self.log_error("Batch signal aggregation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            # Return original signals if aggregation fails
            return trade_signals

    def _process_swap_signals(self, signals: Dict[int, Signal], 
                                context: TransformContext) -> Dict[DomainEventId, PoolSwap]:
        """Process swap signals into pool swap events"""
        swap_signals = {idx: signal for idx, signal in signals.items() if isinstance(signal, SwapSignal)}
        
        if not swap_signals:
            self.log_debug("No swap signals to process", tx_hash=context.transaction.tx_hash)
            return {}
        
        self.log_debug("Processing swap signals into pool swaps",
                    tx_hash=context.transaction.tx_hash,
                    swap_signal_count=len(swap_signals))
        
        pool_swaps = {}
        processed_count = 0
        failed_count = 0
        
        try:
            for log_index, signal in swap_signals.items():
                try:
                    self.log_debug("Processing swap signal",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  pool=signal.pool,
                                  base_amount=signal.base_amount,
                                  quote_amount=signal.quote_amount)

                    pattern = self.registry.get_pattern(signal.pattern)
                    if not pattern:
                        self.log_warning("No pattern found for swap signal",
                                        tx_hash=context.transaction.tx_hash,
                                        log_index=log_index,
                                        pattern_name=signal.pattern)
                        failed_count += 1
                        continue
                    
                    self.log_debug("Processing swap signal with pattern",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  pattern_name=signal.pattern,
                                  pool=signal.pool,
                                  taker=signal.to)
                    
                    result = pattern.produce_events({log_index: signal}, context)

                    if result:
                        pool_swaps.update(result)
                        processed_count += 1
                        
                        self.log_debug("Swap signal processed successfully",
                                      tx_hash=context.transaction.tx_hash,
                                      log_index=log_index,
                                      events_created=len(result))
                    else:
                        self.log_warning("Swap signal pattern processing produced no events",
                                        tx_hash=context.transaction.tx_hash,
                                        log_index=log_index,
                                        pattern_name=signal.pattern)
                        failed_count += 1
                
                except Exception as e:
                    self.log_error("Failed to process individual swap signal",
                                  tx_hash=context.transaction.tx_hash,
                                  log_index=log_index,
                                  error=str(e),
                                  exception_type=type(e).__name__)
                    failed_count += 1
                    continue

            self.log_debug("Swap signal processing completed",
                          tx_hash=context.transaction.tx_hash,
                          total_signals=len(swap_signals),
                          processed_count=processed_count,
                          failed_count=failed_count,
                          pool_swaps_created=len(pool_swaps))

            return pool_swaps
            
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "swap_signal_processing")
            context.add_errors({error.error_id: error})
            self.log_error("Swap signal processing failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return pool_swaps

    def _produce_trade_events(self, pool_swaps: Dict[DomainEventId, PoolSwap],
                              route_signals: Dict[int, RouteSignal|MultiRouteSignal],
                              context: TransformContext) -> Dict[DomainEventId, Trade]:
        """Produce trade events from pool swaps and route information"""
        if not pool_swaps:
            self.log_debug("No pool swaps to produce trade events", tx_hash=context.transaction.tx_hash)
            return {}
        
        self.log_debug("Producing trade events from pool swaps",
                      tx_hash=context.transaction.tx_hash,
                      pool_swap_count=len(pool_swaps),
                      route_signals_count=len(route_signals))
        
        trade_events = {}
        
        try:
            # Group swaps by base token and direction
            trade_swaps = defaultdict(lambda: {"buy": {}, "sell": {}})
            
            for swap_id, swap in pool_swaps.items():
                if not hasattr(swap, 'base_token') or not hasattr(swap, 'direction'):
                    self.log_warning("Pool swap missing required attributes",
                                    tx_hash=context.transaction.tx_hash,
                                    swap_id=swap_id)
                    continue
                
                trade_swaps[swap.base_token][swap.direction][swap_id] = swap
                
                self.log_debug("Categorized pool swap",
                              tx_hash=context.transaction.tx_hash,
                              swap_id=swap_id,
                              base_token=swap.base_token,
                              direction=swap.direction)

            self.log_debug("Pool swaps categorized by token and direction",
                          tx_hash=context.transaction.tx_hash,
                          base_tokens=len(trade_swaps))

            # Process each base token
            for base_token, direction_swaps in trade_swaps.items():
                try:
                    self.log_debug("Processing swaps for base token",
                                  tx_hash=context.transaction.tx_hash,
                                  base_token=base_token,
                                  buy_swaps=len(direction_swaps.get("buy", {})),
                                  sell_swaps=len(direction_swaps.get("sell", {})))

                    # Build route context for this token
                    buy_routes, sell_routes = self._build_route_context(base_token, route_signals)
                    
                    self.log_debug("Route context built",
                                  tx_hash=context.transaction.tx_hash,
                                  base_token=base_token,
                                  buy_routes=len(buy_routes),
                                  sell_routes=len(sell_routes))

                    # Process buy swaps
                    buy_swaps = direction_swaps.get("buy", {})
                    if buy_swaps:
                        if buy_routes and len(buy_routes) == 1:
                            self.log_debug("Building routed buy trades", tx_hash=context.transaction.tx_hash)
                            trades = self._build_routed_trades(base_token, next(iter(buy_routes.values())), buy_swaps, context)
                        else:
                            self.log_debug("Building simple buy trades", tx_hash=context.transaction.tx_hash)
                            trades = self._build_trades(base_token, buy_swaps, context)
                        
                        if trades:
                            trade_events.update(trades)

                    # Process sell swaps
                    sell_swaps = direction_swaps.get("sell", {})
                    if sell_swaps:
                        if sell_routes and len(sell_routes) == 1:
                            self.log_debug("Building routed sell trades", tx_hash=context.transaction.tx_hash)
                            trades = self._build_routed_trades(base_token, next(iter(sell_routes.values())), sell_swaps, context)
                        else:
                            self.log_debug("Building simple sell trades", tx_hash=context.transaction.tx_hash)
                            trades = self._build_trades(base_token, sell_swaps, context)
                        
                        if trades:
                            trade_events.update(trades)
                
                except Exception as e:
                    self.log_error("Failed to process swaps for base token",
                                  tx_hash=context.transaction.tx_hash,
                                  base_token=base_token,
                                  error=str(e),
                                  exception_type=type(e).__name__)
                    continue
            
            self.log_debug("Trade event production completed",
                          tx_hash=context.transaction.tx_hash,
                          trade_events_created=len(trade_events))
            
            return trade_events
        
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_event_production")
            context.add_errors({error.error_id: error})
            self.log_error("Trade event production failed with exception",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return trade_events

    def _build_route_context(self, base_token: EvmAddress, route_signals: Dict[int, RouteSignal|MultiRouteSignal]) -> Tuple[Dict[int, RouteContext], Dict[int, RouteContext]]:
        """Build route context for buy and sell directions"""
        if not route_signals:
            return {}, {}
        
        try:
            buy_routes, sell_routes = {}, {}

            for idx, route in route_signals.items():
                try:
                    if isinstance(route, RouteSignal):
                        if base_token == route.token_out:
                            buy_routes[idx] = RouteContext(
                                router=route.contract,
                                taker=route.to or route.sender,
                                direction="buy",
                                amount=route.amount_out,
                            )
                        if base_token == route.token_in:
                            sell_routes[idx] = RouteContext(
                                router=route.contract,
                                taker=route.to or route.sender,
                                direction="sell",
                                amount=route.amount_in,
                            )
                    elif isinstance(route, MultiRouteSignal):
                        if base_token in route.tokens_out:
                            token_index = route.tokens_out.index(base_token)
                            buy_routes[idx] = RouteContext(
                                router=route.contract,
                                taker=route.to or route.sender,
                                direction="buy",
                                amount=route.amounts_out[token_index],
                            )
                        if base_token in route.tokens_in:
                            token_index = route.tokens_in.index(base_token)
                            sell_routes[idx] = RouteContext(
                                router=route.contract,
                                taker=route.to or route.sender,
                                direction="sell",
                                amount=route.amounts_in[token_index],
                            )
                
                except Exception as e:
                    self.log_warning("Failed to process route signal",
                                    route_index=idx,
                                    error=str(e),
                                    exception_type=type(e).__name__)
                    continue

            # Aggregate routes if necessary (simplified logic for now)
            if len(buy_routes) > 1 or len(sell_routes) > 1:
                self.log_debug("Multiple routes detected - using aggregation logic",
                              buy_routes=len(buy_routes),
                              sell_routes=len(sell_routes))
                
                buy_routes, sell_routes = self._aggregate_routes(buy_routes, sell_routes)
            
            return buy_routes, sell_routes
            
        except Exception as e:
            self.log_error("Failed to build route context",
                          base_token=base_token,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}, {}

    def _aggregate_routes(self, buy_routes: Dict[int, RouteContext], sell_routes: Dict[int, RouteContext]) -> Tuple[Dict[int, RouteContext], Dict[int, RouteContext]]:
        """Aggregate multiple routes when possible"""
        try:
            # Simple aggregation logic - keep only top-level route if amounts match
            if len(buy_routes) > 1:
                top_level = max(buy_routes.keys())
                sum_remaining = sum(amount_to_int(route.amount) for idx, route in buy_routes.items() if idx != top_level)
                if sum_remaining == amount_to_int(buy_routes[top_level].amount):
                    buy_routes = {top_level: buy_routes[top_level]}

            if len(sell_routes) > 1:
                top_level = max(sell_routes.keys())
                sum_remaining = sum(amount_to_int(route.amount) for idx, route in sell_routes.items() if idx != top_level)
                if sum_remaining == amount_to_int(sell_routes[top_level].amount):
                    sell_routes = {top_level: sell_routes[top_level]}
            
            return buy_routes, sell_routes
            
        except Exception as e:
            self.log_error("Route aggregation failed",
                          error=str(e),
                          exception_type=type(e).__name__)
            return buy_routes, sell_routes
        
    def _build_trades(self, base_token: EvmAddress, swaps: Dict[DomainEventId, PoolSwap], context: TransformContext) -> Optional[Dict[DomainEventId, Trade]]:
        """Build trade events from pool swaps"""
        if not swaps:
            self.log_debug("No swaps to build trades from", tx_hash=context.transaction.tx_hash)
            return {}

        try:
            trade_events = {}
            direction = next(iter(swaps.values())).direction
            
            if len(swaps) == 1:
                # Single swap trade
                single_swap = next(iter(swaps.values()))
                trade = Trade(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    taker=single_swap.taker,
                    direction=direction,
                    base_token=base_token,
                    base_amount=single_swap.base_amount,
                    quote_token=single_swap.quote_token,
                    quote_amount=single_swap.quote_amount,
                    swaps=swaps,
                    trade_type="trade",
                )
                context.add_events({trade.content_id: trade})
                context.remove_events(list(swaps.keys()))
                trade_events[trade.content_id] = trade
                
                self.log_debug("Single swap trade created",
                              tx_hash=context.transaction.tx_hash,
                              trade_id=trade.content_id,
                              taker=trade.taker)
            else:
                # Multiple swaps - group by taker
                grouped_swaps = defaultdict(lambda: {"swaps": {}, "amount": 0})
                for swap_id, swap in swaps.items():
                    grouped_swaps[swap.taker]["swaps"][swap_id] = swap
                    grouped_swaps[swap.taker]["amount"] += amount_to_int(swap.base_amount)

                for taker, group_data in grouped_swaps.items():
                    trade = Trade(
                        timestamp=context.transaction.timestamp,
                        tx_hash=context.transaction.tx_hash,
                        taker=taker,
                        direction=direction,
                        base_token=base_token,
                        base_amount=amount_to_str(group_data["amount"]),
                        swaps=group_data["swaps"],
                        trade_type="trade",
                    )
                    context.add_events({trade.content_id: trade})
                    context.remove_events(list(group_data["swaps"].keys()))
                    trade_events[trade.content_id] = trade
                    
                    self.log_debug("Multi-swap trade created",
                                  tx_hash=context.transaction.tx_hash,
                                  trade_id=trade.content_id,
                                  taker=taker,
                                  swap_count=len(group_data["swaps"]))
            
            return trade_events
            
        except Exception as e:
            self.log_error("Failed to build trades",
                          tx_hash=context.transaction.tx_hash,
                          base_token=base_token,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}
    
    def _build_routed_trades(self, base_token: EvmAddress, route: RouteContext, swaps: Dict[DomainEventId, PoolSwap], context: TransformContext) -> Optional[Dict[DomainEventId, Trade]]:
        """Build routed trades with transfer reconciliation"""
        try:
            self.log_debug("Building routed trade",
                          tx_hash=context.transaction.tx_hash,
                          base_token=base_token,
                          route_amount=route.amount,
                          route_direction=route.direction,
                          swap_count=len(swaps))

            base_amount = sum(amount_to_int(swap.base_amount) for swap in swaps.values())

            # Calculate address balances from swap positions
            address_balances = defaultdict(int)
            for swap in swaps.values():
                for position in swap.positions.values():
                    if position.token == base_token:
                        address_balances[position.user] += amount_to_int(position.amount)
                
            trf_in, trf_out = context.get_unmatched_token_transfers(base_token)

            # Check if PoolSwaps already cover route
            if base_amount == amount_to_int(route.amount):
                # Pool side is complete, check taker side
                if amount_to_int(route.amount) == address_balances[route.taker]:
                    self.log_debug("Routed trade matches exact amount - creating complete trade",
                                  tx_hash=context.transaction.tx_hash,
                                  base_token=base_token,
                                  route_amount=route.amount,
                                  taker=route.taker)
                    
                    trade = Trade(
                        timestamp=context.transaction.timestamp,
                        tx_hash=context.transaction.tx_hash,
                        taker=route.taker,
                        router=route.router,
                        direction=route.direction,
                        base_token=base_token,
                        base_amount=route.amount,
                        swaps=swaps,
                        trade_type="trade",
                    )
                    context.add_events({trade.content_id: trade})
                    context.remove_events(list(swaps.keys()))
                    return {trade.content_id: trade}
                    
                else:
                    # Taker side is not complete, need to reconcile unmatched transfers
                    self.log_debug("Attempting transfer reconciliation for routed trade",
                                  tx_hash=context.transaction.tx_hash,
                                  taker_balance=address_balances[route.taker],
                                  route_amount=route.amount)
                    
                    return self._reconcile_routed_trade(base_token, route, swaps, address_balances, trf_in, trf_out, context)
            
            # Unhandled PoolSwaps - fall back to simple trades
            self.log_debug("Route amount mismatch - falling back to simple trades",
                          tx_hash=context.transaction.tx_hash,
                          pool_amount=base_amount,
                          route_amount=route.amount)
            
            return self._build_trades(base_token, swaps, context)
            
        except Exception as e:
            self.log_error("Failed to build routed trade",
                          tx_hash=context.transaction.tx_hash,
                          base_token=base_token,
                          error=str(e),
                          exception_type=type(e).__name__)
            return self._build_trades(base_token, swaps, context)

    def _reconcile_routed_trade(self, base_token: EvmAddress, route: RouteContext, swaps: Dict[DomainEventId, PoolSwap], 
                               address_balances: Dict[EvmAddress, int], trf_in: Dict, trf_out: Dict, context: TransformContext) -> Optional[Dict[DomainEventId, Trade]]:
        """Reconcile transfers for routed trades"""
        try:
            swaps_taker_bal = address_balances[route.taker]
            taker_deficit = amount_to_int(route.amount) - swaps_taker_bal

            if route.direction == "buy":
                unmatched_taker_transfers = {
                    idx: transfer for address, transfers in trf_in.items()
                    if address == route.taker
                    for idx, transfer in transfers.items()
                }
            else:
                unmatched_taker_transfers = {
                    idx: transfer for address, transfers in trf_out.items()
                    if address == route.taker
                    for idx, transfer in transfers.items()
                }

            # Find matching transfer
            for idx, transfer in unmatched_taker_transfers.items():
                if amount_to_int(transfer.amount) == taker_deficit:
                    # Create transfer event
                    positions = self._generate_positions({idx: transfer}, context)
                    transfer_event = Transfer(
                        timestamp=context.transaction.timestamp,
                        tx_hash=context.transaction.tx_hash,
                        token=transfer.token,
                        from_address=transfer.from_address,
                        to_address=transfer.to_address,
                        amount=transfer.amount,
                        positions=positions,
                        signals={idx: transfer},
                    )
                    context.add_events({transfer_event.content_id: transfer_event})

                    # Create complete trade
                    trade = Trade(
                        timestamp=context.transaction.timestamp,
                        tx_hash=context.transaction.tx_hash,
                        taker=route.taker,
                        router=route.router,
                        direction=route.direction,
                        base_token=base_token,
                        base_amount=route.amount,
                        swaps=swaps,
                        trade_type="trade",
                        transfers={transfer_event.content_id: transfer_event},
                    )
                    context.add_events({trade.content_id: trade})
                    context.remove_events(list(swaps.keys()))

                    self.log_debug("Routed trade with transfer reconciliation created",
                                  tx_hash=context.transaction.tx_hash,
                                  trade_id=trade.content_id,
                                  transfer_amount=transfer.amount)

                    return {trade.content_id: trade}
            
            # No matching transfer found
            self.log_warning("No matching transfer found for routed trade reconciliation",
                           tx_hash=context.transaction.tx_hash,
                           taker_deficit=taker_deficit,
                           available_transfers=len(unmatched_taker_transfers))
            
            return self._build_trades(base_token, swaps, context)
            
        except Exception as e:
            self.log_error("Transfer reconciliation failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return self._build_trades(base_token, swaps, context)

    def _generate_positions(self, transfers: Dict[int, TransferSignal], context: TransformContext) -> Dict[DomainEventId, Position]:
        """Generate position changes from transfers"""
        if not transfers:
            return {}
        
        try:
            positions = {}
            
            for transfer in transfers.values():
                # Generate position for recipient (if not zero address and is tracked token)
                if transfer.to_address != ZERO_ADDRESS and transfer.token in context.tracked_tokens:
                    position_in = Position(
                        timestamp=context.transaction.timestamp,
                        tx_hash=context.transaction.tx_hash,
                        user=transfer.to_address,
                        custodian=transfer.to_address,
                        token=transfer.token,
                        amount=transfer.amount,
                    )
                    positions[position_in.content_id] = position_in

                # Generate position for sender (if not zero address and is tracked token)
                if transfer.from_address != ZERO_ADDRESS and transfer.token in context.tracked_tokens:
                    position_out = Position(
                        timestamp=context.transaction.timestamp,
                        tx_hash=context.transaction.tx_hash,
                        user=transfer.from_address,
                        custodian=transfer.from_address,
                        token=transfer.token,
                        amount=amount_to_negative_str(transfer.amount),
                    )
                    positions[position_out.content_id] = position_out

            context.add_positions(positions)
            
            self.log_debug("Positions generated for transfers",
                          tx_hash=context.transaction.tx_hash,
                          transfer_count=len(transfers),
                          position_count=len(positions))
            
            return positions
            
        except Exception as e:
            self.log_error("Failed to generate positions",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return {}
            
    def _check_arbitrage(self, trades: Dict[DomainEventId, Trade], context: TransformContext) -> bool:
        """Check for arbitrage opportunities and mark trades accordingly"""
        if not trades:
            self.log_debug("No trades to check for arbitrage", tx_hash=context.transaction.tx_hash)
            return False
        
        try:
            buy_trades = {trade_id: trade for trade_id, trade in trades.items() if trade.direction == "buy"}
            sell_trades = {trade_id: trade for trade_id, trade in trades.items() if trade.direction == "sell"}
            
            if not buy_trades or not sell_trades:
                self.log_debug("Arbitrage requires both buy and sell trades",
                              tx_hash=context.transaction.tx_hash,
                              buy_trades=len(buy_trades),
                              sell_trades=len(sell_trades))
                return False
            
            buy_net_amount = sum(amount_to_int(trade.base_amount) for trade in buy_trades.values())
            sell_net_amount = sum(amount_to_int(trade.base_amount) for trade in sell_trades.values())

            if buy_net_amount == sell_net_amount:
                self.log_info("Arbitrage opportunity detected",
                              tx_hash=context.transaction.tx_hash,
                              buy_amount=buy_net_amount,
                              sell_amount=sell_net_amount,
                              buy_trades=len(buy_trades),
                              sell_trades=len(sell_trades))
                
                # Mark all trades as arbitrage
                for trade in trades.values():
                    context.remove_events([trade.content_id])
                    new_trade = msgspec.structs.replace(trade, trade_type="arbitrage")
                    context.add_events({new_trade.content_id: new_trade})
                
                return True
            else:
                self.log_debug("No arbitrage detected - amounts don't match",
                              tx_hash=context.transaction.tx_hash,
                              buy_amount=buy_net_amount,
                              sell_amount=sell_net_amount)
                return False
            
        except Exception as e:
            self.log_error("Arbitrage check failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return False

    def _create_processing_error(self, e: Exception, tx_hash: str, stage: str) -> ProcessingError:
        """Create processing error for trade operations"""
        return create_transform_error(
            error_type="trade_processing_exception",
            message=f"Trade processing failed in {stage}: {str(e)}",
            tx_hash=tx_hash,
            contract_address=None,
            transformer_name="TradeProcessor"
        )