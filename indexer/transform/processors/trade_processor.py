# indexer/transform/processors/trade_processor.py

from typing import Dict, Optional, List, Tuple, Literal
from collections import defaultdict
from dataclasses import dataclass
import msgspec

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
            route_signals = {idx: signal for idx, signal in trade_signals.items()
                             if isinstance(signal, (RouteSignal, MultiRouteSignal))}
            known_routers = set()
            for route_signal in route_signals.values():
                known_routers.add(route_signal.contract)

            # Stage 2: Process SwapSignals → PoolSwap events
            pool_swaps = self._process_swap_signals(aggregated_signals, context)
            
            # Stage 3: Aggregate PoolSwaps → Trade events
            trade_events = self._produce_trade_events(pool_swaps, route_signals, context)
            
            # Stage 4: Check for arbitrage
            arbitrage_events = self._check_arbitrage(trade_events, context)
                
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

                batch_swap_signal = pattern.aggregate_signals(batch_dict, token_tuple, context)
                if batch_swap_signal:
                    aggregated[batch_swap_signal.log_index] = batch_swap_signal
                    for idx in batch_dict.keys():
                        del aggregated[idx]

        
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
                
                result = pattern.produce_events({log_index: signal}, context)
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

    def _produce_trade_events(self, pool_swaps: Dict[DomainEventId, PoolSwap],
                              route_signals: Dict[int, RouteSignal|MultiRouteSignal],
                              context: TransformContext) -> Dict[DomainEventId, Trade]:
        if not pool_swaps:
            self.log_debug("No pool swaps to produce trade events", tx_hash=context.transaction.tx_hash)
            return {}
        
        self.log_debug("Producing trade events from pool swaps",
                      tx_hash=context.transaction.tx_hash,
                      pool_swap_count=len(pool_swaps))
        
        trade_events = {}
        trade_swaps = defaultdict(lambda: {"buy": {DomainEventId:PoolSwap}, "sell": {DomainEventId:PoolSwap}})
        
        try:
            for id, swap in pool_swaps.items():
                if swap.direction == "buy":
                    trade_swaps[swap.base_token]["buy"][id] = swap
                else:
                    trade_swaps[swap.base_token]["sell"][id] = swap

            for base_token, dir_swaps in trade_swaps.items():
                buy_routes, sell_routes = self._build_route_context(base_token, route_signals)
                buy_swaps = dir_swaps.get("buy", {})
                sell_swaps = dir_swaps.get("sell", {})                

                if buy_swaps:
                    if buy_routes and len(buy_routes) == 1: # Note: complex multi routes are not supported yet. Only many:one not many:many
                        trades = self._build_routed_trades(base_token, next(iter(buy_routes.values())), buy_swaps, context)
                    else:
                        trades = self._build_trades(base_token, buy_swaps, context)

                    trade_events.update(trades)

                if sell_swaps:
                    if sell_routes and len(sell_routes) == 1: # Note: complex multi routes are not supported yet. Only many:one not many:many
                        trades = self._build_routed_trades(base_token, sell_routes, sell_swaps, context)
                    else:
                        trades = self._build_trades(base_token, sell_swaps, context)

                    trade_events.update(trades)
            
            return trade_events
        
        except Exception as e:
            error = self._create_processing_error(e, context.transaction.tx_hash, "trade_event_production")
            context.add_errors({error.error_id: error})
            self.log_error("Trade event production failed",
                          tx_hash=context.transaction.tx_hash,
                          error=str(e))
            return trade_events

    def _build_route_context(self, base_token: EvmAddress, route_signals: Dict[int, RouteSignal|MultiRouteSignal]) -> Tuple[Dict[int, RouteContext],Dict[int, RouteContext]]:        

        buy_routes, sell_routes = {}, {}

        for idx, route in route_signals.items():
            if isinstance(route, RouteSignal):
                if base_token == route.token_out:
                    buy_routes[idx] = RouteContext(
                        router= route.contract,
                        taker=route.to or route.sender,
                        direction="buy",
                        amount= route.amount_out,
                    )
                if base_token == route.token_in:
                    sell_routes[idx] = RouteContext(
                        router= route.contract,
                        taker=route.to or route.sender,
                        direction="sell",
                        amount= route.amount_in,
                    )
            elif isinstance(route, MultiRouteSignal):
                if base_token in route.tokens_out:
                    buy_routes[idx] = RouteContext(
                        router=route.contract,
                        taker=route.to or route.sender,
                        direction="buy",
                        amount=route.amounts_out[route.tokens_out.index(base_token)],
                    )
                if base_token in route.tokens_in:
                    sell_routes[idx] = RouteContext(
                        router=route.contract,
                        taker=route.to or route.sender,
                        direction="sell",
                        amount=route.amounts_in[route.tokens_in.index(base_token)],
                    )

        if not buy_routes and not sell_routes:
            return None, None
        
        if len(buy_routes) < 2 and len(sell_routes) < 2:
            return buy_routes, sell_routes
        
        if buy_routes > 1:
            # Aggregate buy routes
            top_level = max(buy_routes.keys())
            sum_remaining = sum(int(route.amount) for idx, route in buy_routes.items() if idx != top_level)
            if sum_remaining == buy_routes[top_level].amount:
                buy_routes = {top_level: buy_routes[top_level]}

        if sell_routes > 1:
            # Aggregate sell routes
            top_level = max(sell_routes.keys())
            sum_remaining = sum(int(route.amount) for idx, route in sell_routes.items() if idx != top_level)
            if sum_remaining == sell_routes[top_level].amount:
                sell_routes = {top_level: sell_routes[top_level]}
        
        return buy_routes, sell_routes

        
    def _build_trades(self, base_token: EvmAddress, swaps: Dict[DomainEventId, PoolSwap], context: TransformContext) -> Optional[Dict[DomainEventId,Trade]]:
        trade_events = {}

        if len(swaps) == 0:
            self.log_debug("No swaps to build trades from", tx_hash=context.transaction.tx_hash)
            return trade_events

        direction = swaps[next(iter(swaps))].direction
        if len(swaps) == 1:
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
            context.remove_events(swaps.keys())
            return {trade.content_id: trade}

        else:
            grouped_swaps = {}
            for idx, swap in swaps.items():
                grouped_swaps[swap.taker]["swaps"][idx]= swap
                grouped_swaps[swap.taker]["amount"] += amount_to_int(swap.base_amount)

            for taker, dict in grouped_swaps.items():
                trade = Trade(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    taker=taker,
                    direction=direction,
                    base_token=base_token,
                    base_amount=amount_to_str(dict["amount"]),
                    swaps=dict["swaps"],
                    trade_type="trade",
                )
                context.add_events({trade.content_id: trade})
                context.remove_events(dict["swaps"].keys())
                trade_events[trade.content_id] = trade
            
            return trade_events
    
    def _build_routed_trades(self, base_token: EvmAddress, route: RouteContext, swaps: Dict[DomainEventId,PoolSwap],context: TransformContext) -> Optional[Dict[DomainEventId,Trade]]:
        
        base_amount = sum(amount_to_int(swap.base_amount) for swap in swaps.values())

        address_balances = defaultdict(int)
        for swap in swaps.values():
            for position in swap.positions.values():
                if position.token == base_token:
                    address_balances[position.user] += amount_to_int(position.amount)
            
        trf_in, trf_out = context.get_unmatched_token_transfers(base_token)

        # Check if PoolSwaps already cover route
        if base_amount == route.amount:
            # Pool side is complete, check taker side
            if amount_to_int(route.amount) == address_balances[route.taker]:
                self.log_debug("Routed trade matches exact amount",
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
                context.remove_events(swaps.keys())
                return trade
                
            else:
                # Taker side is not complete, need to reconcile unmatched transfers
                swaps_taker_bal = address_balances[route.taker]
                taker_deficit = amount_to_int(route.amount) - swaps_taker_bal

                if route.direction == "buy":
                    unmatched_taker_transfers = {
                        idx: transfer for idx, transfer in trf_in.items()
                        if transfer.to == route.taker
                    }
                else:
                    unmatched_taker_transfers = {
                        idx: transfer for idx, transfer in trf_out.items()
                        if transfer.to == route.taker
                    }

                # Check each transfer individually
                for idx, transfer in unmatched_taker_transfers.items():
                    if amount_to_int(transfer.amount) == taker_deficit:
                        # Success
                        positions = self._generate_positions([transfer], context)
                        transfer = Transfer(
                            token=transfer.token,
                            from_address=transfer.from_address,
                            to_address=transfer.to_address,
                            amount=transfer.amount,
                            positions=positions,
                            signals={idx: transfer},
                        )
                        context.add_events({transfer.content_id: transfer})

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
                            transfers={transfer.content_id: transfer},
                        )
                        context.add_events({trade.content_id: trade})
                        context.remove_events(swaps.keys())

                        return trade
        
        # Unhandled PoolSwaps currently fail the route
        return self._build_trades(base_token, swaps, context)

    def _generate_positions(self, transfers: List[TransferSignal],context: TransformContext) -> Dict[DomainEventId, Position]:
        positions = {}

        if not transfers:
            return positions
        
        for transfer in transfers:
            if transfer.to_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens:
                position_in = Position(
                    user=transfer.to_address,
                    custodian=transfer.to_address,
                    token=transfer.token,
                    amount=transfer.amount,
                )
                positions[position_in.content_id] = position_in

            if transfer.from_address != ZERO_ADDRESS and transfer.token in context.indexer_tokens:
                position_out = Position(
                    user=transfer.from_address,
                    custodian=transfer.from_address,
                    token=transfer.token,
                    amount=amount_to_negative_str(transfer.amount),
                )
                positions[position_out.content_id] = position_out

        context.add_positions(positions)
        return positions
            
    def _check_arbitrage(self, trades: Dict[DomainEventId,Trade], context: TransformContext) -> bool:
        if not trades:
            self.log_debug("No trades to check for arbitrage", tx_hash=context.transaction.tx_hash)
            return False
        
        buy_trades = {id: trade for id, trade in trades.items() if trade.direction == "buy"}
        sell_trades = {id: trade for id, trade in trades.items() if trade.direction == "sell"}
        buy_net_amount = sum(amount_to_int(trade.base_amount) for trade in buy_trades.values())
        sell_net_amount = sum(amount_to_int(trade.base_amount) for trade in sell_trades.values())

        if buy_net_amount == sell_net_amount:
            self.log_info("Arbitrage detected",
                          tx_hash=context.transaction.tx_hash,
                          buy_amount=buy_net_amount,
                          sell_amount=sell_net_amount)
            
            for trade in trades.values():
                context.remove_events(trade.content_id)
                new_trade = msgspec.structs.replace(trade, trade_type="arbitrage")
                context.add_events({new_trade.content_id: new_trade})
            return True
        
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