# indexer/transform/patterns/trading.py
"""
Clean trading pattern using route context matcher
"""

from typing import Dict, List, Optional, Tuple
from ..processors.matcher import TransferMatcher, RouteContext, PoolMatch
from ..context import TransformContext
from ...types import (
    Signal, SwapSignal, RouteSignal, PoolSwap, Trade, Position,
    EvmAddress, DomainEventId, ZERO_ADDRESS
)
from ...utils.amounts import amount_to_negative_str, is_positive


class TradePattern:
    """Clean trade pattern: match known, infer routers for unmatched transfers"""
    
    def __init__(self, indexer_tokens: set):
        self.matcher = TransferMatcher(indexer_tokens)
    
    def process_trade_signals(self, trade_signals: Dict[int, Signal], 
                             context: TransformContext) -> bool:
        """
        Process trade signals: RouteSignals + SwapSignals
        """
        
        # 1. Build route context from RouteSignals only
        route_context = self.matcher.build_route_context(trade_signals)
        
        # 2. Get swap signals
        swap_signals = [
            signal for signal in trade_signals.values()
            if isinstance(signal, SwapSignal)
        ]
        
        if not swap_signals:
            # Just route signals, mark as consumed
            self._consume_route_signals(trade_signals, context)
            return True
        
        # 3. Match pool swaps with simple 1:1 transfer matching
        available_transfers = context.get_unmatched_transfers()
        pool_matches, remaining_transfers = self.matcher.match_pool_swaps(
            swap_signals, available_transfers, route_context
        )
        
        # 4. Find router transfers (known + inferred that net to zero)
        router_transfers = {}
        inferred_routers = set()
        if route_context:
            router_transfers, inferred_routers = self.matcher.find_router_transfers(
                remaining_transfers, route_context
            )
        
        # 5. Create events
        events = self._create_trade_events(
            pool_matches, router_transfers, route_context, inferred_routers, context
        )
        
        if events:
            context.add_events(events)
            
            # 6. Mark all consumed signals and transfers
            self._mark_consumed(
                trade_signals, pool_matches, router_transfers, context
            )
            
            return True
        
        return False
    
    def _create_trade_events(self, pool_matches: List[PoolMatch],
                            router_transfers: Dict[int, Signal],
                            route_context: Optional[RouteContext],
                            inferred_routers: set,
                            context: TransformContext) -> Dict[DomainEventId, any]:
        """Create appropriate trade events"""
        
        events = {}
        
        # Create PoolSwap events for each matched pool
        pool_swaps = {}
        for pool_match in pool_matches:
            pool_swap = self._create_pool_swap(pool_match, route_context, context)
            pool_swaps[pool_swap.content_id] = pool_swap
            events[pool_swap.content_id] = pool_swap
        
        # Create Trade event if we have multiple pools or router transfers
        if len(pool_matches) > 1 or router_transfers or inferred_routers:
            trade = self._create_trade_event(
                pool_swaps, router_transfers, route_context, inferred_routers, context
            )
            events[trade.content_id] = trade
        
        return events
    

    
    def _create_trade_event(self, pool_swaps: Dict[DomainEventId, PoolSwap],
                           router_transfers: Dict[int, Signal],
                           route_context: RouteContext,
                           inferred_routers: set,
                           context: TransformContext) -> Trade:
        """Create Trade event for complex trades with multiple pools or routers"""
        
        # Generate positions from router transfers
        router_positions = {}
        for transfer in router_transfers.values():
            router_positions.update(self._generate_positions_from_transfer(transfer, context))
        
        # Determine overall trade details
        if route_context:
            base_token = list(route_context.route_tokens)[0] if route_context.route_tokens else None
            taker = route_context.taker
        else:
            # Fallback to first pool swap
            first_swap = next(iter(pool_swaps.values())) if pool_swaps else None
            base_token = first_swap.base_token if first_swap else None
            taker = first_swap.taker if first_swap else None
        
        return Trade(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            taker=taker,
            direction="trade",  # Generic for complex trades
            base_token=base_token,
            base_amount="0",  # Calculate from pool swaps if needed
            swaps=pool_swaps,
            trade_type="trade",
            metadata={
                'pool_count': len(pool_swaps),
                'router_transfer_count': len(router_transfers),
                'known_routers': route_context.routers if route_context else [],
                'inferred_routers': list(inferred_routers),
                'route_tokens': list(route_context.route_tokens) if route_context else []
            }
        )
    


    
    def _consume_route_signals(self, trade_signals: Dict[int, Signal],
                              context: TransformContext) -> None:
        """Consume route signals when no swaps to process"""
        
        route_signals = {
            idx: signal for idx, signal in trade_signals.items()
            if isinstance(signal, RouteSignal)
        }
        
        for signal_idx in route_signals.keys():
            context.mark_signal_consumed(signal_idx)


# Usage in transform manager:
"""
def _process_trade_signals(self, context: TransformContext) -> bool:
    trade_signals = {
        idx: signal for idx, signal in context.signals.items()
        if signal.pattern in ["Swap_A", "Route"]
    }
    
    if not trade_signals:
        return True
    
    trade_pattern = TradePattern(indexer_tokens=context.indexer_tokens)
    return trade_pattern.process_trade_signals(trade_signals, context)
"""