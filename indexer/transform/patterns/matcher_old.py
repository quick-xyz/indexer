# indexer/transform/patterns/trading.py
"""
Clean trading pattern using route context matcher
"""

from typing import Dict, List, Optional, Tuple
from .matcher import TransferMatcher, RouteContext, PoolMatch
from ..context import TransformContext
from ...types import (
    Signal, SwapSignal, RouteSignal, PoolSwap, Trade, Position,
    EvmAddress, DomainEventId
)
from ...utils.amounts import amount_to_negative_str, is_positive


class TradePattern:
    def __init__(self, indexer_tokens: set):
        self.matcher = TransferMatcher(indexer_tokens)
    
    def process_trade_signals(self, trade_signals: Dict[int, Signal], 
                             context: TransformContext) -> bool:

        # 1. Build route context from route signals only
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
        
        # 4. Find router transfers (known + inferred) that zero out router balances
        router_transfers = {}
        inferred_routers = set()
        if route_context:
            router_transfers, inferred_routers = self.matcher.find_router_zero_transfers(
                remaining_transfers, route_context
            )
        
        # 5. Create events based on what we matched
        events = self._create_trade_events(
            pool_matches, router_transfers, route_context, context
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
                            context: TransformContext) -> Dict[DomainEventId, any]:
        """Create trade events from matched transfers"""
        
        events = {}
        
        # Create PoolSwap events
        pool_swaps = {}
        for pool_match in pool_matches:
            pool_swap = self._create_pool_swap(pool_match, route_context, context)
            pool_swaps[pool_swap.content_id] = pool_swap
            events[pool_swap.content_id] = pool_swap
        
        # If we have multiple pools or router transfers, create Trade event
        if len(pool_matches) > 1 or router_transfers:
            trade = self._create_trade_event(
                pool_swaps, router_transfers, route_context, context
            )
            events[trade.content_id] = trade
        
        return events
    
    def _create_pool_swap(self, pool_match: PoolMatch, 
                         route_context: Optional[RouteContext],
                         context: TransformContext) -> PoolSwap:
        """Create PoolSwap event from pool match"""
        
        # Generate positions from the two matched transfers
        positions = {}
        
        # Position from transfer_in
        positions.update(self._generate_positions_from_transfer(
            pool_match.transfer_in, context
        ))
        
        # Position from transfer_out  
        positions.update(self._generate_positions_from_transfer(
            pool_match.transfer_out, context
        ))
        
        # Determine trade direction and amounts
        transfer_in_amount = int(pool_match.transfer_in.amount)
        transfer_out_amount = int(pool_match.transfer_out.amount)
        
        # Figure out which is base and which is quote
        if pool_match.token_in == pool_match.transfer_in.token:
            base_amount = str(transfer_in_amount)
            quote_amount = str(-transfer_out_amount)  # Negative because it's outgoing
            direction = "sell"  # Selling base for quote
        else:
            base_amount = str(-transfer_out_amount)  # Negative because it's outgoing  
            quote_amount = str(transfer_in_amount)
            direction = "buy"   # Buying base with quote
        
        # Use route context for taker if available
        taker = route_context.taker if route_context else pool_match.transfer_in.from_address
        
        return PoolSwap(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            pool=pool_match.pool,
            taker=taker,
            direction=direction,
            base_token=pool_match.token_in,  # Adjust as needed
            base_amount=base_amount,
            quote_token=pool_match.token_out,
            quote_amount=quote_amount,
            positions=positions,
            signals={
                pool_match.transfer_in.log_index: pool_match.transfer_in,
                pool_match.transfer_out.log_index: pool_match.transfer_out
            }
        )
    
    def _create_trade_event(self, pool_swaps: Dict[DomainEventId, PoolSwap],
                           router_transfers: Dict[int, Signal],
                           route_context: RouteContext,
                           context: TransformContext) -> Trade:
        """Create Trade event encompassing multiple pools and router transfers"""
        
        # Generate positions from router transfers
        router_positions = {}
        for transfer in router_transfers.values():
            router_positions.update(self._generate_positions_from_transfer(
                transfer, context
            ))
        
        # Determine overall trade direction and amounts
        # Use route context if available, otherwise infer from pool swaps
        if route_context and route_context.route_tokens:
            direction, base_token, base_amount = self._infer_trade_direction_from_route(
                route_context, context
            )
        else:
            direction, base_token, base_amount = self._infer_trade_direction_from_pools(
                pool_swaps
            )
        
        return Trade(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            taker=route_context.taker,
            direction=direction,
            base_token=base_token,
            base_amount=base_amount,
            swaps=pool_swaps,
            router_positions=router_positions,
            trade_type="trade",  # Could be "arbitrage" based on analysis
            route_context={
                'routers': route_context.routers,
                'route_tokens': list(route_context.route_tokens)
            } if route_context else None
        )
    
    def _generate_positions_from_transfer(self, transfer: Signal, 
                                         context: TransformContext) -> Dict[DomainEventId, Position]:
        """Generate positions from a transfer"""
        positions = {}
        
        # Position for recipient (positive)
        if transfer.to_address != "0x0000000000000000000000000000000000000000":
            position_in = Position(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                user=transfer.to_address,
                token=transfer.token,
                amount=transfer.amount,
            )
            positions[position_in.content_id] = position_in
        
        # Position for sender (negative)
        if transfer.from_address != "0x0000000000000000000000000000000000000000":
            position_out = Position(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                user=transfer.from_address,
                token=transfer.token,
                amount=amount_to_negative_str(transfer.amount),
            )
            positions[position_out.content_id] = position_out
        
        return positions
    
    def _infer_trade_direction_from_route(self, route_context: RouteContext,
                                         context: TransformContext) -> Tuple[str, EvmAddress, str]:
        """Infer trade direction from route context"""
        
        # Find taker's net position changes
        taker_transfers = self.matcher.get_taker_transfers(
            context.get_unmatched_transfers(), route_context
        )
        
        # Calculate net changes for taker
        net_changes = {}
        for transfer in taker_transfers.values():
            token = transfer.token
            amount = int(transfer.amount)
            
            if token not in net_changes:
                net_changes[token] = 0
            
            if transfer.to_address == route_context.taker:
                net_changes[token] += amount
            else:
                net_changes[token] -= amount
        
        # Find which token taker spent (negative) and received (positive)
        spent_token = None
        received_token = None
        spent_amount = 0
        
        for token, net_change in net_changes.items():
            if net_change < 0:
                spent_token = token
                spent_amount = abs(net_change)
            elif net_change > 0:
                received_token = token
        
        if spent_token and received_token:
            return "sell", spent_token, str(spent_amount)
        else:
            # Fallback to first pool swap direction
            return "trade", list(net_changes.keys())[0], "0"
    
    def _infer_trade_direction_from_pools(self, pool_swaps: Dict[DomainEventId, PoolSwap]) -> Tuple[str, EvmAddress, str]:
        """Infer trade direction from pool swaps"""
        
        if not pool_swaps:
            return "trade", "0x0000000000000000000000000000000000000000", "0"
        
        first_swap = next(iter(pool_swaps.values()))
        return first_swap.direction, first_swap.base_token, first_swap.base_amount
    
    def _mark_consumed(self, trade_signals: Dict[int, Signal],
                      pool_matches: List[PoolMatch],
                      router_transfers: Dict[int, Signal],
                      context: TransformContext) -> None:
        """Mark all consumed signals and transfers"""
        
        consumed_signals = {}
        
        # Add all trade signals
        consumed_signals.update(trade_signals)
        
        # Add transfers from pool matches
        for pool_match in pool_matches:
            consumed_signals[pool_match.transfer_in.log_index] = pool_match.transfer_in
            consumed_signals[pool_match.transfer_out.log_index] = pool_match.transfer_out
        
        # Add router transfers
        consumed_signals.update(router_transfers)
        
        context.match_all_signals(consumed_signals)
    
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
# Replace the existing trade processing logic with:

def _process_trade_signals(self, context: TransformContext) -> bool:
    trade_signals = {
        idx: signal for idx, signal in context.signals.items()
        if signal.pattern in ["Swap_A", "Route"]
    }
    
    if not trade_signals:
        return True
    
    trade_pattern = TradePattern(
        indexer_tokens=context.indexer_tokens
    )
    
    return trade_pattern.process_trade_signals(trade_signals, context)
"""