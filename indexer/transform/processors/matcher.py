# indexer/transform/patterns/matcher.py
"""
Clean Route Context Transfer Matcher
Match what we know, infer routers only for unmatched transfers that net to zero
"""

from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict

from ...types import (
    Signal, TransferSignal, SwapSignal, RouteSignal, 
    EvmAddress, ZERO_ADDRESS
)
from ...utils.amounts import amount_to_int, amount_to_str


@dataclass
class RouteContext:
    """Route context identifying real taker and known routers"""
    taker: EvmAddress
    routers: List[EvmAddress]  # Known routers from RouteSignals
    top_level_router: EvmAddress  # Highest log index router (user's entry point)
    route_tokens: Set[EvmAddress]  # All tokens involved in routing


@dataclass
class PoolMatch:
    """Simple pool match: one transfer in, one transfer out"""
    pool: EvmAddress
    token_in: EvmAddress
    transfer_in: TransferSignal
    token_out: EvmAddress  
    transfer_out: TransferSignal


class TransferMatcher:
    """Clean transfer matcher: match known, infer routers from unmatched transfers"""
    
    def __init__(self, indexer_tokens: Set[EvmAddress]):
        self.indexer_tokens = indexer_tokens
    
    def build_route_context(self, trade_signals: Dict[int, Signal]) -> Optional[RouteContext]:
        """Build route context from RouteSignals only"""
        
        route_signals = [
            signal for signal in trade_signals.values() 
            if isinstance(signal, RouteSignal)
        ]
        
        if not route_signals:
            return None
        
        # Find top-level router (highest log index)
        top_route = max(route_signals, key=lambda s: s.log_index)
        
        # Collect known routers and tokens from RouteSignals
        known_routers = set()
        route_tokens = set()
        
        for route_signal in route_signals:
            known_routers.add(route_signal.contract)
            if route_signal.token_in:
                route_tokens.add(route_signal.token_in)
            if route_signal.token_out:
                route_tokens.add(route_signal.token_out)
        
        return RouteContext(
            taker=top_route.sender,
            routers=sorted(list(known_routers)),
            top_level_router=top_route.contract,
            route_tokens=route_tokens
        )
    
    def match_pool_swaps(self, swap_signals: List[SwapSignal], 
                        available_transfers: Dict[int, TransferSignal],
                        route_context: Optional[RouteContext]) -> Tuple[List[PoolMatch], Dict[int, TransferSignal]]:
        """
        Match pool swaps with simple 1:1 transfer matching
        Returns: (pool_matches, remaining_transfers)
        """
        
        pool_matches = []
        remaining_transfers = available_transfers.copy()
        
        for swap_signal in swap_signals:
            pool_match = self._match_single_pool_swap(
                swap_signal, remaining_transfers, route_context
            )
            
            if pool_match:
                pool_matches.append(pool_match)
                # Remove matched transfers
                remaining_transfers.pop(pool_match.transfer_in.log_index, None)
                remaining_transfers.pop(pool_match.transfer_out.log_index, None)
        
        return pool_matches, remaining_transfers
    
    def _match_single_pool_swap(self, swap_signal: SwapSignal,
                               available_transfers: Dict[int, TransferSignal],
                               route_context: Optional[RouteContext]) -> Optional[PoolMatch]:
        """Match transfers for a single pool swap (1 in, 1 out)"""
        
        # Determine expected tokens based on swap direction
        buy_trade = amount_to_int(swap_signal.base_amount) > 0
        
        if buy_trade:
            # Buy: quote_token goes TO pool, base_token comes FROM pool
            token_to_pool = swap_signal.quote_token
            token_from_pool = swap_signal.base_token
        else:
            # Sell: base_token goes TO pool, quote_token comes FROM pool  
            token_to_pool = swap_signal.base_token
            token_from_pool = swap_signal.quote_token
        
        # Find transfer TO pool
        transfer_in = self._find_transfer_to_pool(
            token_to_pool, swap_signal.pool, available_transfers
        )
        
        # Find transfer FROM pool
        transfer_out = self._find_transfer_from_pool(
            token_from_pool, swap_signal.pool, available_transfers
        )
        
        if transfer_in and transfer_out:
            return PoolMatch(
                pool=swap_signal.pool,
                token_in=token_to_pool,
                transfer_in=transfer_in,
                token_out=token_from_pool,
                transfer_out=transfer_out
            )
        
        return None
    
    def _find_transfer_to_pool(self, token: EvmAddress, pool: EvmAddress,
                              transfers: Dict[int, TransferSignal]) -> Optional[TransferSignal]:
        """Find any transfer of token going TO the pool"""
        
        for transfer in transfers.values():
            if transfer.token == token and transfer.to_address == pool:
                return transfer
        
        return None
    
    def _find_transfer_from_pool(self, token: EvmAddress, pool: EvmAddress,
                                transfers: Dict[int, TransferSignal]) -> Optional[TransferSignal]:
        """Find any transfer of token coming FROM the pool"""
        
        for transfer in transfers.values():
            if transfer.token == token and transfer.from_address == pool:
                return transfer
        
        return None
    
    def find_router_transfers(self, remaining_transfers: Dict[int, TransferSignal],
                             route_context: RouteContext) -> Tuple[Dict[int, TransferSignal], Set[EvmAddress]]:
        """
        Find router transfers: known routers + inferred routers (addresses that net to zero)
        Returns: (all_router_transfers, inferred_routers)
        """
        
        # Step 1: Get transfers involving known routers
        known_router_transfers = self._get_known_router_transfers(remaining_transfers, route_context)
        
        # Step 2: Remove known router transfers to see what's left
        unmatched_transfers = {
            idx: transfer for idx, transfer in remaining_transfers.items()
            if idx not in known_router_transfers
        }
        
        # Step 3: Infer routers from remaining transfers (addresses that net to zero)
        inferred_routers, inferred_router_transfers = self._infer_routers_from_unmatched(
            unmatched_transfers, route_context
        )
        
        # Step 4: Combine all router transfers
        all_router_transfers = {**known_router_transfers, **inferred_router_transfers}
        
        return all_router_transfers, inferred_routers
    
    def _get_known_router_transfers(self, transfers: Dict[int, TransferSignal],
                                   route_context: RouteContext) -> Dict[int, TransferSignal]:
        """Get transfers involving known routers from RouteSignals"""
        
        router_transfers = {}
        
        for transfer_id, transfer in transfers.items():
            if (transfer.from_address in route_context.routers or
                transfer.to_address in route_context.routers):
                router_transfers[transfer_id] = transfer
        
        return router_transfers
    
    def _infer_routers_from_unmatched(self, unmatched_transfers: Dict[int, TransferSignal],
                                     route_context: RouteContext) -> Tuple[Set[EvmAddress], Dict[int, TransferSignal]]:
        """
        Infer routers from unmatched transfers: addresses that net to exactly zero
        Returns: (inferred_routers, their_transfers)
        """
        
        if not unmatched_transfers:
            return set(), {}
        
        # Calculate net balance for each address
        address_balances = defaultdict(lambda: defaultdict(int))  # {address: {token: net_amount}}
        
        for transfer in unmatched_transfers.values():
            token = transfer.token
            amount = amount_to_int(transfer.amount)
            
            # Credit recipient
            if transfer.to_address != ZERO_ADDRESS:
                address_balances[transfer.to_address][token] += amount
            
            # Debit sender
            if transfer.from_address != ZERO_ADDRESS:
                address_balances[transfer.from_address][token] -= amount
        
        # Find addresses that net to exactly zero across all tokens
        inferred_routers = set()
        for address, token_balances in address_balances.items():
            # Skip taker and known routers
            if address == route_context.taker or address in route_context.routers:
                continue
            
            # Check if ALL token balances are exactly zero
            if token_balances and all(net_balance == 0 for net_balance in token_balances.values()):
                inferred_routers.add(address)
        
        # Get transfers involving inferred routers
        inferred_router_transfers = {}
        for transfer_id, transfer in unmatched_transfers.items():
            if (transfer.from_address in inferred_routers or 
                transfer.to_address in inferred_routers):
                inferred_router_transfers[transfer_id] = transfer
        
        return inferred_routers, inferred_router_transfers
    
    def validate_router_balances(self, router_transfers: Dict[int, TransferSignal],
                                routers: List[EvmAddress]) -> bool:
        """
        Validate that all routers have zero net balance
        Returns True if all routers are balanced
        """
        
        router_balances = defaultdict(lambda: defaultdict(int))
        
        for transfer in router_transfers.values():
            token = transfer.token
            amount = amount_to_int(transfer.amount)
            
            # Credit recipient
            if transfer.to_address in routers:
                router_balances[transfer.to_address][token] += amount
            
            # Debit sender
            if transfer.from_address in routers:
                router_balances[transfer.from_address][token] -= amount
        
        # Check all routers have zero net balance
        for router, token_balances in router_balances.items():
            for token, net_balance in token_balances.items():
                if net_balance != 0:
                    return False
        
        return True
    
    def get_taker_transfers(self, transfers: Dict[int, TransferSignal],
                           route_context: RouteContext) -> Dict[int, TransferSignal]:
        """Get transfers directly involving the taker"""
        
        taker_transfers = {}
        
        for transfer_id, transfer in transfers.items():
            if (transfer.from_address == route_context.taker or
                transfer.to_address == route_context.taker):
                taker_transfers[transfer_id] = transfer
        
        return taker_transfers