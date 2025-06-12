# indexer/transform/operations.py
"""
Pure business logic operations for transforming signals into domain events.
Stateless functions that implement domain knowledge and transformation rules.
"""

from typing import Dict, Set, Optional, List
from ..types import (
    DomainEvent,
    DomainEventId,
    Signal,
    SwapSignal,
    RouteSignal,
    MultiRouteSignal,
    TransferSignal,
    LiquiditySignal,
    NfpLiquiditySignal,
    EvmAddress,
    Trade,
    Transfer,
    Liquidity,
    ZERO_ADDRESS,
)
from .patterns import TransferPattern, TransferLeg
from .context import TransformerContext


class TransformationOperations:
    """
    Stateless operations for transforming signals into domain events.
    Pure business logic with no instance state or hidden dependencies.
    """
    
    # =============================================================================
    # MAIN ORCHESTRATION FUNCTIONS
    # =============================================================================
    
    @staticmethod
    def create_events_from_signals(context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        """
        Main entry point: Transform all signals in context into domain events
        Pure function - all data from context, returns new events
        """
        events = {}
        
        # Process each domain
        events.update(TransformationOperations._process_liquidity_signals(context))
        events.update(TransformationOperations._process_trading_signals(context))
        events.update(TransformationOperations._process_staking_signals(context))
        events.update(TransformationOperations._process_farming_signals(context))
        
        return events
    
    @staticmethod
    def create_fallback_events(context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        """
        Create fallback events for unprocessed signals
        """
        events = {}
        events.update(TransformationOperations._create_unmatched_transfer_events(context))
        return events
    
    # =============================================================================
    # DOMAIN PROCESSING FUNCTIONS
    # =============================================================================
    
    @staticmethod
    def _process_liquidity_signals(context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        """Process liquidity domain signals"""
        events = {}
        liquidity_signals = context.get_signals_by_type([LiquiditySignal, NfpLiquiditySignal])
        
        for log_index, signal in liquidity_signals.items():
            if context.is_signal_consumed(log_index):
                continue
            
            # Try pattern-based creation first
            event = TransformationOperations._create_liquidity_with_pattern(signal, context)
            
            if event:
                events[event.content_id] = event
                # Note: Manager will handle marking signals as consumed
            else:
                # Fallback to standard creation
                event = TransformationOperations._create_liquidity_from_signal(signal, context)
                if event:
                    events[event.content_id] = event
        
        return events
    
    @staticmethod
    def _process_trading_signals(context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        """Process trading domain signals"""
        events = {}
        
        # Process swap signals
        swap_signals = context.get_signals_by_type(SwapSignal)
        for log_index, signal in swap_signals.items():
            if context.is_signal_consumed(log_index):
                continue
            
            event = TransformationOperations._create_trade_with_pattern(signal, context)
            if event:
                events[event.content_id] = event
            else:
                # Fallback
                event = TransformationOperations._create_trade_from_swap(signal, context)
                if event:
                    events[event.content_id] = event
        
        # Process route signals
        route_signals = context.get_signals_by_type([RouteSignal, MultiRouteSignal])
        for log_index, signal in route_signals.items():
            if context.is_signal_consumed(log_index):
                continue
            
            event = TransformationOperations._create_trade_with_pattern(signal, context)
            if event:
                events[event.content_id] = event
            else:
                # Fallback
                event = TransformationOperations._create_incomplete_trade(signal, context)
                if event:
                    events[event.content_id] = event
        
        return events
    
    @staticmethod
    def _process_staking_signals(context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        """Process staking domain signals - placeholder"""
        # TODO: Implement when staking signals are available
        return {}
    
    @staticmethod
    def _process_farming_signals(context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        """Process farming domain signals - placeholder"""
        # TODO: Implement when farming signals are available
        return {}
    
    # =============================================================================
    # PATTERN VALIDATION FUNCTIONS
    # =============================================================================
    
    @staticmethod
    def validate_signal_against_pattern(signal: Signal, pattern: TransferPattern, context: TransformerContext) -> Optional[Set[int]]:
        """
        Validate a signal against a transfer pattern.
        Returns set of matched transfer indices if valid, None if invalid.
        Pure function with no side effects.
        """
        # Extract addresses from pattern
        address_context = pattern.extract_addresses(signal, context)
        if not address_context:
            return None
        
        # Generate transfer legs
        legs = pattern.generate_transfer_legs(signal, context)
        if not legs:
            return None
        
        # Validate each leg
        all_matched_transfers = set()
        for leg in legs:
            matched_transfers = TransformationOperations._validate_transfer_leg(leg, context)
            if not TransformationOperations._is_leg_satisfied(leg, matched_transfers):
                return None  # Pattern fails if any required leg fails
            
            all_matched_transfers.update(matched_transfers.keys())
        
        return all_matched_transfers if all_matched_transfers else None
    
    @staticmethod
    def _validate_transfer_leg(leg: TransferLeg, context: TransformerContext) -> Dict[int, TransferSignal]:
        """Validate a single transfer leg and return matching transfers"""
        if not leg.from_end or not leg.to_end:
            return {}
        
        # Determine amount for filtering
        amount = leg.amount or TransformationOperations._infer_transfer_amount(leg, context)
        if not amount:
            return {}
        
        # Get matching transfers
        token_transfers = context.get_token_trfs([leg.token]).get(leg.token, {})
        
        # Direct transfers
        from_transfers = token_transfers.get("out", {}).get(leg.from_end, {})
        direct_matches = {
            idx: transfer for idx, transfer in from_transfers.items()
            if transfer.to_address == leg.to_end and transfer.amount == amount
        }
        
        if direct_matches:
            return direct_matches
        
        # TODO: Add multi-hop transfer logic here
        return {}
    
    @staticmethod
    def _infer_transfer_amount(leg: TransferLeg, context: TransformerContext) -> Optional[str]:
        """Infer transfer amount from available transfers"""
        token_transfers = context.get_token_trfs([leg.token]).get(leg.token, {})
        
        # Look for transfers from from_end
        from_transfers = token_transfers.get("out", {}).get(leg.from_end, {})
        if from_transfers:
            return next(iter(from_transfers.values())).amount
        
        # Look for transfers to to_end
        to_transfers = token_transfers.get("in", {}).get(leg.to_end, {})
        if to_transfers:
            return next(iter(to_transfers.values())).amount
        
        return None
    
    @staticmethod
    def _is_leg_satisfied(leg: TransferLeg, matched_transfers: Dict[int, TransferSignal]) -> bool:
        """Check if a leg's requirements are satisfied"""
        transfer_count = len(matched_transfers)
        
        if transfer_count < leg.min_transfers:
            return False
        
        if leg.max_transfers and transfer_count > leg.max_transfers:
            return False
        
        return True
    
    # =============================================================================
    # EVENT CREATION FUNCTIONS
    # =============================================================================
    
    @staticmethod
    def _create_liquidity_with_pattern(signal: LiquiditySignal, context: TransformerContext) -> Optional[Liquidity]:
        """Create liquidity event using pattern validation"""
        # Manager would determine which pattern to use and pass it here
        # For now, this is a placeholder that shows the structure
        
        # This would be called by manager like:
        # pattern = registry.get_pattern("liquidity_add_basic")
        # matched_transfers = TransformationOperations.validate_signal_against_pattern(signal, pattern, context)
        # if matched_transfers:
        #     return TransformationOperations.create_liquidity_event(signal, pattern, matched_transfers, context)
        
        return None  # Placeholder
    
    @staticmethod
    def create_liquidity_event(signal: LiquiditySignal, pattern: TransferPattern, matched_transfers: Set[int], context: TransformerContext) -> Liquidity:
        """
        Create liquidity event from validated components.
        Pure function - no side effects, just creates the event.
        """
        address_context = pattern.extract_addresses(signal, context)
        action = "add" if not signal.base_amount.startswith('-') else "remove"
        
        return Liquidity(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            pool=address_context.pool,
            provider=address_context.provider,
            base_token=address_context.base,
            base_amount=signal.base_amount.lstrip('-'),
            quote_token=address_context.quote,
            quote_amount=signal.quote_amount.lstrip('-'),
            action=action,
            signals={signal.log_index: signal}
        )
    
    @staticmethod
    def _create_trade_with_pattern(signal: Signal, context: TransformerContext) -> Optional[Trade]:
        """Create trade event using pattern validation - placeholder"""
        return None  # Manager will handle pattern selection and validation
    
    @staticmethod
    def create_trade_event(signal: Signal, pattern: TransferPattern, matched_transfers: Set[int], context: TransformerContext) -> Trade:
        """Create trade event from validated components"""
        # Implementation depends on signal type (SwapSignal vs RouteSignal)
        # This would be the pure business logic for creating trades
        pass  # Placeholder
    
    # =============================================================================
    # FALLBACK EVENT CREATION
    # =============================================================================
    
    @staticmethod
    def _create_liquidity_from_signal(signal: LiquiditySignal, context: TransformerContext) -> Optional[Liquidity]:
        """Fallback liquidity creation without pattern validation"""
        action = "add" if not signal.base_amount.startswith('-') else "remove"
        provider = signal.owner or signal.sender
        
        if not provider:
            return None
        
        return Liquidity(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            pool=signal.pool,
            provider=provider,
            base_token=signal.base_token,
            base_amount=signal.base_amount.lstrip('-'),
            quote_token=signal.quote_token,
            quote_amount=signal.quote_amount.lstrip('-'),
            action=action,
            signals={signal.log_index: signal}
        )
    
    @staticmethod
    def _create_trade_from_swap(signal: SwapSignal, context: TransformerContext) -> Optional[Trade]:
        """Fallback trade creation from swap signal"""
        direction = "sell" if float(signal.base_amount) > 0 else "buy"
        taker = signal.to or signal.sender
        
        if not taker:
            return None
        
        return Trade(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            taker=taker,
            direction=direction,
            base_token=signal.base_token,
            base_amount=signal.base_amount.lstrip('-'),
            quote_token=signal.quote_token,
            quote_amount=signal.quote_amount.lstrip('-'),
            signals={signal.log_index: signal}
        )
    
    @staticmethod
    def _create_incomplete_trade(signal: RouteSignal, context: TransformerContext) -> Optional[Trade]:
        """Create incomplete trade from route signal"""
        direction = "sell" if signal.token_in in context.tokens_of_interest else "buy"
        base_token = signal.token_in if signal.token_in in context.tokens_of_interest else signal.token_out
        quote_token = signal.token_out if signal.token_in in context.tokens_of_interest else signal.token_in
        taker = signal.to or signal.sender
        
        if not taker:
            return None
        
        return Trade(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            taker=taker,
            direction=direction,
            base_token=base_token,
            base_amount=signal.amount_in if direction == "sell" else signal.amount_out,
            quote_token=quote_token,
            quote_amount=signal.amount_out if direction == "sell" else signal.amount_in,
            trade_type="incomplete",
            router=signal.contract,
            signals={signal.log_index: signal}
        )
    
    @staticmethod
    def _create_unmatched_transfer_events(context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        """Create transfer events for unmatched transfer signals"""
        events = {}
        unmatched_transfers = context.get_unmatched_transfers()
        
        for transfer_signal in unmatched_transfers.values():
            # Only create events for tokens we care about
            if transfer_signal.token in context.tokens_of_interest:
                transfer_event = Transfer(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    token=transfer_signal.token,
                    from_address=transfer_signal.from_address,
                    to_address=transfer_signal.to_address,
                    amount=transfer_signal.amount,
                    signals={transfer_signal.log_index: transfer_signal}
                )
                events[transfer_event.content_id] = transfer_event
        
        return events
    
    # =============================================================================
    # PATTERN SELECTION HELPERS
    # =============================================================================
    
    @staticmethod
    def suggest_pattern_for_signal(signal: Signal, context: TransformerContext) -> Optional[str]:
        """
        Suggest appropriate pattern name for a signal.
        Pure function that encodes business rules about pattern selection.
        Manager can use this or implement its own logic.
        """
        if isinstance(signal, LiquiditySignal):
            if signal.base_amount.startswith('-') or signal.quote_amount.startswith('-'):
                return "liquidity_remove_basic"
            else:
                return "liquidity_add_basic"
        
        elif isinstance(signal, SwapSignal):
            return "direct_swap"
        
        elif isinstance(signal, RouteSignal):
            # Check for WAVAX involvement
            wavax_address = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
            wavax_transfers = context.get_token_trfs([wavax_address])
            
            has_wrap = any(
                ZERO_ADDRESS in token_data.get("in", {}) or ZERO_ADDRESS in token_data.get("out", {})
                for token_data in wavax_transfers.values()
            )
            
            return "routed_swap_with_wrap" if has_wrap else "routed_swap"
        
        return None