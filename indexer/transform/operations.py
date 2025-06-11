# indexer/transform/operations.py

from typing import Dict, List, Set, Optional
from collections import defaultdict

from .context import TransformerContext
from ..core.config import IndexerConfig
from ..types import (
    ZERO_ADDRESS,
    DomainEvent,
    DomainEventId,
    Signal,
    SwapSignal,
    RouteSignal,
    MultiRouteSignal,
    TransferSignal,
    LiquiditySignal,
    NfpLiquiditySignal,
    NfpCollectSignal,
    EvmAddress,
    Trade,
    PoolSwap,
    Transfer,
    Liquidity,
)
from ..utils import safe_nested_get
from .patterns import (
    TransferPattern,
    LiquidityAddBasic
)

class TransformationOperations:
    def __init__(self, config: IndexerConfig):
        self.config = config
        self.patterns: Dict[str, TransferPattern] = {}
        self._register_patterns()

    def _register_patterns(self):
        self.patterns.update({
            "liquidity_add_basic": LiquidityAddBasic(),
        })

    def _get_pattern(self, name: str) -> Optional[TransferPattern]:
        return self.patterns.get(name)

    def create_context(self, transaction) -> TransformerContext:
        return TransformerContext(transaction, self.config)

    def create_events_from_signals(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        
        events.update(self._process_liquidity_domain(context))
        events.update(self._process_trading_domain(context))
        events.update(self._process_staking_domain(context))
        events.update(self._process_farming_domain(context))
        
        return events
    
    def create_fallback_events(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        
        incomplete_trade_events = self._create_incomplete_trade_events(context)
        events.update(incomplete_trade_events)
        
        unknown_transfer_events = self._create_unknown_transfer_events(context)
        events.update(unknown_transfer_events)
        
        return events

    # =============================================================================
    # LIQUIDITY DOMAIN PROCESSING
    # =============================================================================

    def _process_liquidity_domain(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        liquidity_signals = context.get_signals_by_type([LiquiditySignal, NfpLiquiditySignal])

        for log_index, signal in liquidity_signals.items():
            if context.is_signal_consumed(log_index):
                continue
            
            pattern_name = self._select_liquidity_pattern(signal, context)
            
            if pattern_name:
                pattern = self.pattern_registry.get_pattern(pattern_name)
                match_result = self.pattern_matcher.match_signal_to_pattern(signal, pattern, context)
                
                if match_result and match_result.success:
                    # Create domain event from pattern match
                    event = self._create_liquidity_from_pattern_match(signal, match_result, context)
                    if event:
                        events[event.content_id] = event
                        # Mark matched transfers as consumed
                        for transfer_idx in match_result.matched_transfers:
                            context.mark_signal_consumed(transfer_idx)
                        context.mark_signal_consumed(log_index)
                        continue
            
            event = self._create_liquidity_from_signal(signal, context)
            if event:
                events[event.content_id] = event
                context.mark_signal_consumed(log_index)
        
        events.update(self._apply_liquidity_rules(context))

        return events


    def _select_liquidity_pattern(self, signal: LiquiditySignal, context: TransformerContext) -> Optional[str]:
        if isinstance(signal, LiquiditySignal):
            if signal.action == "remove":
                return "liquidity_remove_basic"
            elif:
                return "liquidity_add_basic"
            else:
                return None
                    
        if isinstance(signal, NfpLiquiditySignal):
            if signal.action == "remove":
                return "liquidity_remove_cl"
            elif:
                return "liquidity_add_cl"
            else:
                return None





    # =============================================================================
    # EVENT CREATION
    # =============================================================================

    def _create_event_from_validated_pattern(self, signal: Signal, address_context: Any, matched_transfers: Set[int], transfer_data: Dict[str, Any], context: TransformerContext) -> Optional[DomainEvent]:
        """Create domain event from validated pattern match"""
        
        if isinstance(signal, LiquiditySignal):
            return self._create_liquidity_from_pattern_match(signal, address_context, matched_transfers, transfer_data, context)
        elif isinstance(signal, SwapSignal):
            return self._create_trade_from_swap_pattern(signal, address_context, matched_transfers, transfer_data, context)
        elif isinstance(signal, RouteSignal):
            return self._create_trade_from_route_pattern(signal, address_context, matched_transfers, transfer_data, context)
        
        return None

    def _create_liquidity_from_pattern_match(self, signal: LiquiditySignal, address_context: Any, matched_transfers: Set[int], transfer_data: Dict[str, Any], context: TransformerContext) -> Optional[Liquidity]:
        """Create liquidity event from pattern match"""
        
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

    def _create_trade_from_swap_pattern(self, signal: SwapSignal, address_context: Any, matched_transfers: Set[int], transfer_data: Dict[str, Any], context: TransformerContext) -> Optional[Trade]:
        """Create trade event from swap pattern match"""
        
        direction = "sell" if float(signal.base_amount) > 0 else "buy"
        
        return Trade(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            taker=address_context.taker,
            direction=direction,
            base_token=signal.base_token,
            base_amount=signal.base_amount.lstrip('-'),
            quote_token=signal.quote_token,
            quote_amount=signal.quote_amount.lstrip('-'),
            signals={signal.log_index: signal}
        )

    def _create_trade_from_route_pattern(self, signal: RouteSignal, address_context: Any, matched_transfers: Set[int], transfer_data: Dict[str, Any], context: TransformerContext) -> Optional[Trade]:
        """Create trade event from route pattern match"""
        
        # Determine direction based on tokens of interest
        direction = "sell" if signal.token_in in self.config.tokens else "buy"
        base_token = signal.token_in if signal.token_in in self.config.tokens else signal.token_out
        quote_token = signal.token_out if signal.token_in in self.config.tokens else signal.token_in
        
        return Trade(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            taker=address_context.taker,
            direction=direction,
            base_token=base_token,
            base_amount=signal.amount_in if direction == "sell" else signal.amount_out,
            quote_token=quote_token,
            quote_amount=signal.amount_out if direction == "sell" else signal.amount_in,
            router=signal.contract,
            signals={signal.log_index: signal}
        )


    # =============================================================================
    # OLD
    # =============================================================================

    def _create_liquidity_from_pattern_match(self, signal: LiquiditySignal, match_result, context: TransformerContext) -> Optional[Liquidity]:
        return Liquidity(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            pool=signal.pool,
            provider=match_result.context_data["provider"],
            base_token=signal.base_token,
            base_amount=signal.base_amount.lstrip('-'),
            quote_token=signal.quote_token,
            quote_amount=signal.quote_amount.lstrip('-'),
            action="add" if not signal.base_amount.startswith('-') else "remove",
            signals={signal.log_index: signal}
        )

    def _create_liquidity_events(self, signal: Signal, context: TransformerContext) -> Optional[Dict[DomainEventId,DomainEvent]]:
        liq_transfer_set = set()

        if isinstance(signal, LiquiditySignal):
            liq_trf = context.get_token_trfs([signal.base_token, signal.quote_token, signal.pool])

            if signal.action == "add":
                ''' Expect 1 base amount from provider, both amounts into pool, receipt to provider, receipt from zero'''

                mint_trf = liq_trf.get(signal.pool).get("in")
                
                if len(mint_trf.keys()) == 1:
                    provider = mint_trf.keys()[0]
                elif signal.owner and signal.owner in mint_trf.keys():
                    provider = signal.owner
                elif signal.sender and signal.sender in mint_trf.keys():
                    provider = signal.sender

                try:
                    receipt_in = liq_trf[signal.pool]["in"][provider].keys()
                    receipt_out = liq_trf[signal.pool]["out"][ZERO_ADDRESS].keys()
                    base_in = liq_trf[signal.base_token]["in"][signal.pool].keys()
                    base_out = liq_trf[signal.base_token]["out"][provider].keys()
                    quote_in = liq_trf[signal.quote_token]["in"][signal.pool].keys()
                    quote_out = liq_trf[signal.quote_token]["out"][provider].keys()
                except KeyError:
                    receipt_in = None  # or {}
                

            if signal.action == "remove":           


    def _get_expected_mint_transfers(dict) -> List[int]:


    def _build_liquidity_expecations(self, signal: Signal, context: TransformerContext) -> Dict[str, str]:
        # apply pool details to context general transfer methods
        expectations = {}
        
        # token: {in/out: {address: {idx: TransferSignal}}}
        # trf_dict[transfer.token]["out"][transfer.from_address][idx] = transfer

        if isinstance(signal, LiquiditySignal):
            expectations["base_token"] = signal.base_token
            expectations["quote_token"] = signal.quote_token
            expectations["base_amount"] = signal.base_amount.lstrip('-')
            expectations["quote_amount"] = signal.quote_amount.lstrip('-')
        
        elif isinstance(signal, NfpLiquiditySignal):
            expectations["base_token"] = signal.base_token
            expectations["quote_token"] = signal.quote_token
            expectations["base_amount"] = signal.base_amount.lstrip('-')
            expectations["quote_amount"] = signal.quote_amount.lstrip('-')
        
        return expectations

    def _create_liquidity_from_signal(self, signal: LiquiditySignal, context: TransformerContext) -> Optional[Liquidity]:
        action = "add" if not signal.base_amount.startswith('-') else "remove"
        
        liquidity = Liquidity(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            pool=signal.pool,
            provider=signal.sender or signal.owner,
            base_token=signal.base_token,
            base_amount=signal.base_amount.lstrip('-'),
            quote_token=signal.quote_token,
            quote_amount=signal.quote_amount.lstrip('-'),
            action=action,
            signals={signal.log_index: signal}
        )
        return liquidity









    def _process_trading_domain(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        
        route_context = self._collect_route_context(context)
        
        events.update(self._process_standard_trades(context, route_context))
        
        events.update(self._apply_trading_rules(context, route_context))
        
        return events

    def _process_staking_domain(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        # TODO: Implement staking signal processing
        # staking_signals = context.get_signals_by_type(StakingSignal)
        
        # Apply staking-specific rules
        events.update(self._apply_staking_rules(context))
        
        return events

    def _process_farming_domain(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        # TODO: Implement farming signal processing
        # farming_signals = context.get_signals_by_type(FarmingSignal)
        
        # Apply farming-specific rules
        events.update(self._apply_farming_rules(context))
        
        return events

    """ DOMAIN RULES """
    def _apply_liquidity_rules(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        
        # Rule: Detect liquidity transfers without corresponding mint/burn signals
        # Rule: Handle batch liquidity operations
        # TODO: Implement specific liquidity rules
        
        return events

    def _apply_trading_rules(self, context: TransformerContext, route_context: Dict) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        
        events.update(self._detect_incomplete_trades(context, route_context))
        events.update(self._detect_multi_hop_trades(context))
        events.update(self._detect_arbitrage_trades(context))
        
        return events

    def _apply_staking_rules(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        # TODO: Implement staking rules
        return events

    def _apply_farming_rules(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        # TODO: Implement farming rules
        return events

    """ TRADE LOGIC """
    def _collect_route_context(self, context: TransformerContext) -> Dict[str, any]:
        route_context = {
            "routes": {},
            "multi_routes": {},
            "expected_tokens": set(),
            "expected_amounts": {}
        }
        
        route_signals = context.get_signals_by_type(RouteSignal)
        for log_index, signal in route_signals.items():
            route_context["routes"][log_index] = signal
            route_context["expected_tokens"].add(signal.token_in)
            route_context["expected_tokens"].add(signal.token_out)
        
        multi_route_signals = context.get_signals_by_type(MultiRouteSignal)
        for log_index, signal in multi_route_signals.items():
            route_context["multi_routes"][log_index] = signal
            route_context["expected_tokens"].update(signal.tokens_in)
            route_context["expected_tokens"].update(signal.tokens_out)
        
        return route_context

    def _process_standard_trades(self, context: TransformerContext, route_context: Dict) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        swap_signals = context.get_signals_by_type(SwapSignal)
        
        for log_index, signal in swap_signals.items():
            if context.is_signal_consumed(log_index):
                continue
                
            matching_route = self._find_matching_route(signal, route_context)
            
            event = self._create_trade_from_swap(signal, context, matching_route)
            if event:
                events[event.content_id] = event
                context.mark_signal_consumed(log_index)
        
        return events

    def _find_matching_route(self, swap_signal: SwapSignal, route_context: Dict) -> Optional[Signal]:
        for route_signal in route_context["routes"].values():
            if (swap_signal.base_token in [route_signal.token_in, route_signal.token_out] and
                swap_signal.quote_token in [route_signal.token_in, route_signal.token_out]):
                return route_signal
        
        for multi_route_signal in route_context["multi_routes"].values():
            if (swap_signal.base_token in multi_route_signal.tokens_in + multi_route_signal.tokens_out and
                swap_signal.quote_token in multi_route_signal.tokens_in + multi_route_signal.tokens_out):
                return multi_route_signal
        
        return None
    
    def _create_trade_from_swap(self, signal: SwapSignal, context: TransformerContext, matching_route: Optional[Signal] = None) -> Optional[Trade]:
        trade = Trade(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            taker=signal.to or signal.sender,
            direction="sell" if signal.base_token in self.config.tokens else "buy",
            base_token=signal.base_token,
            base_amount=signal.base_amount.lstrip('-'),
            quote_token=signal.quote_token,
            quote_amount=signal.quote_amount.lstrip('-'),
            signals={signal.log_index: signal}
        )
        
        if matching_route:
            if hasattr(matching_route, 'contract'):
                trade.router = matching_route.contract
            trade.signals[matching_route.log_index] = matching_route
        
        return trade


    def _detect_incomplete_trades(self, context: TransformerContext, route_context: Dict) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        
        for log_index, route_signal in route_context["routes"].items():
            if context.is_signal_consumed(log_index):
                continue
                
            matching_swaps = self._find_swaps_for_route(route_signal, context)
            
            if not matching_swaps:
                incomplete_trade = Trade(
                    timestamp=context.transaction.timestamp,
                    tx_hash=context.transaction.tx_hash,
                    taker=route_signal.to or route_signal.sender,
                    direction=self._determine_trade_direction(route_signal.token_in, route_signal.token_out),
                    base_token=self._get_base_token(route_signal.token_in, route_signal.token_out),
                    base_amount=self._get_base_amount(route_signal.token_in, route_signal.token_out, route_signal.amount_in, route_signal.amount_out),
                    quote_token=self._get_quote_token(route_signal.token_in, route_signal.token_out),
                    quote_amount=self._get_quote_amount(route_signal.token_in, route_signal.token_out, route_signal.amount_in, route_signal.amount_out),
                    trade_type="incomplete",
                    router=route_signal.contract,
                    signals={route_signal.log_index: route_signal}
                )
                
                events[incomplete_trade.content_id] = incomplete_trade
                context.mark_signal_consumed(log_index)
        
        return events

    def _detect_multi_hop_trades(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        # TODO: Implement multi-hop detection logic
        return events

    def _detect_arbitrage_trades(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        # TODO: Implement arbitrage detection logic
        return events

    def _find_swaps_for_route(self, route_signal: RouteSignal, context: TransformerContext) -> List[SwapSignal]:
        matching_swaps = []
        swap_signals = context.get_signals_by_type(SwapSignal)
        
        for swap_signal in swap_signals.values():
            if (swap_signal.base_token in [route_signal.token_in, route_signal.token_out] and
                swap_signal.quote_token in [route_signal.token_in, route_signal.token_out]):
                matching_swaps.append(swap_signal)
        
        return matching_swaps

    """ LIQUIDITY LOGIC """



    """ TRANSFER LOGIC """
    def _create_transfer_from_signal(self, signal: TransferSignal, context: TransformerContext) -> Optional[Transfer]:
        transfer = Transfer(
            timestamp=context.transaction.timestamp,
            tx_hash=context.transaction.tx_hash,
            token=signal.token,
            from_address=signal.from_address,
            to_address=signal.to_address,
            amount=signal.amount,
            signals={signal.log_index: signal}
        )
        return transfer

    def _create_incomplete_trade_events(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        # Implementation depends on specific reconciliation requirements
        return events

    def _create_unknown_transfer_events(self, context: TransformerContext) -> Dict[DomainEventId, DomainEvent]:
        events = {}
        unexplained_transfers = context.get_unexplained_transfers()
        
        for transfer_signal in unexplained_transfers:
            address_type = self._classify_address(transfer_signal.from_address, transfer_signal.to_address)
            
            transfer = Transfer(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                token=transfer_signal.token,
                from_address=transfer_signal.from_address,
                to_address=transfer_signal.to_address,
                amount=transfer_signal.amount,
                signals={transfer_signal.log_index: transfer_signal}
            )
            
            events[transfer.content_id] = transfer
            context.mark_transfer_explained(transfer_signal)
        
        return events

    def _classify_address(self, from_addr: EvmAddress, to_addr: EvmAddress) -> str:
        if from_addr in self.config.addresses:
            return f"from_{self.config.addresses[from_addr].name}"
        if to_addr in self.config.addresses:
            return f"to_{self.config.addresses[to_addr].name}"
        return "unknown"

    def _determine_trade_direction(self, token_in: EvmAddress, token_out: EvmAddress) -> str:
        if token_in in self.config.tokens:
            return "sell"
        return "buy"

    def _get_base_token(self, token_in: EvmAddress, token_out: EvmAddress) -> EvmAddress:
        if token_in in self.config.tokens:
            return token_in
        return token_out

    def _get_quote_token(self, token_in: EvmAddress, token_out: EvmAddress) -> EvmAddress:
        if token_in in self.config.tokens:
            return token_out
        return token_in

    def _get_base_amount(self, token_in: EvmAddress, token_out: EvmAddress, amount_in: str, amount_out: str) -> str:
        if token_in in self.config.tokens:
            return amount_in
        return amount_out

    def _get_quote_amount(self, token_in: EvmAddress, token_out: EvmAddress, amount_in: str, amount_out: str) -> str:
        if token_in in self.config.tokens:
            return amount_out
        return amount_in