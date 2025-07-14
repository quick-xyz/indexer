# indexer/transform/patterns/liquidity.py

from typing import Dict

from ...types import LiquiditySignal, ZERO_ADDRESS, Liquidity, Reward, LiquiditySignal, DomainEventId
from .base import TransferPattern
from ..context import TransformContext
from ...utils.amounts import amount_to_int, abs_amount


class Mint_A(TransferPattern):    
    def __init__(self):
        super().__init__("Mint_A")
    
    def produce_events(self, signals: Dict[int, LiquiditySignal], context: TransformContext) -> Dict[DomainEventId, Liquidity|Reward]:
        """Process mint signals to create liquidity and reward events"""
        if not self._validate_signal_data(signals, context):
            return {}
        
        self._log_pattern_start(signals, context)
        events = {}
        
        try:
            for signal_idx, signal in signals.items():
                try:
                    self.log_debug("Processing mint signal",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  signal_index=signal_idx,
                                  pool=signal.pool,
                                  base_amount=signal.base_amount,
                                  quote_amount=signal.quote_amount)

                    # Validate signal data
                    if not self._validate_mint_signal(signal, context):
                        continue

                    # Get transfer data
                    pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
                    receipts_in, receipts_out = context.get_unmatched_token_transfers(signal.pool)

                    self.log_debug("Retrieved transfer data",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  pool_in_tokens=len(pool_in),
                                  pool_out_tokens=len(pool_out),
                                  receipts_in_addresses=len(receipts_in),
                                  receipts_out_addresses=len(receipts_out))

                    # Find matching transfers
                    base_trf = pool_in.get(signal.base_token, {})
                    quote_trf = pool_in.get(signal.quote_token, {})

                    base_match = {
                        idx: transfer for idx, transfer in base_trf.items() 
                        if transfer.amount == abs_amount(signal.base_amount)
                    }
                    quote_match = {
                        idx: transfer for idx, transfer in quote_trf.items() 
                        if transfer.amount == abs_amount(signal.quote_amount)
                    }
                    
                    self.log_debug("Transfer matching results",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  base_transfers=len(base_trf),
                                  quote_transfers=len(quote_trf),
                                  base_matches=len(base_match),
                                  quote_matches=len(quote_match))
                    
                    if len(base_match) != 1 or len(quote_match) != 1:
                        self._log_pattern_failure("Transfer matching failed - expected exactly 1 match per token",
                                                 context,
                                                 base_matches=len(base_match),
                                                 quote_matches=len(quote_match))
                        continue

                    # Find provider
                    provider = self._find_mint_provider(signal, base_match, quote_match, receipts_in, context)
                    if not provider:
                        self._log_pattern_failure("Could not identify liquidity provider", context)
                        continue

                    # Get provider's receipt transfers
                    receipts_trf = receipts_in.get(provider, {})
                    if len(receipts_trf) != 1:
                        self._log_pattern_failure("Provider must have exactly 1 receipt transfer",
                                                 context,
                                                 provider=provider,
                                                 receipt_count=len(receipts_trf))
                        continue

                    # Process the mint event
                    mint_event = self._create_mint_event(signal, provider, base_match, quote_match, receipts_trf, context)
                    if mint_event:
                        events[mint_event.content_id] = mint_event
                        
                        self.log_debug("Mint event created successfully",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      event_id=mint_event.content_id,
                                      provider=provider)

                    # Process fee event if applicable
                    fee_event = self._create_mint_fee_event(signal, provider, receipts_in, receipts_out, context)
                    if fee_event:
                        events[fee_event.content_id] = fee_event
                        
                        self.log_debug("Fee event created successfully",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      event_id=fee_event.content_id,
                                      fee_collector=fee_event.recipient)

                except Exception as e:
                    self.log_error("Failed to process mint signal",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  signal_index=signal_idx,
                                  error=str(e),
                                  exception_type=type(e).__name__)
                    continue

            self._log_pattern_success(events, context)
            return events
            
        except Exception as e:
            self.log_error("Mint pattern processing failed",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return events

    def _validate_mint_signal(self, signal: LiquiditySignal, context: TransformContext) -> bool:
        """Validate mint signal data"""
        if not hasattr(signal, 'pool') or not signal.pool:
            self.log_error("Signal missing pool address",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash)
            return False
        
        required_attrs = ['base_token', 'quote_token', 'base_amount', 'quote_amount']
        for attr in required_attrs:
            if not hasattr(signal, attr) or not getattr(signal, attr):
                self.log_error("Signal missing required attribute",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              missing_attribute=attr)
                return False
        
        return True

    def _find_mint_provider(self, signal: LiquiditySignal, base_match: Dict, quote_match: Dict, 
                           receipts_in: Dict, context: TransformContext) -> str:
        """Find the liquidity provider for mint operation"""
        if not receipts_in:
            self.log_warning("No receipt transfers found",
                           pattern_name=self.name,
                           tx_hash=context.transaction.tx_hash)
            return None

        # Get potential providers from token transfers
        potential_providers = set()
        if base_match:
            potential_providers.add(next(iter(base_match.values())).from_address)
        if quote_match:
            potential_providers.add(next(iter(quote_match.values())).from_address)
        
        receipt_receivers = set(receipts_in.keys())
        
        self.log_debug("Provider identification",
                      pattern_name=self.name,
                      tx_hash=context.transaction.tx_hash,
                      potential_providers=list(potential_providers),
                      receipt_receivers=list(receipt_receivers))

        # Find provider who both sent tokens and received receipts
        for receiver in receipt_receivers:
            if receiver in potential_providers:
                self.log_debug("Provider identified via token transfers",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              provider=receiver)
                return receiver
        
        # Fallback to signal owner if they received receipts
        if hasattr(signal, 'owner') and signal.owner and signal.owner in receipt_receivers:
            self.log_debug("Provider identified via signal owner",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          provider=signal.owner)
            return signal.owner
        
        self.log_warning("Could not identify provider",
                        pattern_name=self.name,
                        tx_hash=context.transaction.tx_hash,
                        potential_providers=list(potential_providers),
                        receipt_receivers=list(receipt_receivers))
        return None

    def _create_mint_event(self, signal: LiquiditySignal, provider: str, base_match: Dict, 
                          quote_match: Dict, receipts_trf: Dict, context: TransformContext) -> Liquidity:
        """Create liquidity mint event"""
        try:
            # Combine all transfer signals
            all_signals = base_match | quote_match | receipts_trf
            all_signals[signal.log_index] = signal
            
            # Generate positions
            token_positions = self._generate_positions(base_match | quote_match, context)
            receipt_positions = self._generate_lp_positions(signal.pool, receipts_trf, context)
            all_positions = token_positions | receipt_positions

            # Create liquidity event
            mint = Liquidity(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                pool=signal.pool,
                provider=provider,
                base_token=signal.base_token,
                base_amount=signal.base_amount,
                quote_token=signal.quote_token,
                quote_amount=signal.quote_amount,
                action="add",
                positions=all_positions,
                signals=all_signals,
            )
            
            # Add to context and mark signals consumed
            context.add_events({mint.content_id: mint})
            context.mark_signals_consumed(list(all_signals.keys()))
            
            self.log_debug("Mint event created",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          provider=provider,
                          positions_created=len(all_positions),
                          signals_consumed=len(all_signals))
            
            return mint
            
        except Exception as e:
            self.log_error("Failed to create mint event",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          provider=provider,
                          error=str(e),
                          exception_type=type(e).__name__)
            return None

    def _create_mint_fee_event(self, signal: LiquiditySignal, provider: str, receipts_in: Dict, 
                              receipts_out: Dict, context: TransformContext) -> Reward:
        """Create fee reward event for mint operation"""
        try:
            # Look for fee transfers from zero address
            fee_trf = receipts_out.get(ZERO_ADDRESS, {})
            if not fee_trf:
                self.log_debug("No fee transfers found",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash)
                return None

            # Find fee transfers not going to provider
            fee_match = {
                idx: transfer for idx, transfer in fee_trf.items() 
                if transfer.to_address != provider
            }
            
            if not fee_match:
                self.log_debug("No external fee transfers found",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              total_fee_transfers=len(fee_trf))
                return None

            fee_collector = next(iter(fee_match.values())).to_address
            
            # Get fee collector's receipt transfers
            collector_receipts = receipts_in.get(fee_collector, {})
            if len(collector_receipts) != 1:
                self.log_warning("Fee collector should have exactly 1 receipt transfer",
                               pattern_name=self.name,
                               tx_hash=context.transaction.tx_hash,
                               fee_collector=fee_collector,
                               receipt_count=len(collector_receipts))
                return None

            # Calculate total fee amount
            fee_amount = sum(amount_to_int(transfer.amount) for transfer in collector_receipts.values())
            
            # Generate positions for fee
            fee_positions = self._generate_positions(collector_receipts, context)

            # Create reward event
            fee = Reward(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                contract=signal.pool,
                recipient=fee_collector,
                token=signal.pool,
                amount=str(fee_amount),
                reward_type="fees",
                positions=fee_positions,
                signals=collector_receipts
            )
            
            # Add to context and mark signals consumed
            context.add_events({fee.content_id: fee})
            context.mark_signals_consumed(list(collector_receipts.keys()))
            
            self.log_debug("Fee event created",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          fee_collector=fee_collector,
                          fee_amount=fee_amount,
                          positions_created=len(fee_positions))
            
            return fee
            
        except Exception as e:
            self.log_error("Failed to create fee event",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return None


class Burn_A(TransferPattern):    
    def __init__(self):
        super().__init__("Burn_A")
    
    def produce_events(self, signals: Dict[int, LiquiditySignal], context: TransformContext) -> Dict[DomainEventId, Liquidity|Reward]:
        """Process burn signals to create liquidity and reward events"""
        if not self._validate_signal_data(signals, context):
            return {}
        
        self._log_pattern_start(signals, context)
        events = {}

        try:
            for signal_idx, signal in signals.items():
                try:
                    self.log_debug("Processing burn signal",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  signal_index=signal_idx,
                                  pool=signal.pool,
                                  base_amount=signal.base_amount,
                                  quote_amount=signal.quote_amount)

                    # Validate signal and get provider
                    if not self._validate_burn_signal(signal, context):
                        continue

                    provider = getattr(signal, 'owner', None)
                    if not provider:
                        self._log_pattern_failure("Burn signal missing provider (owner)", context)
                        continue

                    # Get transfer data
                    pool_in, pool_out = context.get_unmatched_contract_transfers(signal.pool)
                    receipts_in, receipts_out = context.get_unmatched_token_transfers(signal.pool)

                    self.log_debug("Retrieved transfer data for burn",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  provider=provider,
                                  pool_out_tokens=len(pool_out),
                                  receipts_out_addresses=len(receipts_out))

                    # Find matching outgoing transfers
                    base_trf = pool_out.get(signal.base_token, {})
                    quote_trf = pool_out.get(signal.quote_token, {})

                    base_match = {
                        idx: transfer for idx, transfer in base_trf.items() 
                        if transfer.amount == abs_amount(signal.base_amount)
                    }
                    quote_match = {
                        idx: transfer for idx, transfer in quote_trf.items() 
                        if transfer.amount == abs_amount(signal.quote_amount)
                    }
                    
                    self.log_debug("Burn transfer matching results",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  base_matches=len(base_match),
                                  quote_matches=len(quote_match))
                    
                    if len(base_match) != 1 or len(quote_match) != 1:
                        self._log_pattern_failure("Transfer matching failed - expected exactly 1 match per token",
                                                 context,
                                                 base_matches=len(base_match),
                                                 quote_matches=len(quote_match))
                        continue

                    # Get provider's receipt transfers
                    receipts_trf = receipts_out.get(provider, {})
                    if len(receipts_trf) != 1:
                        self._log_pattern_failure("Provider must have exactly 1 receipt transfer",
                                                 context,
                                                 provider=provider,
                                                 receipt_count=len(receipts_trf))
                        continue

                    # Process the burn event
                    burn_event = self._create_burn_event(signal, provider, base_match, quote_match, receipts_trf, receipts_in, context)
                    if burn_event:
                        events[burn_event.content_id] = burn_event
                        
                        self.log_debug("Burn event created successfully",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      event_id=burn_event.content_id,
                                      provider=provider)

                    # Process fee event if applicable
                    fee_event = self._create_burn_fee_event(signal, receipts_in, receipts_out, context)
                    if fee_event:
                        events[fee_event.content_id] = fee_event
                        
                        self.log_debug("Burn fee event created successfully",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      event_id=fee_event.content_id,
                                      fee_collector=fee_event.recipient)

                except Exception as e:
                    self.log_error("Failed to process burn signal",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  signal_index=signal_idx,
                                  error=str(e),
                                  exception_type=type(e).__name__)
                    continue

            self._log_pattern_success(events, context)
            return events
            
        except Exception as e:
            self.log_error("Burn pattern processing failed",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return events

    def _validate_burn_signal(self, signal: LiquiditySignal, context: TransformContext) -> bool:
        """Validate burn signal data"""
        if not hasattr(signal, 'pool') or not signal.pool:
            self.log_error("Signal missing pool address",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash)
            return False
        
        required_attrs = ['base_token', 'quote_token', 'base_amount', 'quote_amount']
        for attr in required_attrs:
            if not hasattr(signal, attr) or not getattr(signal, attr):
                self.log_error("Signal missing required attribute",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              missing_attribute=attr)
                return False
        
        return True

    def _create_burn_event(self, signal: LiquiditySignal, provider: str, base_match: Dict, 
                          quote_match: Dict, receipts_trf: Dict, receipts_in: Dict, context: TransformContext) -> Liquidity:
        """Create liquidity burn event"""
        try:
            # Include pool receipt transfers (burning LP tokens)
            pool_trf = receipts_in.get(ZERO_ADDRESS, {})
            receipts_match = receipts_trf | pool_trf
            
            # Combine all transfer signals
            all_signals = base_match | quote_match | receipts_match
            all_signals[signal.log_index] = signal
            
            # Generate positions
            token_positions = self._generate_positions(base_match | quote_match, context)
            receipt_positions = self._generate_lp_positions(signal.pool, receipts_match, context)
            all_positions = token_positions | receipt_positions

            # Create liquidity event
            burn = Liquidity(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                pool=signal.pool,
                provider=provider,
                base_token=signal.base_token,
                base_amount=signal.base_amount,
                quote_token=signal.quote_token,
                quote_amount=signal.quote_amount,
                action="remove",
                positions=all_positions,
                signals=all_signals,
            )
            
            # Add to context and mark signals consumed
            context.add_events({burn.content_id: burn})
            context.mark_signals_consumed(list(all_signals.keys()))
            
            self.log_debug("Burn event created",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          provider=provider,
                          positions_created=len(all_positions),
                          signals_consumed=len(all_signals))
            
            return burn
            
        except Exception as e:
            self.log_error("Failed to create burn event",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          provider=provider,
                          error=str(e),
                          exception_type=type(e).__name__)
            return None

    def _create_burn_fee_event(self, signal: LiquiditySignal, receipts_in: Dict, receipts_out: Dict, context: TransformContext) -> Reward:
        """Create fee reward event for burn operation"""
        try:
            # Look for fee transfers from zero address
            fee_trf = receipts_out.get(ZERO_ADDRESS, {})
            if len(fee_trf) != 1:
                self.log_debug("Expected exactly 1 fee transfer for burn",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              fee_transfer_count=len(fee_trf))
                return None

            fee_collector = next(iter(fee_trf.values())).to_address
            fee_amount = sum(amount_to_int(transfer.amount) for transfer in fee_trf.values())

            # Generate positions for fee
            fee_positions = self._generate_positions(fee_trf, context)

            # Create reward event
            fee = Reward(
                timestamp=context.transaction.timestamp,
                tx_hash=context.transaction.tx_hash,
                contract=signal.pool,
                recipient=fee_collector,
                token=signal.pool,
                amount=str(fee_amount),
                reward_type="fees",
                positions=fee_positions,
                signals=fee_trf
            )
            
            # Add to context and mark signals consumed
            context.add_events({fee.content_id: fee})
            context.mark_signals_consumed(list(fee_trf.keys()))
            
            self.log_debug("Burn fee event created",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          fee_collector=fee_collector,
                          fee_amount=fee_amount,
                          positions_created=len(fee_positions))
            
            return fee
            
        except Exception as e:
            self.log_error("Failed to create burn fee event",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          error=str(e),
                          exception_type=type(e).__name__)
            return None