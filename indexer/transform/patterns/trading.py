# indexer/transform/patterns/trading.py
"""
Trading operation patterns
"""

from typing import Dict, List, Any

from ...types import SwapSignal, RouteSignal, Signal
from ...types.constants import ZERO_ADDRESS
from .base import TransferPattern, TransferLeg, AddressPattern


class DirectSwapPattern(TransferPattern):
    """Pattern for direct pool swaps: token in to pool, token out from pool"""
    
    def __init__(self):
        super().__init__("direct_swap")
    
    def extract_context_data(self, signal: SwapSignal, context) -> Dict[str, Any]:
        return {
            "base_token": signal.base_token,
            "quote_token": signal.quote_token,
            "pool": signal.pool,
            "taker": signal.to or signal.sender
        }
    
    def generate_legs(self, signal: SwapSignal, context_data: Dict[str, Any]) -> List[TransferLeg]:
        # Determine swap direction from signal amounts
        base_amount = float(signal.base_amount)
        
        if base_amount > 0:  # Selling base for quote
            return [
                TransferLeg(
                    token=context_data["base_token"],
                    from_pattern=AddressPattern.TAKER,
                    to_pattern=AddressPattern.POOL,
                    description="Base token in"
                ),
                TransferLeg(
                    token=context_data["quote_token"],
                    from_pattern=AddressPattern.POOL,
                    to_pattern=AddressPattern.TAKER,
                    description="Quote token out"
                )
            ]
        else:  # Buying base with quote
            return [
                TransferLeg(
                    token=context_data["quote_token"],
                    from_pattern=AddressPattern.TAKER,
                    to_pattern=AddressPattern.POOL,
                    description="Quote token in"
                ),
                TransferLeg(
                    token=context_data["base_token"],
                    from_pattern=AddressPattern.POOL,
                    to_pattern=AddressPattern.TAKER,
                    description="Base token out"
                )
            ]


class RoutedSwapPattern(TransferPattern):
    """Pattern for routed swaps: tokens through router to final destination"""
    
    def __init__(self):
        super().__init__("routed_swap")
    
    def extract_context_data(self, signal: RouteSignal, context) -> Dict[str, Any]:
        return {
            "token_in": signal.token_in,
            "token_out": signal.token_out,
            "router": signal.contract,
            "taker": signal.to or signal.sender
        }
    
    def generate_legs(self, signal: RouteSignal, context_data: Dict[str, Any]) -> List[TransferLeg]:
        return [
            # Input leg - tokens from taker to router
            TransferLeg(
                token=context_data["token_in"],
                from_pattern=AddressPattern.TAKER,
                to_pattern=AddressPattern.ROUTER,
                description="Input token to router"
            ),
            # Output leg - tokens from router to taker
            TransferLeg(
                token=context_data["token_out"],
                from_pattern=AddressPattern.ROUTER,
                to_pattern=AddressPattern.TAKER,
                description="Output token from router"
            )
        ]


class RoutedSwapWithWrapPattern(TransferPattern):
    """Pattern for routed swaps with AVAX wrapping/unwrapping"""
    
    def __init__(self):
        super().__init__("routed_swap_with_wrap")
        self.wavax_address = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
    
    def extract_context_data(self, signal: RouteSignal, context) -> Dict[str, Any]:
        return {
            "token_in": signal.token_in,
            "token_out": signal.token_out,
            "router": signal.contract,
            "taker": signal.to or signal.sender,
            "wavax": self.wavax_address
        }
    
    def generate_legs(self, signal: RouteSignal, context_data: Dict[str, Any]) -> List[TransferLeg]:
        legs = []
        
        # Input leg
        legs.append(TransferLeg(
            token=context_data["token_in"],
            from_pattern=AddressPattern.TAKER,
            to_pattern=AddressPattern.ROUTER,
            description="Input token to router"
        ))
        
        # If WAVAX is involved, expect wrap/unwrap transfers
        if context_data["token_in"] == self.wavax_address:
            legs.append(TransferLeg(
                token=self.wavax_address,
                from_pattern=AddressPattern.ZERO,
                to_pattern=AddressPattern.TAKER,
                description="WAVAX mint (wrap)"
            ))
        
        if context_data["token_out"] == self.wavax_address:
            legs.append(TransferLeg(
                token=self.wavax_address,
                from_pattern=AddressPattern.TAKER,
                to_pattern=AddressPattern.ZERO,
                description="WAVAX burn (unwrap)"
            ))
        
        # Output leg
        legs.append(TransferLeg(
            token=context_data["token_out"],
            from_pattern=AddressPattern.ROUTER,
            to_pattern=AddressPattern.TAKER,
            description="Output token from router"
        ))
        
        return legs