# indexer/transform/patterns/liquidity.py
"""
Liquidity operation patterns
"""

from typing import Dict, List, Any

from ...types import LiquiditySignal, Signal
from .base import TransferPattern, TransferLeg, AddressPattern
from ..context import TransformerContext

class LiquidityAddPattern(TransferPattern):    
    def __init__(self):
        super().__init__("liquidity_add")
    
    def extract_context_data(self, signal: LiquiditySignal, context: TransformerContext) -> Dict[str, Any]:
        return {
            "base_token": signal.base_token,
            "quote_token": signal.quote_token,
            "pool": signal.pool,
            "provider": signal.owner or signal.sender or self._infer_provider(signal, context)
        }
    
    def _infer_provider(self, signal: LiquiditySignal, context: TransformerContext) -> str:
        pool_recipients = context.trf_dict.get(signal.pool, {}).get("in", {})
        if len(pool_recipients) == 1:
            return next(iter(pool_recipients.keys()))
        return None
    
    def generate_legs(self, signal: LiquiditySignal, context_data: Dict[str, Any]) -> List[TransferLeg]:
        return [
            TransferLeg(
                token=context_data["base_token"],
                from_pattern=AddressPattern.PROVIDER,
                to_pattern=AddressPattern.POOL,
                description="Base token deposit"
            ),
            TransferLeg(
                token=context_data["quote_token"], 
                from_pattern=AddressPattern.PROVIDER,
                to_pattern=AddressPattern.POOL,
                description="Quote token deposit"
            ),
            TransferLeg(
                token=context_data["pool"],
                from_pattern=AddressPattern.ZERO,
                to_pattern=AddressPattern.PROVIDER,
                description="LP token mint"
            )
        ]


class LiquidityRemovePattern(TransferPattern):
    def __init__(self):
        super().__init__("liquidity_remove")
    
    def extract_context_data(self, signal: LiquiditySignal, context: TransformerContext) -> Dict[str, Any]:
        return {
            "base_token": signal.base_token,
            "quote_token": signal.quote_token, 
            "pool": signal.pool,
            "provider": signal.owner or signal.sender or self._infer_provider(signal, context)
        }
    
    def _infer_provider(self, signal: LiquiditySignal, context: TransformerContext) -> str:
        pool_senders = context.trf_dict.get(signal.pool, {}).get("out", {})
        if len(pool_senders) == 1:
            return next(iter(pool_senders.keys()))
        return None
    
    def generate_legs(self, signal: LiquiditySignal, context_data: Dict[str, Any]) -> List[TransferLeg]:
        return [
            TransferLeg(
                token=context_data["pool"],
                from_pattern=AddressPattern.PROVIDER,
                to_pattern=AddressPattern.ZERO,
                description="LP token burn"
            ),
            TransferLeg(
                token=context_data["base_token"],
                from_pattern=AddressPattern.POOL,
                to_pattern=AddressPattern.PROVIDER,
                description="Base token withdrawal"
            ),
            TransferLeg(
                token=context_data["quote_token"],
                from_pattern=AddressPattern.POOL, 
                to_pattern=AddressPattern.PROVIDER,
                description="Quote token withdrawal"
            )
        ]