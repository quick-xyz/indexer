# indexer/transform/patterns/liquidity.py
"""
Liquidity operation patterns
"""

from typing import Dict, List, Any, Tuple, Optional

from ...types import LiquiditySignal, Signal, EvmAddress, ZERO_ADDRESS
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformerContext

class LiquidityAddBasic(TransferPattern):    
    def __init__(self):
        super().__init__("liquidity_add_basic")
    
    def _extract_addresses(self, signal: LiquiditySignal, context: TransformerContext) -> Optional[AddressContext]:
        addresses = self._determine_provider_and_collector(signal, context)
        
        if addresses is None or addresses[0] is None:
            return None

        return AddressContext(
            base = signal.base_token,
            quote = signal.quote_token,
            pool = signal.pool,
            provider = addresses[0],
            router = signal.sender,
            fee_collector = addresses[1] if addresses[1] else None
        )

    def _determine_provider_and_collector(self, signal: LiquiditySignal, context: TransformerContext) -> Tuple[EvmAddress, EvmAddress]:
        pool_recipients = context.trf_dict.get(signal.pool, {}).get("in", {})
        
        if not pool_recipients:
            return None, None

        if signal.owner in pool_recipients or signal.sender in pool_recipients:
            provider = signal.owner or signal.sender

            if len(pool_recipients) == 1:
                return provider, None
            elif len(pool_recipients) == 2:
                return provider, next(iter(pool_recipients.keys() - {provider}), None)
            else:
                return provider, None

        if len(pool_recipients) == 1:
            return next(iter(pool_recipients.keys())), None
        
        return None, None
    
    def generate_transfer_legs(self, signal: LiquiditySignal, context: TransformerContext) -> Tuple[Optional[AddressContext],Optional[List[TransferLeg]]]:        
        address = self.extract_addresses(signal, context)

        if address is None:
            return None
    
        legs = [
            TransferLeg(
                token = address.base,
                from_end = address.provider,
                to_end = address.pool,
                amount = signal.base_amount.lstrip('-')
            ),
            TransferLeg(
                token = address.quote,
                from_end = address.provider,
                to_end = address.pool,
                amount = signal.quote_amount.lstrip('-')
            ),
            TransferLeg(
                token = address.pool,
                from_end = ZERO_ADDRESS,
                to_end = address.provider,
                amount = signal.receipt_amount
            ),
        ]

        if address.fee_collector:
            legs.append(TransferLeg(
                token = address.pool,
                from_end = ZERO_ADDRESS,
                to_end = address.fee_collector,
                amount = None
            ))

        return address, legs
    


class LiquidityRemoveBasic(TransferPattern):
    def __init__(self):
        super().__init__("liquidity_remove_basic")
    
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