# indexer/transform/patterns/liquidity.py
"""
Liquidity operation patterns
"""

from typing import Dict, List, Any, Tuple, Optional

from ...types import LiquiditySignal, Signal, EvmAddress, ZERO_ADDRESS
from .base import TransferPattern, TransferLeg, AddressContext
from ..context import TransformContext

class Mint_A(TransferPattern):    
    def __init__(self):
        super().__init__("Mint_A")
    
    def process_signal(self, signal: LiquiditySignal, context: TransformContext)-> Optional[Signal]:

        # get unmatched transfer dict, liquidity version
        # TODO: add method to Context to rebuild trf dict without matched transfers
        # infer addresses
        # generate transfer legs using addresses
        # look for transfers
        # check build
        # if not good: check for extra transfers,
            # if true, try again with all extra transfers (matching amounts)
            # if false, fail the processing
        # generate positions and events
        # mark signals as consumed and transfers as matched
        # return errors



    def _extract_addresses(self, signal: LiquiditySignal, context: TransformContext) -> Optional[AddressContext]:
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

    def _determine_provider_and_collector(self, signal: LiquiditySignal, context: TransformContext) -> Tuple[EvmAddress, EvmAddress]:
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
    
    def generate_transfer_legs(self, signal: LiquiditySignal, context: TransformContext) -> Tuple[Optional[AddressContext],Optional[List[TransferLeg]]]:        
        address = self.extract_addresses(signal, context)

        if address is None:
            return None
    
        legs = [
            TransferLeg(
                token = address.base,
                from_end = address.provider,
                to_end = address.pool,
                amount = signal.base_amount
            ),
            TransferLeg(
                token = address.quote,
                from_end = address.provider,
                to_end = address.pool,
                amount = signal.quote_amount
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
    
    def generate_event(self, signal: LiquiditySignal, context: TransformContext) -> Optional[Signal]:
        address_context = self._extract_addresses(signal, context)
        
        if not address_context:
            return None
        
        return Signal(
            log_index=signal.log_index,
            pattern=self.pattern_name,
            contract=signal.contract,
            base_token=address_context.base,
            quote_token=address_context.quote,
            pool=address_context.pool,
            provider=address_context.provider,
            router=address_context.router,
            fee_collector=address_context.fee_collector,
            receipt_amount=signal.receipt_amount
        )





class Burn_A(TransferPattern):
    def __init__(self):
        super().__init__("Burn_A")
    
    def extract_context_data(self, signal: LiquiditySignal, context: TransformContext) -> Dict[str, Any]:
        return {
            "base_token": signal.base_token,
            "quote_token": signal.quote_token, 
            "pool": signal.pool,
            "provider": signal.owner or signal.sender or self._infer_provider(signal, context)
        }
    
    def _infer_provider(self, signal: LiquiditySignal, context: TransformContext) -> str:
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