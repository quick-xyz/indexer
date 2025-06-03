# indexer/transform/transformers/routers/generic_router.py

from typing import List, Dict, Tuple, Optional, Any
from .base_router import BaseRouterTransformer
from ....types import (
    DecodedLog,
    Transaction,
    EvmAddress,
    Transfer,
    DomainEvent,
    ProcessingError,
    DomainEventId,
    ErrorId,
)


class GenericRouterTransformer(BaseRouterTransformer):
    """
    Generic router transformer - operates in router mode to aggregate PoolSwaps only.
    
    This transformer can be customized for different router event formats by:
    1. Overriding _extract_swap_data() for different event structures
    2. Specifying which event names to handle
    3. Customizing trade_type and other parameters
    
    Use this as a base for creating specific router transformers like:
    - JoeRouter02Transformer
    - KyberswapRouterTransformer  
    - OdosRouterTransformer
    - etc.
    """
    
    def __init__(self, contract: EvmAddress, wnative: EvmAddress, 
                 swap_event_names: List[str], trade_type: str = "trade"):
        """
        Initialize generic router transformer.
        
        Args:
            contract: Router contract address
            wnative: Wrapped native token address
            swap_event_names: List of event names to handle (e.g. ["SwapExactTokensForTokens"])
            trade_type: Type of trade to create ("trade", "arbitrage", etc.)
        """
        super().__init__(
            contract=contract,
            wnative=wnative,
            aggregation_mode="router",  # Aggregates only PoolSwaps
            trade_type=trade_type
        )
        self.swap_event_names = swap_event_names

    def _extract_swap_data(self, log: DecodedLog) -> Optional[Dict[str, Any]]:
        """
        Override to handle different router event formats.
        
        This example shows how to handle different event structures.
        Customize this method for specific router implementations.
        """
        try:
            # Example: Handle TraderJoe/Uniswap V2 style events
            if log.name in ["SwapExactTokensForTokens", "SwapTokensForExactTokens"]:
                # These events might have different attribute names
                return {
                    "sender": log.attributes.get("sender"),  # Or "from"
                    "to": log.attributes.get("to"),
                    "token_in": log.attributes.get("tokenIn"),  # Or extract from path[0]
                    "token_out": log.attributes.get("tokenOut"),  # Or extract from path[-1]
                    "amount_in": log.attributes.get("amountIn"),
                    "amount_out": log.attributes.get("amountOut")
                }
            
            # Example: Handle Kyberswap/1inch style events  
            elif log.name == "Swapped":
                return {
                    "sender": log.attributes.get("trader"),
                    "to": log.attributes.get("recipient"),
                    "token_in": log.attributes.get("srcToken"),
                    "token_out": log.attributes.get("dstToken"),
                    "amount_in": log.attributes.get("srcAmount"),
                    "amount_out": log.attributes.get("dstAmount")
                }
            
            # Fallback to LFJ Aggregator format
            else:
                return super()._extract_swap_data(log)
                
        except Exception:
            return None

    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId, Transfer]], Optional[Dict[DomainEventId, DomainEvent]], Optional[Dict[ErrorId, ProcessingError]]]:
        """
        Process generic router logs.
        
        Handles any event names specified in swap_event_names.
        """
        new_events, matched_transfers, errors = {}, {}, {}

        try:
            for log in logs:
                try:
                    if log.name in self.swap_event_names:
                        swap_result = self._handle_swap_event(log, tx, log.name)
                        if swap_result:
                            new_events.update(swap_result["events"])
                            matched_transfers.update(swap_result["transfers"])
                            errors.update(swap_result["errors"])
                
                except Exception as e:
                    self._create_log_exception(e, tx.tx_hash, log.index, self.__class__.__name__, errors)

        except Exception as e:
            self._create_tx_exception(e, tx.tx_hash, self.__class__.__name__, errors)
        
        return (
            matched_transfers if matched_transfers else None, 
            new_events if new_events else None, 
            errors if errors else None
        )


# Example specific router implementations:

class JoeRouterTransformer(GenericRouterTransformer):
    """TraderJoe Router V2 transformer"""
    
    def __init__(self, contract: EvmAddress, wnative: EvmAddress):
        super().__init__(
            contract=contract,
            wnative=wnative,
            swap_event_names=[
                "SwapExactTokensForTokens", 
                "SwapTokensForExactTokens",
                "SwapExactAVAXForTokens",
                "SwapTokensForExactAVAX"
            ],
            trade_type="trade"
        )

    def _extract_swap_data(self, log: DecodedLog) -> Optional[Dict[str, Any]]:
        """Handle TraderJoe specific event format"""
        try:
            # TraderJoe events might have different structures
            if log.name.startswith("SwapExact"):
                return {
                    "sender": log.attributes.get("sender"),
                    "to": log.attributes.get("to"),
                    "token_in": log.attributes.get("tokenIn"),
                    "token_out": log.attributes.get("tokenOut"),
                    "amount_in": log.attributes.get("amountIn"),
                    "amount_out": log.attributes.get("amountOut")
                }
            return super()._extract_swap_data(log)
        except Exception:
            return None


class KyberswapRouterTransformer(GenericRouterTransformer):
    """Kyberswap Meta Router transformer"""
    
    def __init__(self, contract: EvmAddress, wnative: EvmAddress):
        super().__init__(
            contract=contract,
            wnative=wnative,
            swap_event_names=["Swapped", "SwapExecuted"],
            trade_type="trade"
        )

    def _extract_swap_data(self, log: DecodedLog) -> Optional[Dict[str, Any]]:
        """Handle Kyberswap specific event format"""
        try:
            if log.name == "Swapped":
                return {
                    "sender": log.attributes.get("trader"),
                    "to": log.attributes.get("recipient"),
                    "token_in": log.attributes.get("srcToken"),
                    "token_out": log.attributes.get("dstToken"),
                    "amount_in": log.attributes.get("srcAmount"),
                    "amount_out": log.attributes.get("dstAmount")
                }
            return super()._extract_swap_data(log)
        except Exception:
            return None