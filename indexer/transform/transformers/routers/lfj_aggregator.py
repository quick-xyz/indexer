# indexer/transform/transformers/routers/lfj_aggregator.py

from typing import List, Dict, Tuple, Optional
from .router_base import BaseRouterTransformer
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


class LfjAggregatorTransformer(BaseRouterTransformer):
    """
    LFJ Aggregator transformer - operates in aggregator mode to handle meta-aggregation.
    
    This transformer aggregates both PoolSwap events and Trade events from other routers
    into comprehensive Trade events that represent the full user trading experience.
    
    Key Features:
    - Aggregates PoolSwap events from constituent pools
    - Aggregates Trade events from other routers (meta-aggregation)
    - Creates reconciling Swap events for any unaccounted amounts
    - Validates that constituent events sum to aggregator totals
    """
    
    def __init__(self, contract: EvmAddress, wnative: EvmAddress):
        super().__init__(
            contract=contract,
            wnative=wnative,
            aggregation_mode="aggregator",  # Aggregates both PoolSwaps and Trades
            trade_type="trade"
        )

    def process_logs(self, logs: List[DecodedLog], tx: Transaction) -> Tuple[Optional[Dict[DomainEventId, Transfer]], Optional[Dict[DomainEventId, DomainEvent]], Optional[Dict[ErrorId, ProcessingError]]]:
        """
        Process LFJ Aggregator logs.
        
        Handles:
        - SwapExactIn: User specifies exact input amount, receives variable output
        - SwapExactOut: User specifies exact output amount, pays variable input
        """
        new_events, matched_transfers, errors = {}, {}, {}

        try:
            for log in logs:
                try:
                    if log.name in ["SwapExactIn", "SwapExactOut"]:
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