# indexer/transform/transformers/aggregators/lfj_aggregator.py

from .agg_base import AggregatorTransformer
from ....types import EvmAddress


class LfjAggregatorTransformer(AggregatorTransformer):    
    def __init__(self, contract: EvmAddress):
        if not contract:
            raise ValueError("Contract address is required for LfjAggregatorTransformer")
            
        super().__init__(contract=contract)
        
        self.handler_map = {
            "SwapExactIn": self._handle_route,
            "SwapExactOut": self._handle_route,
        }
        
        self.log_info("LfjAggregatorTransformer initialized",
                     contract_address=self.contract_address,
                     handler_count=len(self.handler_map),
                     supported_events=list(self.handler_map.keys()),
                     transformer_type="LFJ_Aggregator")