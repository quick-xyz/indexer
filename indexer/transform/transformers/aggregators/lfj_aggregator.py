# indexer/transform/transformers/aggregators/lfj_aggregator.py

from .agg_base import AggregatorTransformer
from ....types import EvmAddress


class LfjAggregatorTransformer(AggregatorTransformer):    
    def __init__(self, contract: EvmAddress):
        super().__init__(
            contract=contract
        )
        self.handler_map = {
            "SwapExactIn": self._handle_route,
            "SwapExactOut": self._handle_route,
        }