# indexer/transform/transformers/aggregators/kyber_aggregator.py

from typing import Tuple

from .agg_base import AggregatorTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
)
from ....utils.amounts import amount_to_str


class KyberAggregatorTransformer(AggregatorTransformer):    
    def __init__(self, contract: EvmAddress):
        super().__init__(
            contract=contract
        )
        self.handler_map = {
            "Swapped": self._handle_route,
        }

    def _get_route_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str, str]:
        sender = str(log.attributes.get("sender", ""))
        to = str(log.attributes.get("dstReceiver", ""))
        token_in = str(log.attributes.get("srcToken", ""))
        amount_in = amount_to_str(log.attributes.get("spentAmount", 0))
        token_out = str(log.attributes.get("dstToken", ""))
        amount_out = amount_to_str(log.attributes.get("returnAmount", 0))
        return sender, to, token_in, token_out, amount_in, amount_out
    