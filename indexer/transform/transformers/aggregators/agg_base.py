# indexer/transform/transformers/aggregators/agg_base.py

from typing import Dict, Tuple, List

from ..base import BaseTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
    ProcessingError,
    Signal,
    RouteSignal,
    MultiRouteSignal,
    ErrorId,
)
from ....utils.amounts import amount_to_str, is_zero


class AggregatorTransformer(BaseTransformer):    
    def __init__(self, contract: EvmAddress):
        super().__init__(contract_address=contract.lower())
        self.handler_map = {}

    def _get_route_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str, str]:
        sender = str(log.attributes.get("sender", ""))
        to = str(log.attributes.get("to", ""))
        token_in = str(log.attributes.get("tokenIn", ""))
        amount_in = amount_to_str(log.attributes.get("amountIn", 0))
        token_out = str(log.attributes.get("tokenOut", ""))
        amount_out = amount_to_str(log.attributes.get("amountOut", 0))
        return sender, to, token_in, token_out, amount_in, amount_out
    
    def _get_multi_route_attributes(self, log: DecodedLog) -> Tuple[str, str, List[str], List[str], List[str], List[str]]:
        sender = str(log.attributes.get("sender", ""))
        to = str(log.attributes.get("to", ""))
        tokens_in = [str(t) for t in log.attributes.get("tokensIn", [])]
        amounts_in = [amount_to_str(a) for a in log.attributes.get("amountsIn", [])]
        tokens_out = [str(t) for t in log.attributes.get("tokensOut", [])]
        amounts_out = [amount_to_str(a) for a in log.attributes.get("amountsOut", [])]
        return sender, to, tokens_in, tokens_out, amounts_in, amounts_out

    def _validate_route_attributes(self, log: DecodedLog, route: Tuple[str, str, str, str, str, str],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        if is_zero(route[4]) and is_zero(route[5]):
            self.log_warning("Swap amounts are zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        return True

    def _validate_multi_route_attributes(self, log: DecodedLog, route: Tuple[str, str, List[str], List[str], List[str], List[str]],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(route[4:], log.index, errors):
            return False
        return True
    
    def _handle_route(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling route log", log_index=log.index)
        
        route = self._get_route_attributes(log)
        if not self._validate_route_attributes(log, route, errors):
            return
        
        signals[log.index] = RouteSignal(
            log_index=log.index,
            contract=self.contract_address,
            token_in=EvmAddress(route[2].lower()),
            amount_in=route[4],
            token_out=EvmAddress(route[3].lower()),
            amount_out=route[5],
            to=EvmAddress(route[1].lower()) if route[1] else None,
            sender=EvmAddress(route[0].lower()) if route[0] else None,
        )
        self.log_debug("Route signal created", log_index=log.index)
    
    def _handle_multi_route(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling multi route log", log_index=log.index)
        
        route = self._get_multi_route_attributes(log)
        if not self._validate_multi_route_attributes(log, route, errors):
            return
        
        signals[log.index] = MultiRouteSignal(
            log_index=log.index,
            contract=self.contract_address,
            tokens_in= [EvmAddress(t.lower()) for t in route[2]],
            amounts_in=route[4],
            tokens_out= [EvmAddress(t.lower()) for t in route[3]],
            amounts_out=route[5],
            to=EvmAddress(route[1].lower()) if route[1] else None,
            sender=EvmAddress(route[0].lower()) if route[0] else None,
        )
        self.log_debug("Multi route signal created", log_index=log.index)