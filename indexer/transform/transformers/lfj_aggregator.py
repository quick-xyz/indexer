from typing import Union

from ...decode.model.block import DecodedLog
from ..events.base import DomainEvent, TransactionContext
from ..events.transfer import Transfer
from ..events.liquidity import Liquidity
from ..events.trade import Trade
from ...utils.logger import get_logger


class LfjPoolTransformer:
    def __init__(self, contract):
        self.logger = get_logger(__name__)

    def process_bool(self, value: bool) -> str:
        if value:
            return "added"
        else:
            return "removed"


    def handle_logic_update(self, log: DecodedLog, context: TransactionContext) -> list[Liquidity]:
        logic = LogicUpdate(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            logic=log.attributes.get("routerLogic"),
            update=self.process_bool(log.attributes.get("added"))
        )
        return [logic]

    def handle_swap_exact_in(self, log: DecodedLog, context: TransactionContext) -> list[Liquidity]:

log.attributes.get("sender")
log.attributes.get("to")
log.attributes.get("tokenIn")
log.attributes.get("tokenOut")
log.attributes.get("amountIn")
log.attributes.get("amountOut")



    def handle_swap_exact_out(self, log: DecodedLog, context: TransactionContext) -> list[Liquidity]:

log.attributes.get("sender")
log.attributes.get("to")
log.attributes.get("tokenIn")
log.attributes.get("tokenOut")
log.attributes.get("amountIn")
log.attributes.get("amountOut")



    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []
        if log.name == "RouterLogicUpdated":
            events.append(self.handle_transfer(log, context))
        elif log.name == "SwapExactIn":
            events.append(self.handle_swap_exact_in(log, context))
        elif log.name == "SwapExactOut":
            events.append(self.handle_swap_exact_out(log, context))   

        return events