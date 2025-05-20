from typing import Union

from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.trade import Trade
from ....utils.logger import get_logger


class LfjPoolTransformer:
    def __init__(self, contract,base_token):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.base = base_token

    def get_tokens_amounts(self, log: DecodedLog) -> tuple:
        token_in = log.attributes.get("tokenIn")
        token_out = log.attributes.get("tokenOut")
        amount_in = log.attributes.get("amountIn")
        amount_out = log.attributes.get("amountOut")

        if token_in == self.base:
            base_token = token_in
            base_amount = amount_in
            quote_token = token_out
            quote_amount = amount_out
        elif token_out == self.base:
            base_token = token_out
            base_amount = amount_out
            quote_token = token_in
            quote_amount = amount_in

        return base_token, quote_token, base_amount, quote_amount

    def get_direction(self, base_amount: int) -> str:
        if base_amount > 0:
            return "buy"
        else:
            return "sell"

    def process_bool(self, value: bool) -> str:
        if value:
            return "added"
        else:
            return "removed"

    def handle_trade(self, log: DecodedLog, context: TransactionContext) -> list[Trade]:
        base_token, quote_token, base_amount, quote_amount = self.get_tokens_amounts(log)
        direction = self.get_direction(base_amount)

        trade = Trade(
            timestamp=context.timestamp,
            tx_hash=context.tx_hash,
            router=context.contract,
            taker=context.sender,
            direction=direction,
            base_token=base_token,
            base_amount=quote_token,
            quote_token=base_amount,
            quote_amount=quote_amount,
            event_tag=direction,
        )
        return [trade]

    def transform_log(self, log: DecodedLog, context: TransactionContext) -> list[DomainEvent]:
        events = []

        if log.name == "SwapExactIn":
            events.append(self.handle_trade(log, context))
        elif log.name == "SwapExactOut":
            events.append(self.handle_trade(log, context))   

        return events