from typing import List

from ....decode.model.block import DecodedLog
from ...events.base import DomainEvent, TransactionContext
from ...events.transfer import Transfer, TransferIds
from ...events.liquidity import Liquidity, Position
from ...events.trade import PoolSwap
from ....utils.logger import get_logger
from ....utils.lb_byte32_decoder import decode_amounts


class TokenTransformer:
    def __init__(self, contract, decimals: int):
        self.logger = get_logger(__name__)
        self.contract = contract
        self.decimals = decimals

    def _get_tokens(self) -> tuple:
        if self.token_x == self.base:
            base_token = self.token_x
            quote_token = self.token_y
        elif self.token_y == self.base:
            base_token = self.token_y
            quote_token = self.token_x

        return base_token, quote_token

    def get_direction(self, base_amount: int) -> str:
        if base_amount > 0:
            return "buy"
        else: