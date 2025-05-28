from typing import List

from ...decode.model.block import DecodedLog
from ..events.base import DomainEvent
from ..events.transfer import Transfer, TransferIds
from ..events.liquidity import Liquidity, Position
from ..events.trade import PoolSwap
from ...utils.logger import get_logger
from ...utils.lb_byte32_decoder import decode_amounts

from ..events.transfer import Transfer

class TokenTransformer:
    def __init__(self, contract):
        self.logger = get_logger(__name__)
        self.contract = contract

    def process_transfers(self, logs: List[DecodedLog]) -> List[DomainEvent]:
        if log.name == "Transfer":
            transfer = Transfer(
                timestamp=context.timestamp,
                tx_hash=context.tx_hash,
                from_address=log.attributes.get("from"),
                to_address=log.attributes.get("to"),
                token=log.contract,
                amount=log.attributes.get("value"),
                decimals=self.decimals
            )
            return [transfer]