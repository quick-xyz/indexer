from typing import Union

from ...decode.model.block import DecodedLog
from ..events.base import DomainEvent
from ..events.transfer import Transfer
from ..events.liquidity import Liquidity
from ..events.trade import Trade
from ...utils.logger import get_logger


class LfjPoolTransformer:
    def __init__(self, contract):
        self.logger = get_logger(__name__)

