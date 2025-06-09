# indexer/transform/transformers/tokens/wavax.py

from typing import Tuple

from .token_base import TokenTransformer
from ....types import (
    DecodedLog,
)
from ....utils.amounts import amount_to_str

class WavaxTransformer(TokenTransformer):   
    def __init__(self, contract: str):
        super().__init__(contract=contract)

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        from_addr = str(log.attributes.get("src", ""))
        to_addr = str(log.attributes.get("dst", ""))
        value = amount_to_str(log.attributes.get("wad", 0))
        sender = ""
        return from_addr, to_addr, value, sender