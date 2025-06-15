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
        from_addr = log.attributes.get("src", "")
        to_addr = log.attributes.get("dst", "")
        value = log.attributes.get("wad", 0)  # Keep as int initially
        sender = ""  # WAVAX doesn't have sender in Transfer events
        
        # Convert addresses to strings if they're not already
        from_addr = str(from_addr) if from_addr is not None else ""
        to_addr = str(to_addr) if to_addr is not None else ""
        
        return from_addr, to_addr, value, sender 