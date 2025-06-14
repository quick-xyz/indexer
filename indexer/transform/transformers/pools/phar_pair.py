# indexer/transform/transformers/pools/phar_pair.py

from typing import Tuple

from .pool_base import PoolTransformer
from ....types import DecodedLog, EvmAddress
from ....utils.amounts import amount_to_str


class PharPairTransformer(PoolTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress):
        super().__init__(contract,token0,token1,base_token)

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        from_addr = str(log.attributes.get("from", ""))
        to_addr = str(log.attributes.get("to", ""))
        value = amount_to_str(log.attributes.get("amount", 0))
        sender = str(log.attributes.get("sender", ""))
        
        return from_addr, to_addr, value, sender