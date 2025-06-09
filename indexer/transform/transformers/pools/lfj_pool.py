# indexer/transform/transformers/pools/lfj_pool.py

from .pool_base import PoolTransformer
from ....types import EvmAddress


class LfjPoolTransformer(PoolTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress, fee_collector: EvmAddress):
        super().__init__(contract,token0,token1,base_token,fee_collector)