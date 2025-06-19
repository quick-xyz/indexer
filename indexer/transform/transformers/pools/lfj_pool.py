# indexer/transform/transformers/pools/lfj_pool.py

from .pool_base import PoolTransformer
from ....types import EvmAddress


class LfjPoolTransformer(PoolTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, base_token: EvmAddress, fee_collector: EvmAddress):
        if not contract or not token0 or not token1 or not base_token or not fee_collector:
            raise ValueError("All addresses including fee_collector are required for LfjPoolTransformer")
            
        super().__init__(contract, token0, token1, base_token, fee_collector)
        
        self.log_info("LfjPoolTransformer initialized",
                     contract_address=self.contract_address,
                     token0=self.token0,
                     token1=self.token1,
                     base_token=self.base_token,
                     quote_token=self.quote_token,
                     fee_collector=self.fee_collector,
                     transformer_type="LFJ_Pool",
                     protocol="Liquidity_Finance_Joe")