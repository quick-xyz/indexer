# indexer/database/model/tables/__init__.py

# Processing tables
from .processing import DBTransactionProcessing, DBBlockProcessing, DBProcessingJob

# Event tables
from .events.liquidity import DBLiquidity
from .events.position import DBPosition
from .events.reward import DBReward
from .events.staking import DBStaking
from .events.trade import DBTrade, DBPoolSwap
from .events.transfer import DBTransfer


# Detail tables (pricing/valuation)
from .detail.event_detail import DBEventDetail
from .detail.pool_swap_detail import DBPoolSwapDetail
from .detail.trade_detail import DBTradeDetail

# Asset tables
from .asset_price import DBAssetPrice
from .asset_volume import DBAssetVolume

__all__ = [
    # Processing tables
    'DBTransactionProcessing',
    'DBBlockProcessing',
    'DBProcessingJob',
    
    # Domain event tables
    'DBLiquidity',
    'DBPosition',
    'DBReward',
    'DBStaking',
    'DBTrade',
    'DBPoolSwap',
    'DBTransfer',

    # Detail tables
    'DBEventDetail',
    'DBPoolSwapDetail',
    'DBTradeDetail',

    # Asset tables
    'DBAssetPrice',
    'DBAssetVolume',
]