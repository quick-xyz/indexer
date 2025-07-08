# indexer/database/indexer/tables/__init__.py

# Processing tables
from .processing import (
    TransactionProcessing, BlockProcessing, ProcessingJob,
    TransactionStatus, JobType, JobStatus
)

# Event tables
from .events.trade import Trade, PoolSwap, TradeDirection, TradeType
from .events.transfer import Transfer
from .events.liquidity import Liquidity, LiquidityAction
from .events.reward import Reward
from .events.position import Position

# Detail tables (pricing/valuation)
from .detail.pool_swap_detail import (
    PoolSwapDetail, PricingDenomination, PricingMethod
)
from .detail.trade_detail import TradeDetail
from .detail.event_detail import EventDetail

__all__ = [
    # Processing tables
    'TransactionProcessing',
    'BlockProcessing', 
    'ProcessingJob',
    'TransactionStatus',
    'JobType',
    'JobStatus',
    
    # Domain event tables
    'Trade',
    'PoolSwap',
    'Transfer',
    'Liquidity',
    'Reward',
    'Position',
    
    # Enums
    'TradeDirection',
    'TradeType',
    'LiquidityAction',
    
    # Detail tables
    'PoolSwapDetail',
    'TradeDetail', 
    'EventDetail',
    
    # Detail enums
    'PricingDenomination',
    'PricingMethod'
]