# indexer/database/repository.py

from typing import Optional
from contextlib import contextmanager

from .connection import ModelDatabaseManager, SharedDatabaseManager
from ..core.logging import IndexerLogger, log_with_context

# Import all indexer database repositories
from .indexer.repositories.trade_repository import TradeRepository
from .indexer.repositories.pool_swap_repository import PoolSwapRepository
from .indexer.repositories.position_repository import PositionRepository
from .indexer.repositories.processing_repository import ProcessingRepository
from .indexer.repositories.pool_swap_detail_repository import PoolSwapDetailRepository
from .indexer.repositories.trade_detail_repository import TradeDetailRepository
from .indexer.repositories.event_detail_repository import EventDetailRepository

# Import calculation service repositories (indexer database)
from .indexer.repositories.asset_price_repository import AssetPriceRepository
from .indexer.repositories.asset_volume_repository import AssetVolumeRepository

# Import shared database repositories
from .shared.repositories.block_prices_repository import BlockPricesRepository
from .shared.repositories.periods_repository import PeriodsRepository
from .shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository

# Import pricing service repositories (shared database)
from .shared.repositories.price_vwap_repository import PriceVwapRepository

# Import transfer/liquidity/reward repositories that extend DomainEventRepository
from .indexer.repositories.transfer_repository import TransferRepository
from .indexer.repositories.liquidity_repository import LiquidityRepository
from .indexer.repositories.reward_repository import RewardRepository

import logging


class RepositoryManager:
    """
    Central manager for all repositories with dual database support.
    
    Manages both indexer database repositories (model-specific data)
    and shared database repositories (chain-level infrastructure).
    
    Integrates with dependency injection container and provides
    unified access to all database operations.
    
    Repository Distribution:
    - Shared Database: Infrastructure, configuration, canonical pricing
    - Indexer Database: Model-specific events, details, analytics
    """
    
    def __init__(self, 
                 model_db_manager: ModelDatabaseManager,
                 shared_db_manager: Optional[SharedDatabaseManager] = None):
        self.model_db_manager = model_db_manager
        self.shared_db_manager = shared_db_manager
        
        self.logger = IndexerLogger.get_logger('database.repository_manager')
        
        # Initialize indexer database repositories (model-specific data)
        self._init_indexer_repositories()
        
        # Initialize shared database repositories (infrastructure data)
        if shared_db_manager:
            self._init_shared_repositories()
        
        log_with_context(
            self.logger, logging.INFO, "RepositoryManager initialized",
            has_shared_db=shared_db_manager is not None,
            model_db_url=model_db_manager.config.url.split('/')[-1] if model_db_manager.config.url else "unknown"
        )
    
    def _init_indexer_repositories(self):
        """Initialize repositories for indexer database (model-specific data)"""
        # Processing repositories
        self.processing = ProcessingRepository(self.model_db_manager)
        
        # Domain event repositories
        self.trades = TradeRepository(self.model_db_manager)
        self.pool_swaps = PoolSwapRepository(self.model_db_manager)
        self.positions = PositionRepository(self.model_db_manager)
        self.transfers = TransferRepository(self.model_db_manager)
        self.liquidity = LiquidityRepository(self.model_db_manager)
        self.rewards = RewardRepository(self.model_db_manager)
        
        # Detail repositories (pricing/valuation)
        self.pool_swap_details = PoolSwapDetailRepository(self.model_db_manager)
        self.trade_details = TradeDetailRepository(self.model_db_manager)
        self.event_details = EventDetailRepository(self.model_db_manager)
        
        # Calculation service repositories (indexer database)
        self.asset_prices = AssetPriceRepository(self.model_db_manager)
        self.asset_volumes = AssetVolumeRepository(self.model_db_manager)
        
        log_with_context(
            self.logger, logging.DEBUG, "Indexer database repositories initialized",
            repository_count=11
        )
    
    def _init_shared_repositories(self):
        """Initialize repositories for shared database (infrastructure data)"""
        # Infrastructure repositories
        self.block_prices = BlockPricesRepository(self.shared_db_manager)
        self.periods = PeriodsRepository(self.shared_db_manager)
        self.pool_pricing_configs = PoolPricingConfigRepository(self.shared_db_manager)
        
        # Pricing service repositories (shared database)
        self.price_vwap = PriceVwapRepository(self.shared_db_manager)
        
        log_with_context(
            self.logger, logging.DEBUG, "Shared database repositories initialized",
            repository_count=4
        )
    
    @contextmanager
    def get_model_session(self):
        """Get session from model database manager (primary database)"""
        with self.model_db_manager.get_session() as session:
            yield session
    
    @contextmanager
    def get_shared_session(self):
        """Get session from shared database manager"""
        if not self.shared_db_manager:
            raise RuntimeError("Shared database manager not available")
        
        with self.shared_db_manager.get_session() as session:
            yield session
    
    # =====================================================================
    # REPOSITORY ACCESS METHODS
    # =====================================================================
    
    # Processing repositories
    def get_processing_repository(self) -> ProcessingRepository:
        """Get processing repository for batch processing operations"""
        return self.processing
    
    # Domain event repositories (indexer database)
    def get_trade_repository(self) -> TradeRepository:
        """Get trade repository for trade event operations"""
        return self.trades
    
    def get_pool_swap_repository(self) -> PoolSwapRepository:
        """Get pool swap repository for swap event operations"""
        return self.pool_swaps
    
    def get_position_repository(self) -> PositionRepository:
        """Get position repository for position event operations"""
        return self.positions
    
    def get_transfer_repository(self) -> TransferRepository:
        """Get transfer repository for transfer event operations"""
        return self.transfers
    
    def get_liquidity_repository(self) -> LiquidityRepository:
        """Get liquidity repository for liquidity event operations"""
        return self.liquidity
    
    def get_reward_repository(self) -> RewardRepository:
        """Get reward repository for reward event operations"""
        return self.rewards
    
    # Detail repositories (indexer database)
    def get_pool_swap_detail_repository(self) -> PoolSwapDetailRepository:
        """Get pool swap detail repository for swap pricing operations"""
        return self.pool_swap_details
    
    def get_trade_detail_repository(self) -> TradeDetailRepository:
        """Get trade detail repository for trade pricing operations"""
        return self.trade_details
    
    def get_event_detail_repository(self) -> EventDetailRepository:
        """Get event detail repository for event pricing operations"""
        return self.event_details
    
    # Calculation service repositories (indexer database)
    def get_asset_price_repository(self) -> AssetPriceRepository:
        """Get asset price repository for OHLC candle operations"""
        return self.asset_prices
    
    def get_asset_volume_repository(self) -> AssetVolumeRepository:
        """Get asset volume repository for protocol volume metrics"""
        return self.asset_volumes
    
    # Infrastructure repositories (shared database)
    def get_block_prices_repository(self) -> BlockPricesRepository:
        """Get block prices repository for chain-level pricing operations"""
        if not self.shared_db_manager:
            raise RuntimeError("Shared database manager required for block prices repository")
        return self.block_prices
    
    def get_periods_repository(self) -> PeriodsRepository:
        """Get periods repository for time period management"""
        if not self.shared_db_manager:
            raise RuntimeError("Shared database manager required for periods repository")
        return self.periods
    
    def get_pool_pricing_config_repository(self) -> PoolPricingConfigRepository:
        """Get pool pricing config repository for pricing configuration"""
        if not self.shared_db_manager:
            raise RuntimeError("Shared database manager required for pool pricing config repository")
        return self.pool_pricing_configs
    
    # Pricing service repositories (shared database)
    def get_price_vwap_repository(self) -> PriceVwapRepository:
        """Get price VWAP repository for canonical pricing operations"""
        if not self.shared_db_manager:
            raise RuntimeError("Shared database manager required for price VWAP repository")
        return self.price_vwap
    
    # =====================================================================
    # REPOSITORY STATUS AND MONITORING
    # =====================================================================
    
    def get_repository_status(self) -> dict:
        """Get status information about all repositories"""
        status = {
            'model_database': {
                'url': self.model_db_manager.config.url.split('/')[-1] if self.model_db_manager.config.url else "unknown",
                'repositories': [
                    'processing', 'trades', 'pool_swaps', 'positions', 'transfers', 
                    'liquidity', 'rewards', 'pool_swap_details', 'trade_details', 
                    'event_details', 'asset_prices', 'asset_volumes'
                ]
            }
        }
        
        if self.shared_db_manager:
            status['shared_database'] = {
                'url': self.shared_db_manager.config.url.split('/')[-1] if self.shared_db_manager.config.url else "unknown",
                'repositories': [
                    'block_prices', 'periods', 'pool_pricing_configs', 'price_vwap'
                ]
            }
        else:
            status['shared_database'] = {
                'status': 'not_configured',
                'repositories': []
            }
        
        return status
    
    def validate_repository_connections(self) -> bool:
        """Validate that all repository database connections are working"""
        try:
            # Test model database connection
            with self.model_db_manager.get_session() as session:
                session.execute("SELECT 1").scalar()
            
            # Test shared database connection if available
            if self.shared_db_manager:
                with self.shared_db_manager.get_session() as session:
                    session.execute("SELECT 1").scalar()
            
            log_with_context(
                self.logger, logging.INFO, "Repository connections validated successfully"
            )
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Repository connection validation failed",
                error=str(e)
            )
            return False
    
    # =====================================================================
    # LEGACY COMPATIBILITY METHODS
    # =====================================================================
    
    def get_trade_detail_repo(self) -> TradeDetailRepository:
        """Legacy compatibility method for trade detail repository access"""
        return self.get_trade_detail_repository()
    
    def get_pool_swap_detail_repo(self) -> PoolSwapDetailRepository:
        """Legacy compatibility method for pool swap detail repository access"""
        return self.get_pool_swap_detail_repository()
    
    def get_event_detail_repo(self) -> EventDetailRepository:
        """Legacy compatibility method for event detail repository access"""
        return self.get_event_detail_repository()