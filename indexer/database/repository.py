# indexer/database/repository.py

from typing import Optional
from contextlib import contextmanager

from .connection import ModelDatabaseManager, InfrastructureDatabaseManager
from ..core.logging_config import IndexerLogger, log_with_context

# Import all indexer database repositories
from .indexer.repositories.trade_repository import TradeRepository
from .indexer.repositories.pool_swap_repository import PoolSwapRepository
from .indexer.repositories.position_repository import PositionRepository
from .indexer.repositories.processing_repository import ProcessingRepository
from .indexer.repositories.pool_swap_detail_repository import PoolSwapDetailRepository
from .indexer.repositories.trade_detail_repository import TradeDetailRepository
from .indexer.repositories.event_detail_repository import EventDetailRepository

# Import shared database repositories
from .shared.repositories.block_prices_repository import BlockPricesRepository
from .shared.repositories.periods_repository import PeriodsRepository
from .shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository

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
    """
    
    def __init__(self, 
                 model_db_manager: ModelDatabaseManager,
                 infrastructure_db_manager: Optional[InfrastructureDatabaseManager] = None):
        self.model_db_manager = model_db_manager
        self.infrastructure_db_manager = infrastructure_db_manager
        
        self.logger = IndexerLogger.get_logger('database.repository_manager')
        
        # Initialize indexer database repositories (model-specific data)
        self._init_indexer_repositories()
        
        # Initialize shared database repositories (infrastructure data)
        if infrastructure_db_manager:
            self._init_shared_repositories()
        
        log_with_context(
            self.logger, logging.INFO, "RepositoryManager initialized",
            has_shared_db=infrastructure_db_manager is not None,
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
        
        log_with_context(
            self.logger, logging.DEBUG, "Indexer database repositories initialized"
        )
    
    def _init_shared_repositories(self):
        """Initialize repositories for shared database (infrastructure data)"""
        # Infrastructure repositories
        self.block_prices = BlockPricesRepository(self.infrastructure_db_manager)
        self.periods = PeriodsRepository(self.infrastructure_db_manager)
        self.pool_pricing_configs = PoolPricingConfigRepository(self.infrastructure_db_manager)
        
        log_with_context(
            self.logger, logging.DEBUG, "Shared database repositories initialized"
        )
    
    @contextmanager
    def get_session(self):
        """Get session for indexer database (model-specific data)"""
        with self.model_db_manager.get_session() as session:
            yield session
    
    @contextmanager
    def get_shared_session(self):
        """Get session for shared database (infrastructure data)"""
        if not self.infrastructure_db_manager:
            raise RuntimeError("Shared database manager not available")
        
        with self.infrastructure_db_manager.get_session() as session:
            yield session
    
    @contextmanager
    def get_transaction(self):
        """Get transaction context for indexer database"""
        with self.model_db_manager.get_transaction() as session:
            yield session
    
    @contextmanager
    def get_shared_transaction(self):
        """Get transaction context for shared database"""
        if not self.infrastructure_db_manager:
            raise RuntimeError("Shared database manager not available")
        
        with self.infrastructure_db_manager.get_transaction() as session:
            yield session
    
    def has_shared_access(self) -> bool:
        """Check if shared database access is available"""
        return self.infrastructure_db_manager is not None
    
    def get_event_repository(self, event_type: str):
        """
        Get appropriate repository for domain event type.
        
        Used by DomainEventWriter to route events to correct repositories.
        """
        event_type_lower = event_type.lower()
        
        if event_type_lower in ['trade']:
            return self.trades
        elif event_type_lower in ['poolswap', 'pool_swap']:
            return self.pool_swaps
        elif event_type_lower in ['transfer']:
            return self.transfers
        elif event_type_lower in ['liquidity']:
            return self.liquidity
        elif event_type_lower in ['reward']:
            return self.rewards
        elif event_type_lower in ['position']:
            return self.positions
        else:
            raise ValueError(f"Unknown event type: {event_type}")
    
    def health_check(self) -> dict:
        """
        Perform health check on database connections.
        
        Returns status of both indexer and shared database connections.
        """
        health_status = {
            'indexer_db': False,
            'shared_db': False,
            'errors': []
        }
        
        # Test indexer database connection
        try:
            with self.get_session() as session:
                from sqlalchemy import text
                result = session.execute(text("SELECT 1")).fetchone()
                health_status['indexer_db'] = result[0] == 1
        except Exception as e:
            health_status['errors'].append(f"Indexer DB error: {str(e)}")
        
        # Test shared database connection (if available)
        if self.has_shared_access():
            try:
                with self.get_shared_session() as session:
                    from sqlalchemy import text
                    result = session.execute(text("SELECT 1")).fetchone()
                    health_status['shared_db'] = result[0] == 1
            except Exception as e:
                health_status['errors'].append(f"Shared DB error: {str(e)}")
        
        return health_status


# Legacy compatibility - keeping the old BaseRepository import for any code that still uses it
# This will be removed once all imports are updated
from .base_repository import BaseRepository, DomainEventBaseRepository, ProcessingBaseRepository

__all__ = [
    'RepositoryManager',
    'BaseRepository',
    'DomainEventBaseRepository', 
    'ProcessingBaseRepository'
]