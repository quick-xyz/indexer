# indexer/database/repository.py

from typing import Any, Type

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from .connection import ModelDatabaseManager
from .indexer.tables.events.transfer import Transfer
from .indexer.tables.events.liquidity import Liquidity
from .indexer.tables.events.reward import Reward
from ..core.logging_config import IndexerLogger, log_with_context
from .indexer.repositories.trade_repository import TradeRepository
from .indexer.repositories.pool_swap_repository import PoolSwapRepository
from .indexer.repositories.position_repository import PositionRepository
from .indexer.repositories.event_repository import DomainEventRepository
from .indexer.repositories.processing_repository import ProcessingRepository


import logging


class BaseRepository:
    """Base repository providing common CRUD operations for all models."""
    
    def __init__(self, db_manager: ModelDatabaseManager, model_class: Type):
        self.db_manager = db_manager
        self.model_class = model_class
        self.logger = IndexerLogger.get_logger(f'database.repository.{model_class.__name__.lower()}')
    
    def create(self, session: Session, **kwargs):
        """Create a new record"""
        try:
            instance = self.model_class(**kwargs)
            session.add(instance)
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Record created",
                            model=self.model_class.__name__,
                            id=getattr(instance, 'id', 'N/A'))
            return instance
            
        except IntegrityError as e:
            log_with_context(self.logger, logging.ERROR, "Integrity error creating record",
                            model=self.model_class.__name__,
                            error=str(e))
            raise
    
    def get_by_id(self, session: Session, record_id: Any):
        """Get record by primary key"""
        try:
            return session.query(self.model_class).filter(
                self.model_class.id == record_id
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting record by ID",
                            model=self.model_class.__name__,
                            record_id=record_id,
                            error=str(e))
            raise


class RepositoryManager:
    """Central manager for all repositories, integrates with DI container."""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        self.db_manager = db_manager
        self.logger = IndexerLogger.get_logger('database.repository.manager')
        
        # Initialize all repositories
        self.trades = TradeRepository(db_manager)
        self.pool_swaps = PoolSwapRepository(db_manager)
        self.positions = PositionRepository(db_manager)
        self.transfers = DomainEventRepository(db_manager, Transfer)
        self.liquidity = DomainEventRepository(db_manager, Liquidity)
        self.rewards = DomainEventRepository(db_manager, Reward)
        self.processing = ProcessingRepository(db_manager)
        
        self.logger.info("RepositoryManager initialized with all repositories")
    
    def get_session(self):
        """Get database session (delegates to DatabaseManager)"""
        return self.db_manager.get_session()
    
    def get_transaction(self):
        """Get database transaction (delegates to DatabaseManager)"""
        return self.db_manager.get_transaction()