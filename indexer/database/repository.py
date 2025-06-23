# indexer/database/repository.py

from typing import List, Optional, Dict, Any, Type
from abc import ABC, abstractmethod
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, and_, or_
from sqlalchemy.exc import IntegrityError, NoResultFound

from .connection import DatabaseManager
from .models.base import DomainEventModel, BaseModel
from .models.events.trade import Trade, PoolSwap
from .models.events.position import Position
from .models.events.transfer import Transfer
from .models.events.liquidity import Liquidity
from .models.events.reward import Reward
from .models.processing import TransactionProcessing, BlockProcessing, ProcessingJob
from ..core.logging_config import IndexerLogger, log_with_context
from ..types.new import EvmAddress, EvmHash, DomainEventId

import logging


class BaseRepository:
    """Base repository providing common CRUD operations for all models."""
    
    def __init__(self, db_manager: DatabaseManager, model_class: Type):
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


class DomainEventRepository(BaseRepository):
    """Base repository for domain events with common query patterns."""
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId):
        """Get domain event by content_id"""
        try:
            return session.query(self.model_class).filter(
                self.model_class.content_id == content_id
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting record by content_id",
                            model=self.model_class.__name__,
                            content_id=content_id,
                            error=str(e))
            raise
    
    def get_by_tx_hash(self, session: Session, tx_hash: EvmHash) -> List:
        """Get all events for a transaction"""
        try:
            return session.query(self.model_class).filter(
                self.model_class.tx_hash == tx_hash
            ).order_by(self.model_class.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting records by tx_hash",
                            model=self.model_class.__name__,
                            tx_hash=tx_hash,
                            error=str(e))
            raise
    
    def get_by_block_range(self, session: Session, start_block: int, end_block: int) -> List:
        """Get events in block range"""
        try:
            return session.query(self.model_class).filter(
                and_(
                    self.model_class.block_number >= start_block,
                    self.model_class.block_number <= end_block
                )
            ).order_by(self.model_class.block_number, self.model_class.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting records by block range",
                            model=self.model_class.__name__,
                            start_block=start_block,
                            end_block=end_block,
                            error=str(e))
            raise
    
    def get_recent(self, session: Session, limit: int = 100) -> List:
        """Get most recent events"""
        try:
            return session.query(self.model_class).order_by(
                desc(self.model_class.timestamp)
            ).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting recent records",
                            model=self.model_class.__name__,
                            limit=limit,
                            error=str(e))
            raise


class TradeRepository(DomainEventRepository):
    """Repository for trade events with trade-specific queries"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, Trade)
    
    def get_by_taker(self, session: Session, taker: EvmAddress, limit: int = 100) -> List[Trade]:
        """Get trades by taker address"""
        try:
            return session.query(Trade).filter(
                Trade.taker == taker
            ).order_by(desc(Trade.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting trades by taker",
                            taker=taker,
                            error=str(e))
            raise
    
    def get_by_token(self, session: Session, token: EvmAddress, limit: int = 100) -> List[Trade]:
        """Get trades involving a specific token"""
        try:
            return session.query(Trade).filter(
                or_(Trade.base_token == token, Trade.quote_token == token)
            ).order_by(desc(Trade.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting trades by token",
                            token=token,
                            error=str(e))
            raise
    
    def get_arbitrage_trades(self, session: Session, limit: int = 100) -> List[Trade]:
        """Get arbitrage trades"""
        try:
            return session.query(Trade).filter(
                Trade.trade_type == 'arbitrage'
            ).order_by(desc(Trade.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting arbitrage trades",
                            error=str(e))
            raise


class PoolSwapRepository(DomainEventRepository):
    """Repository for pool swap events"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, PoolSwap)
    
    def get_by_trade_id(self, session: Session, trade_id: DomainEventId) -> List[PoolSwap]:
        """Get all swaps for a specific trade"""
        try:
            return session.query(PoolSwap).filter(
                PoolSwap.trade_id == trade_id
            ).order_by(PoolSwap.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting swaps by trade_id",
                            trade_id=trade_id,
                            error=str(e))
            raise
    
    def get_by_pool(self, session: Session, pool: EvmAddress, limit: int = 100) -> List[PoolSwap]:
        """Get swaps for a specific pool"""
        try:
            return session.query(PoolSwap).filter(
                PoolSwap.pool == pool
            ).order_by(desc(PoolSwap.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting swaps by pool",
                            pool=pool,
                            error=str(e))
            raise


class PositionRepository(DomainEventRepository):
    """Repository for position events"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, Position)
    
    def get_by_user(self, session: Session, user: EvmAddress, limit: int = 100) -> List[Position]:
        """Get positions for a specific user"""
        try:
            return session.query(Position).filter(
                Position.user == user
            ).order_by(desc(Position.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting positions by user",
                            user=user,
                            error=str(e))
            raise
    
    def get_by_parent(self, session: Session, parent_id: DomainEventId, parent_type: str) -> List[Position]:
        """Get positions created by a specific parent event"""
        try:
            return session.query(Position).filter(
                and_(
                    Position.parent_id == parent_id,
                    Position.parent_type == parent_type
                )
            ).order_by(Position.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting positions by parent",
                            parent_id=parent_id,
                            parent_type=parent_type,
                            error=str(e))
            raise
    
    def get_user_token_positions(self, session: Session, user: EvmAddress, token: EvmAddress) -> List[Position]:
        """Get all positions for a user-token pair"""
        try:
            return session.query(Position).filter(
                and_(
                    Position.user == user,
                    Position.token == token
                )
            ).order_by(Position.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting user token positions",
                            user=user,
                            token=token,
                            error=str(e))
            raise


class ProcessingRepository(BaseRepository):
    """Repository for transaction processing status"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__(db_manager, TransactionProcessing)
    
    def get_by_tx_hash(self, session: Session, tx_hash: EvmHash):
        """Get processing status by transaction hash"""
        try:
            return session.query(TransactionProcessing).filter(
                TransactionProcessing.tx_hash == tx_hash
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting processing by tx_hash",
                            tx_hash=tx_hash,
                            error=str(e))
            raise
    
    def get_failed_transactions(self, session: Session, max_retries: int = 3) -> List[TransactionProcessing]:
        """Get failed transactions that can be retried"""
        try:
            return session.query(TransactionProcessing).filter(
                and_(
                    TransactionProcessing.status == 'failed',
                    TransactionProcessing.retry_count < max_retries
                )
            ).order_by(TransactionProcessing.last_processed_at).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting failed transactions",
                            error=str(e))
            raise


class RepositoryManager:
    """Central manager for all repositories, integrates with DI container."""
    
    def __init__(self, db_manager: DatabaseManager):
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