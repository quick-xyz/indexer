# indexer/database/model/repositories/pool_swap_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import desc

from .....indexer.types import DomainEventId, EvmAddress
from ...connection import ModelDatabaseManager
from .event_repository import DomainEventRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

from ...model.tables import DBPoolSwap


class PoolSwapRepository(DomainEventRepository):
    """Repository for pool swap events"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, DBPoolSwap)
        self.logger = IndexerLogger.get_logger('database.repositories.pool_swap')

    def get_by_trade_id(self, session: Session, trade_id: DomainEventId) -> List[DBPoolSwap]:
        """Get all swaps for a specific trade"""
        try:
            return session.query(DBPoolSwap).filter(
                DBPoolSwap.trade_id == trade_id
            ).order_by(DBPoolSwap.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting swaps by trade_id",
                            trade_id=trade_id,
                            error=str(e))
            raise
    
    def get_by_pool(self, session: Session, pool: EvmAddress, limit: int = 100) -> List[DBPoolSwap]:
        """Get swaps for a specific pool"""
        try:
            return session.query(DBPoolSwap).filter(
                DBPoolSwap.pool == pool
            ).order_by(desc(DBPoolSwap.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting swaps by pool",
                            pool=pool,
                            error=str(e))
            raise
