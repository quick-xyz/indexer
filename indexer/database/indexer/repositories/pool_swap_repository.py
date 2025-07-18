# indexer/database/indexer/repositories/pool_swap_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import desc

from ...connection import ModelDatabaseManager
from ..tables.events.trade import PoolSwap
from ....core.logging import log_with_context
from ....types.new import EvmAddress, DomainEventId
from .event_repository import DomainEventRepository

import logging


class PoolSwapRepository(DomainEventRepository):
    """Repository for pool swap events"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
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
