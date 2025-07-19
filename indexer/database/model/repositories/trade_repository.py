# indexer/database/model/repositories/trade_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import desc, or_

from ....types import EvmAddress
from ...connection import ModelDatabaseManager
from .event_repository import DomainEventRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

from ..tables import DBTrade


class TradeRepository(DomainEventRepository):
    """Repository for trade events with trade-specific queries"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, DBTrade)
        self.logger = IndexerLogger.get_logger('database.repositories.trade')

    def get_by_taker(self, session: Session, taker: EvmAddress, limit: int = 100) -> List[DBTrade]:
        """Get trades by taker address"""
        try:
            return session.query(DBTrade).filter(
                DBTrade.taker == taker
            ).order_by(desc(DBTrade.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting trades by taker",
                            taker=taker,
                            error=str(e))
            raise
    
    def get_by_token(self, session: Session, token: EvmAddress, limit: int = 100) -> List[DBTrade]:
        """Get trades involving a specific token"""
        try:
            return session.query(DBTrade).filter(
                or_(DBTrade.base_token == token, DBTrade.quote_token == token)
            ).order_by(desc(DBTrade.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting trades by token",
                            token=token,
                            error=str(e))
            raise

    def get_arbitrage_trades(self, session: Session, limit: int = 100) -> List[DBTrade]:
        """Get arbitrage trades"""
        try:
            return session.query(DBTrade).filter(
                DBTrade.trade_type == 'arbitrage'
            ).order_by(desc(DBTrade.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting arbitrage trades",
                            error=str(e))
            raise