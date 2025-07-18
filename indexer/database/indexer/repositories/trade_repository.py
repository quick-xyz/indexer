# indexer/database/indexer/repositories/trade_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import desc, or_

from ...connection import ModelDatabaseManager
from ..tables.events.trade import Trade
from ....core.logging import log_with_context
from ....types.new import EvmAddress
from .event_repository import DomainEventRepository

import logging


class TradeRepository(DomainEventRepository):
    """Repository for trade events with trade-specific queries"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
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