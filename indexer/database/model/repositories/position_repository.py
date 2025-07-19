# indexer/database/model/repositories/position_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import desc, and_

from ....types import DomainEventId, EvmAddress
from ...connection import ModelDatabaseManager
from .event_repository import DomainEventBaseRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

from ..tables import DBPosition


class PositionRepository(DomainEventBaseRepository[DBPosition]):
    """Repository for position events"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, DBPosition)
        self.logger = IndexerLogger.get_logger('database.repositories.position')

    def get_by_user(self, session: Session, user: EvmAddress, limit: int = 100) -> List[DBPosition]:
        """Get positions for a specific user"""
        try:
            return session.query(DBPosition).filter(
                DBPosition.user == user
            ).order_by(desc(DBPosition.timestamp)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting positions by user",
                            user=user,
                            error=str(e))
            raise
    
    def get_by_parent(self, session: Session, parent_id: DomainEventId, parent_type: str) -> List[DBPosition]:
        """Get positions created by a specific parent event"""
        try:
            return session.query(DBPosition).filter(
                and_(
                    DBPosition.parent_id == parent_id,
                    DBPosition.parent_type == parent_type
                )
            ).order_by(DBPosition.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting positions by parent",
                            parent_id=parent_id,
                            parent_type=parent_type,
                            error=str(e))
            raise
    
    def get_user_token_positions(self, session: Session, user: EvmAddress, token: EvmAddress) -> List[DBPosition]:
        """Get all positions for a user-token pair"""
        try:
            return session.query(DBPosition).filter(
                and_(
                    DBPosition.user == user,
                    DBPosition.token == token
                )
            ).order_by(DBPosition.timestamp).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting user token positions",
                            user=user,
                            token=token,
                            error=str(e))
            raise