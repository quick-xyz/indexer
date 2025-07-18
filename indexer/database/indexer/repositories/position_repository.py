# indexer/database/indexer/repositories/position_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import desc, and_

from ...connection import ModelDatabaseManager
from ..tables.events.position import Position
from ....core.logging import log_with_context
from ....types.new import EvmAddress, DomainEventId
from ...base_repository import DomainEventBaseRepository

import logging


class PositionRepository(DomainEventBaseRepository[Position]):
    """Repository for position events"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
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