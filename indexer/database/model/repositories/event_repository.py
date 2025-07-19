# indexer/database/model/repositories/event_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import desc, and_

from ....types import EvmHash, DomainEventId
from ....core.logging import log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ...base_repository import DomainEventBaseRepository


class DomainEventRepository(DomainEventBaseRepository):
    """
    Base repository for domain events with common query patterns.
    
    Now extends DomainEventBaseRepository to inherit bulk operations like
    bulk_create() and bulk_create_skip_existing() that are required by the DomainEventWriter.
    
    Maintains the dual database architecture by extending the proper base class
    while ensuring all event repositories have the required bulk methods.
    """
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId):
        """Get domain event by content_id"""
        try:
            return session.query(self.model_class).filter(
                self.model_class.content_id == content_id
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting record by content_id",
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
            log_with_context(self.logger, ERROR, "Error getting records by tx_hash",
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
            log_with_context(self.logger, ERROR, "Error getting records by block range",
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
            log_with_context(self.logger, ERROR, "Error getting recent records",
                            model=self.model_class.__name__,
                            limit=limit,
                            error=str(e))
            raise