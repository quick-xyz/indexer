# indexer/database/model/repositories/processing_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import and_

from .....indexer.types import EvmHash
from ...connection import ModelDatabaseManager
from ...base_repository import BaseRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

from ...model.tables import DBTransactionProcessing


class ProcessingRepository(BaseRepository):
    """Repository for transaction processing status"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, DBTransactionProcessing)
        self.logger = IndexerLogger.get_logger('database.repositories.processing')
    
    def get_by_tx_hash(self, session: Session, tx_hash: EvmHash):
        """Get processing status by transaction hash"""
        try:
            return session.query(DBTransactionProcessing).filter(
                DBTransactionProcessing.tx_hash == tx_hash
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting processing by tx_hash",
                            tx_hash=tx_hash,
                            error=str(e))
            raise
    
    def get_failed_transactions(self, session: Session, max_retries: int = 3) -> List[DBTransactionProcessing]:
        """Get failed transactions that can be retried"""
        try:
            return session.query(DBTransactionProcessing).filter(
                and_(
                    DBTransactionProcessing.status == 'failed',
                    DBTransactionProcessing.retry_count < max_retries
                )
            ).order_by(DBTransactionProcessing.last_processed_at).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting failed transactions",
                            error=str(e))
            raise
