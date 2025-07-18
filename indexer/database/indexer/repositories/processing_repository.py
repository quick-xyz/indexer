# indexer/database/indexer/repositories/processing_repository.py

from typing import List

from sqlalchemy.orm import Session
from sqlalchemy import and_

from ...connection import ModelDatabaseManager
from ..tables.processing import TransactionProcessing
from ....core.logging import log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ....types.new import EvmHash
from ...base_repository import BaseRepository


class ProcessingRepository(BaseRepository):
    """Repository for transaction processing status"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, TransactionProcessing)
    
    def get_by_tx_hash(self, session: Session, tx_hash: EvmHash):
        """Get processing status by transaction hash"""
        try:
            return session.query(TransactionProcessing).filter(
                TransactionProcessing.tx_hash == tx_hash
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting processing by tx_hash",
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
            log_with_context(self.logger, ERROR, "Error getting failed transactions",
                            error=str(e))
            raise
