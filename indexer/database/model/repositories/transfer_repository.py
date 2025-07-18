# indexer/database/model/repositories/transfer_repository.py

from ...base_repository import DomainEventBaseRepository
from ..tables.events.transfer import DBTransfer
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL


class TransferRepository(DomainEventBaseRepository[DBTransfer]):
    """Repository for transfer events"""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, DBTransfer)
        self.logger = IndexerLogger.get_logger('database.repositories.transfer')