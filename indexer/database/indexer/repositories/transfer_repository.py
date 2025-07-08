# indexer/database/indexer/repositories/transfer_repository.py

from ...base_repository import DomainEventBaseRepository
from ..tables.events.transfer import Transfer


class TransferRepository(DomainEventBaseRepository[Transfer]):
    """Repository for transfer events"""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, Transfer)