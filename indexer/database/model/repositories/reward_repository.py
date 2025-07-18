# indexer/database/model/repositories/reward_repository.py

from ...base_repository import DomainEventBaseRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ..tables.events.reward import DBReward


class RewardRepository(DomainEventBaseRepository[DBReward]):
    """Repository for reward events"""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, DBReward)
        self.logger = IndexerLogger.get_logger('database.repositories.reward')
