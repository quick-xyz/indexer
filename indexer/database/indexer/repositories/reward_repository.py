# indexer/database/indexer/repositories/reward_repository.py

from ...base_repository import DomainEventBaseRepository
from ..tables.events.reward import Reward


class RewardRepository(DomainEventBaseRepository[Reward]):
    """Repository for reward events"""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, Reward)