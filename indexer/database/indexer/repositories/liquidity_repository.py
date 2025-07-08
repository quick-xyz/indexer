# indexer/database/indexer/repositories/liquidity_repository.py

from ...base_repository import DomainEventBaseRepository
from ..tables.events.liquidity import Liquidity


class LiquidityRepository(DomainEventBaseRepository[Liquidity]):
    """Repository for liquidity events"""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, Liquidity)