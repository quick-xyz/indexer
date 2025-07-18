# indexer/database/model/repositories/liquidity_repository.py

from ...base_repository import DomainEventBaseRepository
from ..tables.events.liquidity import DBLiquidity
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL


class LiquidityRepository(DomainEventBaseRepository[DBLiquidity]):
    """Repository for liquidity events"""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, DBLiquidity)
        self.logger = IndexerLogger.get_logger('database.repositories.liquidity')