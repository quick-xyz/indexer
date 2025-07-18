# indexer/database/connection.py

from typing import Generator, Optional
from contextlib import contextmanager

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.pool import QueuePool

from ..core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ..types.configs.config import DatabaseConfig


class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        if not config:
            raise ValueError("DatabaseConfig is required")
        
        self.config = config
        self.logger = IndexerLogger.get_logger(f'database.{self.__class__.__name__.lower()}')
        self._engine = None
        self._session_factory = None
        self._scoped_session = None
        self._repositories = {}  # Cache for repository instances
        
        log_with_context(self.logger, INFO, "DatabaseManager initialized",
                        db_url_host=self._extract_host_from_url(config.url))
    
    def _extract_host_from_url(self, url: str) -> str:
        try:
            if '@' in url and '/' in url:
                after_at = url.split('@')[1]
                host_part = after_at.split('/')[0]
                return host_part
            return "unknown"
        except Exception:
            return "unknown"
    
    def initialize(self) -> None:
        if self._engine is not None:
            self.logger.warning("Database already initialized")
            return
        
        try:
            self.logger.info("Initializing database engine")
            
            self._engine = create_engine(
                self.config.url,
                poolclass=QueuePool,
                pool_size=getattr(self.config, 'pool_size', 5),
                max_overflow=getattr(self.config, 'max_overflow', 10),
                pool_timeout=30,
                pool_recycle=3600,  # Recycle connections after 1 hour
                echo=False,  # Set to True for SQL debugging
            )
            
            self._session_factory = sessionmaker(
                bind=self._engine,
                expire_on_commit=False,  # Keep objects accessible after commit
            )
            
            self._scoped_session = scoped_session(self._session_factory)
            
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            log_with_context(self.logger, INFO, "Database initialized successfully",
                            pool_size=getattr(self.config, 'pool_size', 5),
                            max_overflow=getattr(self.config, 'max_overflow', 10))
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Failed to initialize database",
                            error=str(e),
                            exception_type=type(e).__name__)
            raise
    
    def shutdown(self) -> None:
        self.logger.info("Shutting down database connections")
        
        try:
            if self._scoped_session:
                self._scoped_session.remove()
                self._scoped_session = None
            
            if self._engine:
                self._engine.dispose()
                self._engine = None
            
            self._session_factory = None
            
            self.logger.info("Database shutdown completed")
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error during database shutdown",
                            error=str(e),
                            exception_type=type(e).__name__)
    
    @property
    def engine(self) -> Engine:
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._engine
    
    @property
    def session_factory(self) -> sessionmaker:
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._session_factory
    
    def get_scoped_session(self) -> scoped_session:
        if self._scoped_session is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._scoped_session
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        session = self._session_factory()
        try:
            log_with_context(self.logger, DEBUG, "Database session created")
            yield session
        except Exception as e:
            log_with_context(self.logger, ERROR, "Database session error, rolling back",
                            error=str(e),
                            exception_type=type(e).__name__)
            session.rollback()
            raise
        finally:
            session.close()
            log_with_context(self.logger, DEBUG, "Database session closed")
    
    @contextmanager
    def get_transaction(self) -> Generator[Session, None, None]:
        with self.get_session() as session:
            try:
                yield session
                session.commit()
                log_with_context(self.logger, DEBUG, "Database transaction committed")
            except Exception as e:
                session.rollback()
                log_with_context(self.logger, ERROR, "Database transaction rolled back",
                                error=str(e),
                                exception_type=type(e).__name__)
                raise
    
    def health_check(self) -> bool:
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            log_with_context(self.logger, ERROR, "Database health check failed",
                            error=str(e),
                            exception_type=type(e).__name__)
            return False

    def _get_or_create_repository(self, repo_class, repo_name):
        """Get cached repository or create new instance"""
        if repo_name not in self._repositories:
            self._repositories[repo_name] = repo_class(self)
        return self._repositories[repo_name]
    
    def clear_repository_cache(self):
        """Clear the repository cache (useful for testing)"""
        self._repositories.clear()


class SharedDatabaseManager(DatabaseManager):
    """
    Database manager for shared database (indexer_shared) with repository management.
    
    Manages both database connections and the repositories that operate on this database.
    """
    
    # === Configuration Repositories ===
    
    def get_model_repo(self):
        """Get the model repository"""
        from .shared.repositories.config.model_repository import ModelRepository
        return self._get_or_create_repository(ModelRepository, 'model')
    
    def get_contract_repo(self):
        """Get the contract repository"""
        from .shared.repositories.config.contract_repository import ContractRepository
        return self._get_or_create_repository(ContractRepository, 'contract')
    
    def get_token_repo(self):
        """Get the token repository"""
        from .shared.repositories.config.token_repository import TokenRepository
        return self._get_or_create_repository(TokenRepository, 'token')
    
    def get_source_repo(self):
        """Get the source repository"""
        from .shared.repositories.config.source_repository import SourceRepository
        return self._get_or_create_repository(SourceRepository, 'source')
    
    def get_address_repo(self):
        """Get the address repository"""
        from .shared.repositories.config.address_repository import AddressRepository
        return self._get_or_create_repository(AddressRepository, 'address')
    
    def get_label_repo(self):
        """Get the label repository"""
        from .shared.repositories.config.label_repository import LabelRepository
        return self._get_or_create_repository(LabelRepository, 'label')
    
    def get_pool_repo(self):
        """Get the pool repository"""
        from .shared.repositories.config.pool_repository import PoolRepository
        return self._get_or_create_repository(PoolRepository, 'pool')
    
    def get_pricing_repo(self):
        """Get the pricing repository"""
        from .shared.repositories.config.pricing_repository import PricingRepository
        return self._get_or_create_repository(PricingRepository, 'pricing')
    
    def get_relations_repo(self):
        """Get the model relations repository"""
        from .shared.repositories.config.model_relations_repository import ModelRelationsRepository
        return self._get_or_create_repository(ModelRelationsRepository, 'relations')
    
    # === Infrastructure Repositories ===
    
    def get_block_prices_repo(self):
        """Get the block prices repository"""
        from .shared.repositories.block_prices_repository import BlockPricesRepository
        return self._get_or_create_repository(BlockPricesRepository, 'block_prices')
    
    def get_periods_repo(self):
        """Get the periods repository"""
        from .shared.repositories.periods_repository import PeriodsRepository
        return self._get_or_create_repository(PeriodsRepository, 'periods')
    
    def get_price_vwap_repo(self):
        """Get the price VWAP repository"""
        from .shared.repositories.price_vwap_repository import PriceVwapRepository
        return self._get_or_create_repository(PriceVwapRepository, 'price_vwap')
    
    def get_pool_pricing_config_repo(self):
        """Get the pool pricing config repository"""
        from .shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
        return self._get_or_create_repository(PoolPricingConfigRepository, 'pool_pricing_config')


class ModelDatabaseManager(DatabaseManager):
    """
    Database manager for model-specific database (e.g. blub_test) with repository management.
    
    Manages both database connections and the repositories that operate on this database.
    """
    
    # === Domain Event Repositories ===
    
    def get_trade_repo(self):
        """Get the trade repository"""
        from .model.repositories.trade_repository import TradeRepository
        return self._get_or_create_repository(TradeRepository, 'trade')
    
    def get_pool_swap_repo(self):
        """Get the pool swap repository"""
        from .model.repositories.pool_swap_repository import PoolSwapRepository
        return self._get_or_create_repository(PoolSwapRepository, 'pool_swap')
    
    def get_position_repo(self):
        """Get the position repository"""
        from .model.repositories.position_repository import PositionRepository
        return self._get_or_create_repository(PositionRepository, 'position')
    
    def get_transfer_repo(self):
        """Get the transfer repository"""
        from .model.repositories.transfer_repository import TransferRepository
        return self._get_or_create_repository(TransferRepository, 'transfer')
    
    def get_liquidity_repo(self):
        """Get the liquidity repository"""
        from .model.repositories.liquidity_repository import LiquidityRepository
        return self._get_or_create_repository(LiquidityRepository, 'liquidity')
    
    def get_reward_repo(self):
        """Get the reward repository"""
        from .model.repositories.reward_repository import RewardRepository
        return self._get_or_create_repository(RewardRepository, 'reward')
    
    # === Detail Repositories (Pricing/Valuation) ===
    
    def get_pool_swap_detail_repo(self):
        """Get the pool swap detail repository"""
        from .model.repositories.pool_swap_detail_repository import PoolSwapDetailRepository
        return self._get_or_create_repository(PoolSwapDetailRepository, 'pool_swap_detail')
    
    def get_trade_detail_repo(self):
        """Get the trade detail repository"""
        from .model.repositories.trade_detail_repository import TradeDetailRepository
        return self._get_or_create_repository(TradeDetailRepository, 'trade_detail')
    
    def get_event_detail_repo(self):
        """Get the event detail repository"""
        from .model.repositories.event_detail_repository import EventDetailRepository
        return self._get_or_create_repository(EventDetailRepository, 'event_detail')
    
    # === Processing Repository ===
    
    def get_processing_repo(self):
        """Get the processing repository"""
        from .model.repositories.processing_repository import ProcessingRepository
        return self._get_or_create_repository(ProcessingRepository, 'processing')
    
    # === Calculation Service Repositories ===
    
    def get_asset_price_repo(self):
        """Get the asset price repository"""
        from .model.repositories.asset_price_repository import AssetPriceRepository
        return self._get_or_create_repository(AssetPriceRepository, 'asset_price')
    
    def get_asset_volume_repo(self):
        """Get the asset volume repository"""
        from .model.repositories.asset_volume_repository import AssetVolumeRepository
        return self._get_or_create_repository(AssetVolumeRepository, 'asset_volume')