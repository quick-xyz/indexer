# indexer/services/pricing_service.py

from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from sqlalchemy import and_, exists, distinct

from ..core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ..database.repository_manager import RepositoryManager
from ..database.connection import DatabaseManager
from ..database.shared.tables.periods import Period, PeriodType
from ..database.shared.repositories.block_prices_repository import BlockPricesRepository
from ..database.shared.repositories.periods_repository import PeriodsRepository
from ..clients.quicknode_rpc import QuickNodeRpcClient
from ..database.indexer.tables.detail.pool_swap_detail import PricingDenomination, PricingMethod
from ..database.shared.tables.config.config import Model, Contract


from ..database.indexer.tables.events.trade import PoolSwap
from ..database.indexer.tables.events.trade import Trade, PoolSwap
from ..database.indexer.tables.detail.pool_swap_detail import PoolSwapDetail
from ..database.indexer.tables.detail.trade_detail import TradeDetail, TradePricingMethod


class PricingService:
    """
    Pricing service responsible for maintaining time-based periods and canonical pricing.
    
    This service handles both shared and indexer database operations:
    - BlockPrice operations use shared database (chain-level data)
    - Period operations use shared database (chain-level time infrastructure)
    - Canonical pricing uses shared database (price_vwap table)
    - Event pricing details use indexer database (model-specific data)
    
    Handles:
    - Period table population using QuickNode block-timestamp lookup
    - Time-based AVAX price population (every minute)
    - Direct pricing for pool swaps and trades
    - Canonical price generation using 5-minute VWAP from pricing pools
    - Global pricing application to unpriced events
    - Gap detection and backfilling for all pricing operations
    """
    
    def __init__(
        self,
        shared_db_manager: DatabaseManager,  # Shared database for prices and periods
        model_db_manager: DatabaseManager,  # Indexer database for event details
        rpc_client: QuickNodeRpcClient,
    ):
        self.shared_db_manager = shared_db_manager  # For block prices, periods, canonical pricing
        self.model_db_manager = model_db_manager  # For event pricing details
        self.rpc_client = rpc_client
        
        # Direct repository access for frequently used operations
        self.block_prices_repo = BlockPricesRepository(shared_db_manager)
        self.periods_repo = PeriodsRepository(shared_db_manager)
        
        self.logger = IndexerLogger.get_logger('services.pricing_service')
        
        log_with_context(
            self.logger, INFO, "PricingService initialized",
            shared_database=shared_db_manager.config.url.split('/')[-1],
            model_database=model_db_manager.config.url.split('/')[-1]
        )
    
    # =====================================================================
    # INFRASTRUCTURE METHODS (EXISTING)
    # =====================================================================
    
    def update_periods_to_present(self, period_types: Optional[List[PeriodType]] = None) -> Dict[str, int]:
        """
        Update periods from last recorded period to present time.
        
        This is the main method that would be called by a cron job.
        Handles gap detection and backfill for time-based operations.
        
        Args:
            period_types: List of period types to update. If None, updates all types.
            
        Returns:
            Dict with period creation statistics
        """
        if period_types is None:
            period_types = [PeriodType.ONE_MIN, PeriodType.FIVE_MIN, PeriodType.ONE_HOUR, 
                          PeriodType.FOUR_HOUR, PeriodType.ONE_DAY]
        
        log_with_context(
            self.logger, INFO, "Updating periods to present",
            period_types=[pt.value for pt in period_types]
        )
        
        try:
            with self.shared_db_manager.get_session() as session:
                total_created = 0
                
                for period_type in period_types:
                    created = self.periods_repo.update_periods_to_present(session, period_type)
                    total_created += created
                    
                    log_with_context(
                        self.logger, INFO, "Period type updated",
                        period_type=period_type.value,
                        periods_created=created
                    )
                
                session.commit()
                
                log_with_context(
                    self.logger, INFO, "Period update complete",
                    total_periods_created=total_created
                )
                
                return {'periods_created': total_created}
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to update periods",
                error=str(e)
            )
            return {'periods_created': 0, 'errors': 1}
    
    def update_minute_prices_to_present(self) -> Dict[str, int]:
        """
        Update AVAX prices using Chainlink for all minutes up to present.
        
        This pulls AVAX-USD prices from Chainlink and stores them in block_prices table.
        Should be called every minute for real-time pricing.
        
        Returns:
            Dict with price update statistics
        """
        log_with_context(
            self.logger, INFO, "Updating minute prices to present"
        )
        
        try:
            with self.shared_db_manager.get_session() as session:
                # Implementation would use Chainlink client to fetch prices
                # and block_prices_repo to store them
                results = self.block_prices_repo.update_minute_prices_to_present(session)
                
                session.commit()
                
                log_with_context(
                    self.logger, INFO, "Minute price update complete",
                    **results
                )
                
                return results
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to update minute prices",
                error=str(e)
            )
            return {'prices_updated': 0, 'errors': 1}
    
    # =====================================================================
    # DIRECT PRICING METHODS (EXISTING)
    # =====================================================================
    
    def calculate_swap_pricing(self, asset_address: str, days: Optional[int] = None) -> Dict[str, int]:
        """
        Calculate direct pricing for pool swaps using configured pricing strategies.
        
        Uses INDEXER database for swap details (model-specific event data).
        
        Args:
            asset_address: Asset to calculate swap pricing for
            days: Number of days to look back. If None, processes all unpriced swaps
            
        Returns:
            Dict with pricing statistics
        """
        log_with_context(
            self.logger, INFO, "Calculating swap pricing",
            asset_address=asset_address,
            days=days
        )
        
        # Get repository for pool swap details
        pool_swap_detail_repo = self.model_db_manager.get_pool_swap_detail_repo()
        
        try:
            # Calculate direct pricing using repository method
            with self.model_db_manager.get_session() as session:
                results = pool_swap_detail_repo.calculate_direct_pricing(
                    session,
                    asset_address=asset_address,
                    days_back=days
                )
                
                session.commit()
            
            log_with_context(
                self.logger, INFO, "Swap pricing calculation complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to calculate swap pricing",
                asset_address=asset_address,
                error=str(e)
            )
            return {'swaps_priced': 0, 'errors': 1}
    
    def calculate_trade_pricing(self, asset_address: str, days: Optional[int] = None) -> Dict[str, int]:
        """
        Calculate direct pricing for trades using volume-weighted aggregation from constituent swaps.
        
        Uses INDEXER database for trade details (model-specific event data).
        
        Args:
            asset_address: Asset to calculate trade pricing for
            days: Number of days to look back. If None, processes all unpriced trades
            
        Returns:
            Dict with pricing statistics
        """
        log_with_context(
            self.logger, INFO, "Calculating trade pricing",
            asset_address=asset_address,
            days=days
        )
        
        # Get repository for trade details
        trade_detail_repo = self.model_db_manager.get_trade_detail_repo()
        
        
        try:
            # Calculate direct pricing using repository method
            with self.model_db_manager.get_session() as session:
                results = trade_detail_repo.calculate_direct_pricing(
                    session,
                    asset_address=asset_address,
                    days_back=days
                )
                
                session.commit()
            
            log_with_context(
                self.logger, INFO, "Trade pricing calculation complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to calculate trade pricing",
                asset_address=asset_address,
                error=str(e)
            )
            return {'trades_priced': 0, 'errors': 1}

    # =====================================================================
    # CANONICAL PRICING METHODS (NEW)
    # =====================================================================

    def generate_canonical_prices(
        self, 
        timestamp_minutes: List[int], 
        asset_address: str,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Generate 5-minute VWAP canonical prices from pricing pools.
        
        Creates price_vwap records representing canonical price authority.
        Uses pricing pools (pricing_pool=True) to calculate volume-weighted prices.
        
        Logic:
        1. Find all pools configured as pricing pools for the asset
        2. Get pool_swap_details from pricing pools for each minute 
        3. Calculate volume-weighted price for that minute
        4. Calculate 5-minute trailing VWAP (current + 4 previous minutes)
        5. Create price_vwap records for USD and/or AVAX denominations
        6. Return creation statistics
        
        Args:
            timestamp_minutes: List of minute timestamps to generate prices for
            asset_address: Target asset address to generate canonical prices for
            denomination: USD, AVAX, or None for both (default: both)
            
        Returns:
            Dict with canonical price creation statistics
        """
        log_with_context(
            self.logger, INFO, "Generating canonical prices",
            asset_address=asset_address,
            minute_count=len(timestamp_minutes),
            denomination=denomination.value if denomination else "both"
        )
        
        # Determine which denominations to process
        denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
        
        try:
            results = {'prices_created': 0, 'errors': 0, 'minutes_processed': 0}
            
            # Get pricing pool configurations and repositories
            pool_pricing_repo = self.shared_db_manager.get_pool_pricing_config_repo()
            pool_swap_detail_repo = self.model_db_manager.get_pool_swap_detail_repo()
            price_vwap_repo = self.shared_db_manager.get_price_vwap_repository()
            
            with self.shared_db_manager.get_session() as shared_session:
                with self.model_db_manager.get_session() as model_session:
                    
                    # Get model ID for pool pricing config lookup
                    model = shared_session.query(Model).filter(
                        Model.database_name == self.model_db_manager.config.url.split('/')[-1]
                    ).first()
                    
                    if not model:
                        log_with_context(
                            self.logger, ERROR, "Could not find model for database",
                            database_name=self.model_db_manager.config.url.split('/')[-1]
                        )
                        return {'prices_created': 0, 'errors': 1, 'minutes_processed': 0}
                    
                    # Process each timestamp minute
                    for timestamp_minute in timestamp_minutes:
                        try:
                            results['minutes_processed'] += 1
                            
                            # Convert timestamp to datetime for period lookup
                            minute_dt = datetime.fromtimestamp(timestamp_minute, tz=timezone.utc)
                            
                            # Find pricing pools for this asset and timestamp
                            pricing_pools = pool_pricing_repo.get_pricing_pools_for_model(
                                shared_session, model.id, timestamp_minute
                            )
                            
                            # Filter pools where this asset is the base token
                            asset_pricing_pools = []
                            for pool_config in pricing_pools:
                                contract = shared_session.query(Contract).filter(
                                    Contract.id == pool_config.contract_id
                                ).first()
                                
                                if contract and contract.base_token_address and contract.base_token_address.lower() == asset_address.lower():
                                    asset_pricing_pools.append(pool_config)
                            
                            if not asset_pricing_pools:
                                log_with_context(
                                    self.logger, DEBUG, "No pricing pools found for asset at timestamp",
                                    asset_address=asset_address,
                                    timestamp_minute=timestamp_minute
                                )
                                continue
                            
                            # Process each denomination
                            for denom in denominations:
                                try:
                                    # Get all pool swap details from pricing pools for this minute
                                    swap_details = []
                                    for pool_config in asset_pricing_pools:
                                        details = pool_swap_detail_repo.get_details_for_pool_and_minute(
                                            model_session,
                                            pool_config.contract_id,
                                            timestamp_minute,
                                            denom
                                        )
                                        swap_details.extend(details)
                                    
                                    if not swap_details:
                                        log_with_context(
                                            self.logger, DEBUG, "No swap details found for minute",
                                            asset_address=asset_address,
                                            timestamp_minute=timestamp_minute,
                                            denomination=denom.value
                                        )
                                        continue
                                    
                                    # Calculate volume-weighted price for this minute
                                    total_volume = Decimal('0')
                                    weighted_price_sum = Decimal('0')
                                    
                                    for detail in swap_details:
                                        volume = Decimal(str(detail.base_amount))  # Volume in base token
                                        price = Decimal(str(detail.price))  # Price per base token
                                        
                                        total_volume += volume
                                        weighted_price_sum += (volume * price)
                                    
                                    if total_volume == 0:
                                        log_with_context(
                                            self.logger, DEBUG, "Zero volume for minute",
                                            asset_address=asset_address,
                                            timestamp_minute=timestamp_minute,
                                            denomination=denom.value
                                        )
                                        continue
                                    
                                    # Calculate minute price (volume-weighted average)
                                    minute_price = weighted_price_sum / total_volume
                                    
                                    # Calculate 5-minute trailing VWAP
                                    # Get previous 4 minutes of canonical prices
                                    vwap_prices = []
                                    vwap_volumes = []
                                    
                                    for i in range(5):  # Current minute + 4 previous
                                        lookup_timestamp = timestamp_minute - (i * 60)
                                        
                                        if i == 0:
                                            # Current minute - use calculated values
                                            vwap_prices.append(minute_price)
                                            vwap_volumes.append(total_volume)
                                        else:
                                            # Previous minutes - lookup existing canonical prices
                                            existing_price = price_vwap_repo.get_canonical_price(
                                                shared_session,
                                                asset_address,
                                                lookup_timestamp,
                                                denom
                                            )
                                            
                                            if existing_price:
                                                vwap_prices.append(Decimal(str(existing_price.price_period)))
                                                vwap_volumes.append(Decimal(str(existing_price.base_volume)))
                                    
                                    # Calculate 5-minute VWAP
                                    if len(vwap_prices) >= 1:  # At least current minute
                                        total_vwap_volume = sum(vwap_volumes)
                                        weighted_vwap_sum = sum(price * volume for price, volume in zip(vwap_prices, vwap_volumes))
                                        
                                        if total_vwap_volume > 0:
                                            vwap_price = weighted_vwap_sum / total_vwap_volume
                                        else:
                                            vwap_price = minute_price
                                    else:
                                        vwap_price = minute_price
                                    
                                    # Calculate quote volume for storage
                                    quote_volume = total_volume * minute_price
                                    
                                    # Create canonical price record
                                    canonical_price = price_vwap_repo.create_canonical_price(
                                        shared_session,
                                        asset_address,
                                        timestamp_minute,
                                        denom,
                                        total_volume,      # base_volume
                                        quote_volume,      # quote_volume
                                        minute_price,      # price_period
                                        vwap_price         # price_vwap (5-minute VWAP)
                                    )
                                    
                                    if canonical_price:
                                        results['prices_created'] += 1
                                        log_with_context(
                                            self.logger, DEBUG, "Canonical price created",
                                            asset_address=asset_address,
                                            timestamp_minute=timestamp_minute,
                                            denomination=denom.value,
                                            minute_price=str(minute_price),
                                            vwap_price=str(vwap_price),
                                            volume=str(total_volume)
                                        )
                                    
                                except Exception as e:
                                    results['errors'] += 1
                                    log_with_context(
                                        self.logger, ERROR, "Error processing denomination",
                                        asset_address=asset_address,
                                        timestamp_minute=timestamp_minute,
                                        denomination=denom.value,
                                        error=str(e)
                                    )
                        
                        except Exception as e:
                            results['errors'] += 1
                            log_with_context(
                                self.logger, ERROR, "Error processing timestamp minute",
                                asset_address=asset_address,
                                timestamp_minute=timestamp_minute,
                                error=str(e)
                            )
                    
                    # Commit all changes
                    shared_session.commit()
                    
            log_with_context(
                self.logger, INFO, "Canonical price generation complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to generate canonical prices",
                asset_address=asset_address,
                error=str(e)
            )
            return {'prices_created': 0, 'errors': 1, 'minutes_processed': 0}

    def apply_canonical_pricing_to_global_events(
        self, 
        block_numbers: List[int], 
        asset_address: str,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Apply canonical pricing to events that couldn't be directly priced.
        
        Finds pool swaps and trades without direct pricing and applies
        canonical prices from price_vwap table to create global pricing.
        
        Logic:
        1. Find pool_swaps without direct pricing (no pool_swap_details)
        2. Find trades without direct pricing (pricing_method != 'DIRECT')
        3. Use price_vwap to calculate pricing for these events
        4. Create pool_swap_details and trade_details with pricing_method = 'GLOBAL'
        5. Return pricing statistics
        
        Args:
            block_numbers: List of blocks to process for global pricing
            asset_address: Target asset address to apply pricing for
            denomination: USD, AVAX, or None for both (default: both)
            
        Returns:
            Dict with global pricing application statistics
        """
        log_with_context(
            self.logger, INFO, "Applying canonical pricing to global events",
            asset_address=asset_address,
            block_count=len(block_numbers),
            denomination=denomination.value if denomination else "both"
        )
        
        # Determine which denominations to process
        denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
        
        try:
            results = {
                'swaps_priced': 0, 
                'trades_priced': 0, 
                'errors': 0, 
                'blocks_processed': 0
            }
            
            # Get repositories
            pool_swap_detail_repo = self.model_db_manager.get_pool_swap_detail_repo()
            price_vwap_repo = self.shared_db_manager.get_price_vwap_repository()
            trade_detail_repo = self.model_db_manager.get_trade_detail_repo()
            
            with self.shared_db_manager.get_session() as shared_session:
                with self.model_db_manager.get_session() as model_session:

                    # Process each block
                    for block_number in block_numbers:
                        try:
                            results['blocks_processed'] += 1
                            
                            # Find pool swaps without direct pricing in this block
                            unpriced_swaps = model_session.query(PoolSwap).filter(
                                and_(
                                    PoolSwap.block_number == block_number,
                                    PoolSwap.base_token == asset_address.lower(),
                                    ~exists().where(
                                        and_(
                                            PoolSwapDetail.content_id == PoolSwap.content_id,
                                            PoolSwapDetail.price_method.in_([
                                                PricingMethod.DIRECT_AVAX,
                                                PricingMethod.DIRECT_USD
                                            ])
                                        )
                                    )
                                )
                            ).all()
                            
                            # Find trades without direct pricing in this block
                            unpriced_trades = model_session.query(Trade).filter(
                                and_(
                                    Trade.block_number == block_number,
                                    Trade.base_token == asset_address.lower(),
                                    ~exists().where(
                                        and_(
                                            TradeDetail.content_id == Trade.content_id,
                                            TradeDetail.price_method == TradePricingMethod.DIRECT
                                        )
                                    )
                                )
                            ).all()
                            
                            # Process unpriced swaps
                            for swap in unpriced_swaps:
                                try:
                                    # Get minute timestamp for canonical price lookup
                                    swap_minute = (swap.timestamp // 60) * 60
                                    
                                    # Process each denomination
                                    for denom in denominations:
                                        # Get canonical price for this minute
                                        canonical_price = price_vwap_repo.get_canonical_price(
                                            shared_session,
                                            asset_address,
                                            swap_minute,
                                            denom
                                        )
                                        
                                        if not canonical_price:
                                            log_with_context(
                                                self.logger, DEBUG, "No canonical price found for swap",
                                                content_id=str(swap.content_id),
                                                timestamp=swap.timestamp,
                                                denomination=denom.value
                                            )
                                            continue
                                        
                                        # Calculate swap valuation using canonical price
                                        base_amount = Decimal(str(swap.base_amount_human))
                                        price_per_token = Decimal(str(canonical_price.price_vwap))
                                        total_value = base_amount * price_per_token
                                        
                                        # Create pool swap detail with global pricing
                                        swap_detail = pool_swap_detail_repo.create_swap_detail(
                                            model_session,
                                            swap.content_id,
                                            denom,
                                            base_amount,
                                            price_per_token,
                                            total_value,
                                            PricingMethod.GLOBAL
                                        )
                                        
                                        if swap_detail:
                                            results['swaps_priced'] += 1
                                    
                                except Exception as e:
                                    results['errors'] += 1
                                    log_with_context(
                                        self.logger, ERROR, "Error pricing swap",
                                        content_id=str(swap.content_id),
                                        error=str(e)
                                    )
                            
                            # Process unpriced trades
                            for trade in unpriced_trades:
                                try:
                                    # Get minute timestamp for canonical price lookup
                                    trade_minute = (trade.timestamp // 60) * 60
                                    
                                    # Process each denomination
                                    for denom in denominations:
                                        # Get canonical price for this minute
                                        canonical_price = price_vwap_repo.get_canonical_price(
                                            shared_session,
                                            asset_address,
                                            trade_minute,
                                            denom
                                        )
                                        
                                        if not canonical_price:
                                            log_with_context(
                                                self.logger, DEBUG, "No canonical price found for trade",
                                                content_id=str(trade.content_id),
                                                timestamp=trade.timestamp,
                                                denomination=denom.value
                                            )
                                            continue
                                        
                                        # Calculate trade valuation using canonical price
                                        base_amount = Decimal(str(trade.base_amount_human))
                                        price_per_token = Decimal(str(canonical_price.price_vwap))
                                        total_value = base_amount * price_per_token
                                        
                                        # Create trade detail with global pricing
                                        trade_detail = trade_detail_repo.create_trade_detail(
                                            model_session,
                                            trade.content_id,
                                            denom,
                                            total_value,
                                            price_per_token,
                                            TradePricingMethod.GLOBAL
                                        )
                                        
                                        if trade_detail:
                                            results['trades_priced'] += 1
                                    
                                except Exception as e:
                                    results['errors'] += 1
                                    log_with_context(
                                        self.logger, ERROR, "Error pricing trade",
                                        content_id=str(trade.content_id),
                                        error=str(e)
                                    )
                        
                        except Exception as e:
                            results['errors'] += 1
                            log_with_context(
                                self.logger, ERROR, "Error processing block",
                                block_number=block_number,
                                error=str(e)
                            )
                    
                    # Commit all changes
                    model_session.commit()
                    
            log_with_context(
                self.logger, INFO, "Global pricing application complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to apply canonical pricing to global events",
                asset_address=asset_address,
                error=str(e)
            )
            return {'swaps_priced': 0, 'trades_priced': 0, 'errors': 1, 'blocks_processed': 0}

    # =====================================================================
    # CONVENIENCE METHODS (NEW)
    # =====================================================================

    def update_canonical_pricing(
        self, 
        asset_address: str, 
        minutes: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive canonical pricing update for an asset.
        
        Convenience method that generates canonical prices for recent minutes
        and identifies periods that need canonical pricing.
        
        Args:
            asset_address: Target asset address
            minutes: Number of minutes back to process (default: 1440 = 24 hours)
            denomination: USD, AVAX, or None for both (default: both)
            
        Returns:
            Dict with canonical pricing update statistics
        """
        if minutes is None:
            minutes = 1440  # Default to 24 hours
        
        log_with_context(
            self.logger, INFO, "Updating canonical pricing",
            asset_address=asset_address,
            minutes=minutes,
            denomination=denomination.value if denomination else "both"
        )
        
        try:
            # Calculate timestamp range
            current_time = int(datetime.now(timezone.utc).timestamp())
            start_time = current_time - (minutes * 60)
            
            # Generate minute timestamps
            timestamp_minutes = []
            for i in range(minutes):
                minute_timestamp = start_time + (i * 60)
                # Round to minute boundary
                minute_timestamp = (minute_timestamp // 60) * 60
                timestamp_minutes.append(minute_timestamp)
            
            # Generate canonical prices
            results = self.generate_canonical_prices(timestamp_minutes, asset_address, denomination)
            
            log_with_context(
                self.logger, INFO, "Canonical pricing update complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to update canonical pricing",
                asset_address=asset_address,
                error=str(e)
            )
            return {'prices_created': 0, 'errors': 1, 'minutes_processed': 0}

    def update_global_pricing(
        self, 
        asset_address: str, 
        blocks: Optional[List[int]] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive global pricing update for an asset.
        
        Convenience method that applies canonical pricing to unpriced events.
        If no blocks specified, finds recent blocks with unpriced events.
        
        Args:
            asset_address: Target asset address
            blocks: Specific blocks to process (default: auto-detect recent blocks)
            denomination: USD, AVAX, or None for both (default: both)
            
        Returns:
            Dict with global pricing update statistics
        """
        log_with_context(
            self.logger, INFO, "Updating global pricing",
            asset_address=asset_address,
            block_count=len(blocks) if blocks else "auto-detect",
            denomination=denomination.value if denomination else "both"
        )
        
        try:
            if blocks is None:
                # Auto-detect recent blocks with unpriced events
                with self.model_db_manager.get_session() as session:
                    
                    # Look for blocks in last 24 hours with unpriced events
                    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)
                    cutoff_timestamp = int(cutoff_time.timestamp())
                    
                    # Find blocks with unpriced swaps
                    unpriced_swap_blocks = session.query(distinct(PoolSwap.block_number)).filter(
                        and_(
                            PoolSwap.base_token == asset_address.lower(),
                            PoolSwap.timestamp >= cutoff_timestamp,
                            ~exists().where(PoolSwapDetail.content_id == PoolSwap.content_id)
                        )
                    ).all()
                    
                    # Find blocks with unpriced trades
                    unpriced_trade_blocks = session.query(distinct(Trade.block_number)).filter(
                        and_(
                            Trade.base_token == asset_address.lower(),
                            Trade.timestamp >= cutoff_timestamp,
                            ~exists().where(TradeDetail.content_id == Trade.content_id)
                        )
                    ).all()
                    
                    # Combine and deduplicate blocks
                    all_blocks = set()
                    for block_tuple in unpriced_swap_blocks:
                        all_blocks.add(block_tuple[0])
                    for block_tuple in unpriced_trade_blocks:
                        all_blocks.add(block_tuple[0])
                    
                    blocks = sorted(list(all_blocks))
                    
                    log_with_context(
                        self.logger, INFO, "Auto-detected blocks with unpriced events",
                        asset_address=asset_address,
                        block_count=len(blocks)
                    )
            
            if not blocks:
                log_with_context(
                    self.logger, INFO, "No blocks need global pricing",
                    asset_address=asset_address
                )
                return {'swaps_priced': 0, 'trades_priced': 0, 'errors': 0, 'blocks_processed': 0}
            
            # Apply canonical pricing to global events
            results = self.apply_canonical_pricing_to_global_events(blocks, asset_address, denomination)
            
            log_with_context(
                self.logger, INFO, "Global pricing update complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to update global pricing",
                asset_address=asset_address,
                error=str(e)
            )
            return {'swaps_priced': 0, 'trades_priced': 0, 'errors': 1, 'blocks_processed': 0}

    def update_pricing_all(
        self, 
        asset_address: str,
        days: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive pricing update including direct, canonical, and global pricing.
        
        Runs the complete pricing pipeline in proper order:
        1. Direct pricing (existing swap/trade pricing methods)
        2. Canonical price generation from pricing pools  
        3. Global pricing application to unpriced events
        
        Args:
            asset_address: Target asset address
            days: Number of days to look back for gaps (default: 7)
            denomination: USD, AVAX, or None for both (default: both)
            
        Returns:
            Dict with comprehensive pricing statistics
        """
        if days is None:
            days = 7  # Default to 1 week
            
        log_with_context(
            self.logger, INFO, "Starting comprehensive pricing update",
            asset_address=asset_address,
            days=days,
            denomination=denomination.value if denomination else "both"
        )
        
        try:
            total_results = {
                'direct_swaps': 0,
                'direct_trades': 0, 
                'canonical_prices': 0,
                'global_swaps': 0,
                'global_trades': 0,
                'total_errors': 0
            }
            
            # 1. Direct pricing for swaps
            log_with_context(self.logger, INFO, "Phase 1: Direct swap pricing")
            direct_swap_results = self.calculate_swap_pricing(asset_address, days)
            total_results['direct_swaps'] = direct_swap_results.get('swaps_priced', 0)
            total_results['total_errors'] += direct_swap_results.get('errors', 0)
            
            # 2. Direct pricing for trades
            log_with_context(self.logger, INFO, "Phase 2: Direct trade pricing")
            direct_trade_results = self.calculate_trade_pricing(asset_address, days)
            total_results['direct_trades'] = direct_trade_results.get('trades_priced', 0)
            total_results['total_errors'] += direct_trade_results.get('errors', 0)
            
            # 3. Canonical pricing generation
            log_with_context(self.logger, INFO, "Phase 3: Canonical pricing generation")
            minutes = days * 24 * 60 if days else 1440  # Convert days to minutes
            canonical_results = self.update_canonical_pricing(asset_address, minutes, denomination)
            total_results['canonical_prices'] = canonical_results.get('prices_created', 0)
            total_results['total_errors'] += canonical_results.get('errors', 0)
            
            # 4. Global pricing application
            log_with_context(self.logger, INFO, "Phase 4: Global pricing application")
            global_results = self.update_global_pricing(asset_address, None, denomination)
            total_results['global_swaps'] = global_results.get('swaps_priced', 0)
            total_results['global_trades'] = global_results.get('trades_priced', 0)
            total_results['total_errors'] += global_results.get('errors', 0)
            
            log_with_context(
                self.logger, INFO, "Comprehensive pricing update complete",
                asset_address=asset_address,
                **total_results
            )
            
            return total_results
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed comprehensive pricing update",
                asset_address=asset_address,
                error=str(e)
            )
            return {'total_errors': 1}

    # =====================================================================
    # STATUS AND MONITORING METHODS
    # =====================================================================

    def get_pricing_status(self, asset_address: str) -> Dict:
        """
        Get comprehensive pricing status for an asset.
        
        Returns detailed statistics about pricing coverage, gaps, and recent activity.
        
        Args:
            asset_address: Target asset address
            
        Returns:
            Dict with detailed pricing status information
        """
        log_with_context(
            self.logger, INFO, "Getting pricing status",
            asset_address=asset_address
        )
        
        try:
            status = {
                'asset_address': asset_address,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'direct_pricing': {'usd': {}, 'avax': {}},
                'canonical_pricing': {'usd': {}, 'avax': {}},
                'global_pricing': {'usd': {}, 'avax': {}},
                'gaps': {},
                'recent_activity': {}
            }
            
            pool_swap_detail_repo = self.model_db_manager.get_pool_swap_detail_repo()
            price_vwap_repo = self.shared_db_manager.get_price_vwap_repository()
            trade_detail_repo = self.model_db_manager.get_trade_detail_repo()
            
            with self.shared_db_manager.get_session() as shared_session:
                with self.model_db_manager.get_session() as model_session:
                    
                    # Get direct pricing status
                    for denom in [PricingDenomination.USD, PricingDenomination.AVAX]:
                        # Pool swap pricing status
                        swap_stats = pool_swap_detail_repo.get_pricing_stats(
                            model_session, asset_address, denom
                        )
                        status['direct_pricing'][denom.value]['swaps'] = swap_stats
                        
                        # Trade pricing status
                        trade_stats = trade_detail_repo.get_pricing_stats(
                            model_session, asset_address, denom
                        )
                        status['direct_pricing'][denom.value]['trades'] = trade_stats
                        
                        # Canonical pricing status
                        canonical_stats = price_vwap_repo.get_canonical_pricing_stats(
                            shared_session, asset_address, denom
                        )
                        status['canonical_pricing'][denom.value] = canonical_stats
            
            log_with_context(
                self.logger, INFO, "Pricing status retrieved",
                asset_address=asset_address
            )
            
            return status
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to get pricing status",
                asset_address=asset_address,
                error=str(e)
            )
            return {'error': str(e)}