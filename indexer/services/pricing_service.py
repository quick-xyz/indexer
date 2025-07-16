# indexer/services/pricing_service.py

from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from ..core.logging_config import IndexerLogger, log_with_context
from ..database.repository import RepositoryManager
from ..database.connection import DatabaseManager
from ..database.shared.tables.periods import Period, PeriodType
from ..database.shared.repositories.block_prices_repository import BlockPricesRepository
from ..database.shared.repositories.periods_repository import PeriodsRepository
from ..clients.quicknode_rpc import QuickNodeRpcClient
from ..database.indexer.tables.detail.pool_swap_detail import PricingDenomination, PricingMethod

import logging


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
        indexer_db_manager: DatabaseManager,  # Indexer database for event details
        rpc_client: QuickNodeRpcClient,
        repository_manager: RepositoryManager
    ):
        self.shared_db_manager = shared_db_manager  # For block prices, periods, canonical pricing
        self.indexer_db_manager = indexer_db_manager  # For event pricing details
        self.rpc_client = rpc_client
        self.repository_manager = repository_manager
        
        # Direct repository access for frequently used operations
        self.block_prices_repo = BlockPricesRepository(shared_db_manager)
        self.periods_repo = PeriodsRepository(shared_db_manager)
        
        self.logger = IndexerLogger.get_logger('services.pricing_service')
        
        log_with_context(
            self.logger, logging.INFO, "PricingService initialized",
            shared_database=shared_db_manager.config.url.split('/')[-1],
            indexer_database=indexer_db_manager.config.url.split('/')[-1]
        )
    
    def update_periods_to_present(self, period_types: Optional[List[PeriodType]] = None) -> Dict[str, int]:
        """
        Update periods from last recorded period to present time.
        
        This is the main method that would be called by a cron job.
        Uses SHARED database for periods (chain-level time infrastructure).
        
        Args:
            period_types: List of period types to update. If None, updates all types.
            
        Returns:
            Dict with creation counts per period type
        """
        if period_types is None:
            period_types = [PeriodType.ONE_MINUTE, PeriodType.FIVE_MINUTE, PeriodType.ONE_HOUR, PeriodType.ONE_DAY]
        
        log_with_context(
            self.logger, logging.INFO, "Updating periods to present",
            period_types=[pt.value for pt in period_types]
        )
        
        results = {}
        
        for period_type in period_types:
            try:
                created_count = self.periods_repo.create_periods_to_present(
                    period_type=period_type,
                    rpc_client=self.rpc_client
                )
                results[period_type.value] = created_count
                
                log_with_context(
                    self.logger, logging.INFO, "Updated periods",
                    period_type=period_type.value,
                    created_count=created_count
                )
                
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to update periods",
                    period_type=period_type.value,
                    error=str(e)
                )
                results[period_type.value] = 0
        
        return results
    
    def update_minute_prices_to_present(self) -> Dict[str, int]:
        """
        Update minute-by-minute AVAX prices to present time.
        
        Uses SHARED database for block prices (chain-level pricing data).
        
        Returns:
            Dict with creation statistics
        """
        log_with_context(self.logger, logging.INFO, "Updating minute prices to present")
        
        try:
            created_count = self.block_prices_repo.create_minute_prices_to_present(
                rpc_client=self.rpc_client
            )
            
            log_with_context(
                self.logger, logging.INFO, "Updated minute prices",
                created_count=created_count
            )
            
            return {'minute_prices_created': created_count}
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to update minute prices",
                error=str(e)
            )
            return {'minute_prices_created': 0}
    
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
            self.logger, logging.INFO, "Calculating swap pricing",
            asset_address=asset_address,
            days=days
        )
        
        # Get repository for pool swap details
        pool_swap_detail_repo = self.repository_manager.get_pool_swap_detail_repository()
        
        try:
            # Calculate direct pricing using repository method
            results = pool_swap_detail_repo.calculate_direct_pricing(
                asset_address=asset_address,
                days_back=days
            )
            
            log_with_context(
                self.logger, logging.INFO, "Swap pricing calculation complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to calculate swap pricing",
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
            self.logger, logging.INFO, "Calculating trade pricing",
            asset_address=asset_address,
            days=days
        )
        
        # Get repository for trade details
        trade_detail_repo = self.repository_manager.get_trade_detail_repository()
        
        try:
            # Calculate direct pricing using repository method
            results = trade_detail_repo.calculate_direct_pricing(
                asset_address=asset_address,
                days_back=days
            )
            
            log_with_context(
                self.logger, logging.INFO, "Trade pricing calculation complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to calculate trade pricing",
                asset_address=asset_address,
                error=str(e)
            )
            return {'trades_priced': 0, 'errors': 1}

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
        
        Args:
            timestamp_minutes: List of minute timestamps to process
            asset_address: Asset to generate pricing for  
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with creation statistics: {'usd_created': 0, 'avax_created': 0, 'errors': 0}
        """
        log_with_context(
            self.logger, logging.INFO, "Generating canonical prices",
            asset_address=asset_address,
            minutes_count=len(timestamp_minutes),
            denomination=denomination.value if denomination else "both"
        )
        
        results = {'usd_created': 0, 'avax_created': 0, 'errors': 0}
        denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
        
        # Get repositories
        price_vwap_repo = self.repository_manager.get_price_vwap_repository()
        pool_swap_detail_repo = self.repository_manager.get_pool_swap_detail_repository() 
        
        with self.shared_db_manager.get_session() as session:
            for minute_timestamp in timestamp_minutes:
                try:
                    # Calculate 5-minute trailing window (current + 4 previous minutes)
                    end_time = datetime.fromtimestamp(minute_timestamp, tz=timezone.utc)
                    start_time = end_time - timedelta(minutes=4)  # 5-minute window
                    
                    for denom in denominations:
                        # Check if canonical price already exists
                        existing_price = price_vwap_repo.get_canonical_price(
                            session, asset_address, minute_timestamp, denom
                        )
                        if existing_price:
                            continue
                            
                        # Get pricing pool swap details for 5-minute window
                        pricing_swaps = pool_swap_detail_repo.get_pricing_pool_swaps_in_timeframe(
                            session, asset_address, start_time, end_time, denom
                        )
                        
                        if not pricing_swaps:
                            log_with_context(
                                self.logger, logging.WARNING, "No pricing pool data for canonical price",
                                asset_address=asset_address,
                                timestamp_minute=minute_timestamp,
                                denomination=denom.value
                            )
                            continue
                        
                        # Calculate volume-weighted average price
                        total_volume = Decimal('0')
                        total_value = Decimal('0')
                        
                        for swap in pricing_swaps:
                            if denom == PricingDenomination.USD:
                                volume = swap.volume_usd or Decimal('0')
                                price = swap.price_usd or Decimal('0')
                            else:  # AVAX
                                volume = swap.volume_avax or Decimal('0') 
                                price = swap.price_avax or Decimal('0')
                                
                            total_volume += volume
                            total_value += (volume * price)
                        
                        if total_volume == Decimal('0'):
                            log_with_context(
                                self.logger, logging.WARNING, "Zero volume for canonical price calculation",
                                asset_address=asset_address,
                                timestamp_minute=minute_timestamp,
                                denomination=denom.value
                            )
                            continue
                        
                        # Calculate VWAP
                        canonical_price = total_value / total_volume
                        
                        # Create canonical price record
                        price_vwap_repo.create_canonical_price(
                            session,
                            asset_address=asset_address,
                            timestamp_minute=minute_timestamp,
                            denomination=denom,
                            price=canonical_price,
                            volume=total_volume,
                            pool_count=len(set(swap.pool_address for swap in pricing_swaps)),
                            swap_count=len(pricing_swaps)
                        )
                        
                        if denom == PricingDenomination.USD:
                            results['usd_created'] += 1
                        else:
                            results['avax_created'] += 1
                            
                        log_with_context(
                            self.logger, logging.DEBUG, "Created canonical price",
                            asset_address=asset_address,
                            timestamp_minute=minute_timestamp,
                            denomination=denom.value,
                            price=float(canonical_price),
                            volume=float(total_volume)
                        )
                        
                except Exception as e:
                    results['errors'] += 1
                    log_with_context(
                        self.logger, logging.ERROR, "Error generating canonical price",
                        asset_address=asset_address,
                        timestamp_minute=minute_timestamp,
                        error=str(e)
                    )
                    continue
        
        log_with_context(
            self.logger, logging.INFO, "Canonical price generation complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def apply_canonical_pricing_to_global_events(
        self, 
        block_numbers: List[int], 
        asset_address: str,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Apply canonical pricing to events that lack direct pricing.
        
        Finds pool swaps and trades without direct pricing, then applies canonical
        prices from price_vwap table to create detail records with GLOBAL method.
        
        Args:
            block_numbers: List of block numbers to process
            asset_address: Asset to apply global pricing for
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with statistics: {'swaps_priced': 0, 'trades_priced': 0, 'errors': 0}
        """
        log_with_context(
            self.logger, logging.INFO, "Applying canonical pricing to global events",
            asset_address=asset_address,
            blocks_count=len(block_numbers),
            denomination=denomination.value if denomination else "both"
        )
        
        results = {'swaps_priced': 0, 'trades_priced': 0, 'errors': 0}
        denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
        
        # Get repositories
        price_vwap_repo = self.repository_manager.get_price_vwap_repository()
        pool_swap_detail_repo = self.repository_manager.get_pool_swap_detail_repository()
        trade_detail_repo = self.repository_manager.get_trade_detail_repository()
        pool_swap_repo = self.repository_manager.get_pool_swap_repository()
        trade_repo = self.repository_manager.get_trade_repository()
        
        with self.indexer_db_manager.get_session() as indexer_session, \
             self.shared_db_manager.get_session() as shared_session:
            
            for block_number in block_numbers:
                try:
                    # 1. Find pool swaps without direct pricing
                    unpriced_swaps = pool_swap_repo.get_swaps_without_pricing(
                        indexer_session, block_number, asset_address
                    )
                    
                    for swap in unpriced_swaps:
                        # Get canonical price for swap timestamp
                        swap_minute = int(swap.timestamp.timestamp() // 60 * 60)
                        
                        for denom in denominations:
                            canonical_price = price_vwap_repo.get_canonical_price(
                                shared_session, asset_address, swap_minute, denom
                            )
                            
                            if not canonical_price:
                                log_with_context(
                                    self.logger, logging.WARNING, "No canonical price for global swap pricing",
                                    asset_address=asset_address,
                                    block_number=block_number,
                                    timestamp_minute=swap_minute,
                                    denomination=denom.value
                                )
                                continue
                            
                            # Apply canonical pricing to create pool_swap_detail
                            pool_swap_detail_repo.create_global_pricing_detail(
                                indexer_session,
                                swap=swap,
                                denomination=denom,
                                canonical_price=canonical_price.price,
                                pricing_method=PricingMethod.GLOBAL
                            )
                            
                            results['swaps_priced'] += 1
                    
                    # 2. Find trades without direct pricing  
                    unpriced_trades = trade_repo.get_trades_without_direct_pricing(
                        indexer_session, block_number, asset_address
                    )
                    
                    for trade in unpriced_trades:
                        # Get canonical price for trade timestamp
                        trade_minute = int(trade.timestamp.timestamp() // 60 * 60)
                        
                        for denom in denominations:
                            canonical_price = price_vwap_repo.get_canonical_price(
                                shared_session, asset_address, trade_minute, denom
                            )
                            
                            if not canonical_price:
                                log_with_context(
                                    self.logger, logging.WARNING, "No canonical price for global trade pricing",
                                    asset_address=asset_address,
                                    block_number=block_number,
                                    timestamp_minute=trade_minute,
                                    denomination=denom.value
                                )
                                continue
                                
                            # Apply canonical pricing to create trade_detail
                            trade_detail_repo.create_global_pricing_detail(
                                indexer_session,
                                trade=trade,
                                denomination=denom, 
                                canonical_price=canonical_price.price,
                                pricing_method=PricingMethod.GLOBAL
                            )
                            
                            results['trades_priced'] += 1
                            
                except Exception as e:
                    results['errors'] += 1
                    log_with_context(
                        self.logger, logging.ERROR, "Error applying canonical pricing",
                        asset_address=asset_address,
                        block_number=block_number,
                        error=str(e)
                    )
                    continue
        
        log_with_context(
            self.logger, logging.INFO, "Global pricing application complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def update_canonical_pricing(
        self, 
        asset_address: str, 
        minutes: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive canonical pricing update for an asset.
        
        Detects gaps in canonical pricing and generates missing price_vwap records.
        This is the main method for scheduled canonical pricing updates.
        
        Args:
            asset_address: Asset to update canonical pricing for
            minutes: Number of minutes to process (from most recent). If None, processes all gaps
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with statistics: {'usd_created': 0, 'avax_created': 0, 'errors': 0}
        """
        log_with_context(
            self.logger, logging.INFO, "Starting canonical pricing update",
            asset_address=asset_address,
            minutes=minutes,
            denomination=denomination.value if denomination else "both"
        )
        
        # Get repositories for gap detection
        price_vwap_repo = self.repository_manager.get_price_vwap_repository()
        periods_repo = self.repository_manager.get_periods_repository()
        
        with self.shared_db_manager.get_session() as session:
            # Determine time range for processing
            if minutes:
                # Process specific number of minutes from now
                end_time = datetime.now(timezone.utc)
                start_time = end_time - timedelta(minutes=minutes)
                
                # Get 1-minute periods in range
                target_periods = periods_repo.get_periods_in_timeframe(
                    session, start_time, end_time, PeriodType.ONE_MINUTE
                )
                timestamp_minutes = [int(p.timestamp.timestamp()) for p in target_periods]
                
            else:
                # Find gaps in canonical pricing for this asset
                denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
                
                all_gaps = []
                for denom in denominations:
                    gaps = price_vwap_repo.find_canonical_pricing_gaps(
                        session, asset_address, denom
                    )
                    all_gaps.extend(gaps)
                
                # Get unique timestamp minutes that need processing
                timestamp_minutes = sorted(list(set(all_gaps)))
        
        if not timestamp_minutes:
            log_with_context(
                self.logger, logging.INFO, "No canonical pricing gaps found",
                asset_address=asset_address
            )
            return {'usd_created': 0, 'avax_created': 0, 'errors': 0}
        
        log_with_context(
            self.logger, logging.INFO, "Processing canonical pricing gaps",
            asset_address=asset_address,
            gaps_count=len(timestamp_minutes)
        )
        
        # Generate canonical prices for identified gaps
        return self.generate_canonical_prices(timestamp_minutes, asset_address, denomination)

    def update_global_pricing(
        self, 
        asset_address: str, 
        blocks: Optional[List[int]] = None,
        days: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive global pricing update for an asset.
        
        Finds events without direct pricing and applies canonical prices.
        This is the main method for scheduled global pricing updates.
        
        Args:
            asset_address: Asset to update global pricing for  
            blocks: Specific block numbers to process. If None, detects gaps
            days: Number of days to look back for gaps. If None, processes all gaps
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with statistics: {'swaps_priced': 0, 'trades_priced': 0, 'errors': 0}
        """
        log_with_context(
            self.logger, logging.INFO, "Starting global pricing update",
            asset_address=asset_address,
            blocks=len(blocks) if blocks else None,
            days=days,
            denomination=denomination.value if denomination else "both"
        )
        
        if blocks:
            # Process specific blocks provided
            target_blocks = blocks
        else:
            # Find blocks with unpriced events
            pool_swap_repo = self.repository_manager.get_pool_swap_repository()
            trade_repo = self.repository_manager.get_trade_repository()
            
            with self.indexer_db_manager.get_session() as session:
                # Determine time range
                if days:
                    cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
                else:
                    cutoff_time = None
                    
                # Find blocks with unpriced swaps
                unpriced_swap_blocks = pool_swap_repo.get_blocks_with_unpriced_swaps(
                    session, asset_address, cutoff_time
                )
                
                # Find blocks with unpriced trades  
                unpriced_trade_blocks = trade_repo.get_blocks_with_unpriced_trades(
                    session, asset_address, cutoff_time
                )
                
                # Combine and deduplicate
                target_blocks = sorted(list(set(unpriced_swap_blocks + unpriced_trade_blocks)))
        
        if not target_blocks:
            log_with_context(
                self.logger, logging.INFO, "No global pricing gaps found",
                asset_address=asset_address
            )
            return {'swaps_priced': 0, 'trades_priced': 0, 'errors': 0}
        
        log_with_context(
            self.logger, logging.INFO, "Processing global pricing gaps",
            asset_address=asset_address,
            blocks_count=len(target_blocks)
        )
        
        # Apply canonical pricing to unpriced events
        return self.apply_canonical_pricing_to_global_events(target_blocks, asset_address, denomination)

    def update_pricing_all(
        self, 
        asset_address: str,
        days: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive pricing update including direct, canonical, and global pricing.
        
        This is the main entry point for complete pricing updates, following the
        pricing pipeline: infrastructure → direct → canonical → global.
        
        Args:
            asset_address: Asset to update all pricing for
            days: Number of days to look back for gaps. If None, processes all gaps  
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with comprehensive statistics from all pricing operations
        """
        log_with_context(
            self.logger, logging.INFO, "Starting comprehensive pricing update",
            asset_address=asset_address,
            days=days,
            denomination=denomination.value if denomination else "both"
        )
        
        results = {
            'periods_created': 0,
            'block_prices_created': 0,
            'swaps_priced_direct': 0,
            'trades_priced_direct': 0,
            'usd_canonical_created': 0,
            'avax_canonical_created': 0,
            'swaps_priced_global': 0,
            'trades_priced_global': 0,
            'total_errors': 0
        }
        
        try:
            # 1. Infrastructure Updates
            log_with_context(self.logger, logging.INFO, "Updating pricing infrastructure", asset_address=asset_address)
            
            # Update periods to present
            period_results = self.update_periods_to_present()
            results['periods_created'] = sum(period_results.values())
            
            # Update block prices to present  
            price_results = self.update_minute_prices_to_present()
            results['block_prices_created'] = sum(price_results.values())
            
            # 2. Direct Pricing Updates
            log_with_context(self.logger, logging.INFO, "Updating direct pricing", asset_address=asset_address)
            
            # Update swap direct pricing
            swap_results = self.calculate_swap_pricing(asset_address, days)
            results['swaps_priced_direct'] = swap_results.get('swaps_priced', 0)
            results['total_errors'] += swap_results.get('errors', 0)
            
            # Update trade direct pricing
            trade_results = self.calculate_trade_pricing(asset_address, days) 
            results['trades_priced_direct'] = trade_results.get('trades_priced', 0)
            results['total_errors'] += trade_results.get('errors', 0)
            
            # 3. Canonical Pricing Updates
            log_with_context(self.logger, logging.INFO, "Updating canonical pricing", asset_address=asset_address)
            
            canonical_results = self.update_canonical_pricing(asset_address, denomination=denomination)
            results['usd_canonical_created'] = canonical_results.get('usd_created', 0)
            results['avax_canonical_created'] = canonical_results.get('avax_created', 0)
            results['total_errors'] += canonical_results.get('errors', 0)
            
            # 4. Global Pricing Updates  
            log_with_context(self.logger, logging.INFO, "Updating global pricing", asset_address=asset_address)
            
            global_results = self.update_global_pricing(asset_address, days=days, denomination=denomination)
            results['swaps_priced_global'] = global_results.get('swaps_priced', 0)
            results['trades_priced_global'] = global_results.get('trades_priced', 0)
            results['total_errors'] += global_results.get('errors', 0)
            
        except Exception as e:
            results['total_errors'] += 1
            log_with_context(
                self.logger, logging.ERROR, "Error in comprehensive pricing update",
                asset_address=asset_address,
                error=str(e)
            )
            raise
        
        log_with_context(
            self.logger, logging.INFO, "Comprehensive pricing update complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def get_pricing_status(self, asset_address: str) -> Dict[str, any]:
        """
        Get comprehensive pricing status for an asset.
        
        Provides detailed statistics about pricing coverage and gaps for monitoring.
        
        Args:
            asset_address: Asset to check pricing status for
            
        Returns:
            Dict with comprehensive pricing statistics and gap information
        """
        log_with_context(
            self.logger, logging.INFO, "Getting pricing status",
            asset_address=asset_address
        )
        
        # Get repositories
        price_vwap_repo = self.repository_manager.get_price_vwap_repository()
        pool_swap_detail_repo = self.repository_manager.get_pool_swap_detail_repository()
        trade_detail_repo = self.repository_manager.get_trade_detail_repository()
        pool_swap_repo = self.repository_manager.get_pool_swap_repository()
        trade_repo = self.repository_manager.get_trade_repository()
        
        status = {
            'asset_address': asset_address,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'canonical_pricing': {},
            'direct_pricing': {},
            'global_pricing': {},
            'gaps': {}
        }
        
        with self.shared_db_manager.get_session() as shared_session, \
             self.indexer_db_manager.get_session() as indexer_session:
            
            # Canonical pricing status
            for denom in [PricingDenomination.USD, PricingDenomination.AVAX]:
                canonical_stats = price_vwap_repo.get_canonical_pricing_stats(
                    shared_session, asset_address, denom
                )
                status['canonical_pricing'][denom.value] = canonical_stats
                
                # Find canonical pricing gaps  
                gaps = price_vwap_repo.find_canonical_pricing_gaps(
                    shared_session, asset_address, denom, limit=10
                )
                status['gaps'][f'canonical_{denom.value}'] = len(gaps)
            
            # Direct pricing status
            direct_swap_stats = pool_swap_detail_repo.get_direct_pricing_stats(
                indexer_session, asset_address
            )
            status['direct_pricing']['swaps'] = direct_swap_stats
            
            direct_trade_stats = trade_detail_repo.get_direct_pricing_stats(
                indexer_session, asset_address  
            )
            status['direct_pricing']['trades'] = direct_trade_stats
            
            # Global pricing status and gaps
            unpriced_swap_count = pool_swap_repo.count_unpriced_swaps(
                indexer_session, asset_address
            )
            status['global_pricing']['unpriced_swaps'] = unpriced_swap_count
            
            unpriced_trade_count = trade_repo.count_unpriced_trades(
                indexer_session, asset_address
            )
            status['global_pricing']['unpriced_trades'] = unpriced_trade_count
            
            # Recent activity
            status['recent_activity'] = {
                'last_canonical_price': price_vwap_repo.get_latest_canonical_price_timestamp(
                    shared_session, asset_address
                ),
                'last_direct_swap_pricing': pool_swap_detail_repo.get_latest_pricing_timestamp(
                    indexer_session, asset_address
                ),
                'last_direct_trade_pricing': trade_detail_repo.get_latest_pricing_timestamp(
                    indexer_session, asset_address
                )
            }
        
        return status