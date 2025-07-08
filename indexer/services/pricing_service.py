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
    
    Currently handles:
    - Period table population using QuickNode block-timestamp lookup
    - Time-based AVAX price population (every minute)
    - Gap detection and backfilling for periods and prices
    
    Future expansion will include:
    - OHLCV candle generation
    - Volume-weighted average price (VWAP) calculations
    - Canonical price table management
    """
    
    def __init__(
        self,
        shared_db_manager: DatabaseManager,  # Shared database for prices and periods
        rpc_client: QuickNodeRpcClient
    ):
        self.shared_db_manager = shared_db_manager  # For block prices and periods
        self.rpc_client = rpc_client
        
        # All repositories use shared database
        self.block_prices_repo = BlockPricesRepository(shared_db_manager)
        self.periods_repo = PeriodsRepository(shared_db_manager)
        
        self.logger = IndexerLogger.get_logger('services.pricing_service')
        
        log_with_context(
            self.logger, logging.INFO, "PricingService initialized",
            shared_database=shared_db_manager.config.url.split('/')[-1]
        )
    
    def update_periods_to_present(self, period_types: Optional[List[PeriodType]] = None) -> Dict[str, int]:
        """
        Update periods from last recorded period to present time.
        
        This is the main method that would be called by a cron job.
        Uses SHARED database for periods (chain-level time infrastructure).
        
        Args:
            period_types: List of period types to update. If None, updates all types.
            
        Returns:
            Dict with statistics about periods created
        """
        log_with_context(
            self.logger, logging.INFO, "Starting period update to present"
        )
        
        if period_types is None:
            period_types = list(PeriodType)
        
        stats = {
            'total_periods_created': 0,
            'periods_by_type': {},
            'errors': []
        }
        
        try:
            # Get current blockchain state
            latest_block_number = self.rpc_client.get_latest_block_number()
            latest_block = self.rpc_client.get_block(latest_block_number)
            current_timestamp = latest_block['timestamp']
            
            # Process each period type using shared database
            with self.shared_db_manager.get_session() as session:
                for period_type in period_types:
                    try:
                        periods_created = self._update_periods_for_type(
                            session, period_type, current_timestamp, latest_block_number
                        )
                        stats['periods_by_type'][period_type.value] = periods_created
                        stats['total_periods_created'] += periods_created
                        
                        log_with_context(
                            self.logger, logging.INFO, "Period type update completed",
                            period_type=period_type.value,
                            periods_created=periods_created
                        )
                        
                    except Exception as e:
                        error_msg = f"Failed to update {period_type.value}: {str(e)}"
                        stats['errors'].append(error_msg)
                        
                        log_with_context(
                            self.logger, logging.ERROR, "Period type update failed",
                            period_type=period_type.value,
                            error=str(e)
                        )
                
                session.commit()
            
            log_with_context(
                self.logger, logging.INFO, "Period update completed",
                **{k: v for k, v in stats.items() if k != 'errors'},
                error_count=len(stats['errors'])
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Period update failed",
                error=str(e)
            )
            stats['errors'].append(f"Critical failure: {str(e)}")
            return stats
    
    def update_minute_prices_to_present(self) -> Dict[str, int]:
        """
        Update minute-by-minute AVAX prices from last recorded price to present.
        
        This populates the block_prices table with time-based pricing data.
        Uses SHARED database for block prices (chain-level data).
        
        Returns:
            Dict with statistics about prices created
        """
        log_with_context(
            self.logger, logging.INFO, "Starting minute price update to present"
        )
        
        stats = {
            'prices_created': 0,
            'prices_skipped': 0,
            'latest_block_processed': None,
            'errors': []
        }
        
        try:
            # Get current blockchain state
            latest_block_number = self.rpc_client.get_latest_block_number()
            latest_block = self.rpc_client.get_block(latest_block_number)
            current_timestamp = latest_block['timestamp']
            
            stats['latest_block_processed'] = latest_block_number
            
            # Get the last price record to determine where to start
            # Use shared database session
            with self.shared_db_manager.get_session() as session:
                last_price = self.block_prices_repo.get_latest_price(session)
                
                if last_price:
                    # Start from the next minute after the last price
                    start_timestamp = last_price.timestamp + 60
                    # Round down to minute boundary
                    start_timestamp = (start_timestamp // 60) * 60
                    log_with_context(
                        self.logger, logging.DEBUG, "Found existing prices, continuing from last",
                        last_price_timestamp=last_price.timestamp,
                        start_timestamp=start_timestamp
                    )
                else:
                    # No existing prices, start from a reasonable point (e.g., 24 hours ago)
                    start_timestamp = current_timestamp - (24 * 3600)  # 24 hours ago
                    start_timestamp = (start_timestamp // 60) * 60  # Round to minute
                    log_with_context(
                        self.logger, logging.INFO, "No existing prices found, starting from 24 hours ago",
                        start_timestamp=start_timestamp
                    )
                
                # Generate minute prices from start_timestamp to current_timestamp
                prices_created, prices_skipped = self._generate_minute_prices_for_range(
                    session, start_timestamp, current_timestamp, latest_block_number
                )
                
                session.commit()  # Commit all price changes
                
                stats['prices_created'] = prices_created
                stats['prices_skipped'] = prices_skipped
            
            log_with_context(
                self.logger, logging.INFO, "Minute price update completed",
                **stats
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Minute price update failed",
                error=str(e)
            )
            stats['errors'].append(f"Critical failure: {str(e)}")
            return stats
    
    def _update_periods_for_type(
        self, 
        session,
        period_type: PeriodType, 
        current_timestamp: int, 
        latest_block_number: int
    ) -> int:
        """Update periods for a specific period type (uses shared database)"""
        # Find the last recorded period
        last_period = self.periods_repo.get_latest_period(session, period_type)
        
        if last_period and last_period.time_close:
            start_time = last_period.time_close + period_type.seconds()
        else:
            # Start from a reasonable default (e.g., 7 days ago)
            start_time = current_timestamp - (7 * 24 * 3600)
            start_time = self._round_to_period_boundary(start_time, period_type)
        
        # Generate periods from start_time to current_timestamp
        periods_created = 0
        time_cursor = start_time
        
        while time_cursor < current_timestamp:
            period_end = time_cursor + period_type.seconds()
            
            # Find block range for this period
            start_block = self._find_block_by_timestamp(time_cursor, latest_block_number)
            end_block = self._find_block_by_timestamp(period_end, latest_block_number)
            
            if start_block and end_block:
                period = self.periods_repo.create_period(
                    session=session,
                    period_type=period_type,
                    time_open=time_cursor,
                    time_close=period_end,
                    block_open=start_block,
                    block_close=end_block
                )
                
                if period:
                    periods_created += 1
            
            time_cursor = period_end
        
        return periods_created
    
    def _generate_minute_prices_for_range(
        self,
        session,  # Shared database session
        start_timestamp: int,
        end_timestamp: int,
        latest_block_number: int
    ) -> Tuple[int, int]:
        """Generate minute-by-minute prices for a timestamp range (uses shared database)"""
        
        prices_created = 0
        prices_skipped = 0
        current_timestamp = start_timestamp
        
        log_with_context(
            self.logger, logging.DEBUG, "Generating minute prices for range",
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )
        
        while current_timestamp < end_timestamp:
            try:
                # Check if price already exists for this minute
                existing_price = self.block_prices_repo.get_price_near_timestamp(
                    session, current_timestamp, tolerance_seconds=30
                )
                
                if existing_price:
                    log_with_context(
                        self.logger, logging.DEBUG, "Price already exists for timestamp",
                        timestamp=current_timestamp
                    )
                    prices_skipped += 1
                    current_timestamp += 60  # Next minute
                    continue
                
                # Find the block closest to this timestamp
                block_number = self._find_block_by_timestamp(
                    current_timestamp, latest_block_number, find_after=False
                )
                
                if block_number is None:
                    log_with_context(
                        self.logger, logging.WARNING, "Could not find block for timestamp",
                        timestamp=current_timestamp
                    )
                    current_timestamp += 60
                    continue
                
                # Fetch price at this block
                price_usd = self.rpc_client.get_chainlink_price_at_block(block_number)
                
                if price_usd is None:
                    log_with_context(
                        self.logger, logging.WARNING, "Could not fetch price for block",
                        block_number=block_number,
                        timestamp=current_timestamp
                    )
                    # Create a placeholder record so we don't keep trying
                    placeholder_price = self.block_prices_repo.create_block_price(
                        session=session,
                        block_number=block_number,
                        timestamp=current_timestamp,
                        price_usd=Decimal('0')  # Placeholder
                    )
                    current_timestamp += 60
                    continue
                
                # Create the price record
                price_record = self.block_prices_repo.create_block_price(
                    session=session,
                    block_number=block_number,
                    timestamp=current_timestamp,
                    price_usd=price_usd
                )
                
                if price_record:
                    prices_created += 1
                    log_with_context(
                        self.logger, logging.DEBUG, "Minute price created",
                        timestamp=current_timestamp,
                        block_number=block_number,
                        price_usd=str(price_usd)
                    )
                else:
                    prices_skipped += 1
                
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to create minute price",
                    timestamp=current_timestamp,
                    error=str(e)
                )
            
            # Move to next minute
            current_timestamp += 60
        
        # Flush all prices (commit happens at the caller level)
        session.flush()
        
        log_with_context(
            self.logger, logging.INFO, "Minute price generation completed",
            prices_created=prices_created,
            prices_skipped=prices_skipped
        )
        
        return prices_created, prices_skipped
    
    def _find_block_by_timestamp(
        self, 
        target_timestamp: int, 
        max_block: int,
        find_after: bool = True
    ) -> Optional[int]:
        """
        Find block number closest to a target timestamp using binary search.
        
        Args:
            target_timestamp: Target timestamp to find
            max_block: Maximum block number to search
            find_after: If True, find block at or after timestamp. If False, find block at or before.
            
        Returns:
            Block number or None if not found
        """
        try:
            min_block = max(0, max_block - 100000)  # Search within reasonable range
            
            # Binary search for the target timestamp
            while min_block <= max_block:
                mid_block = (min_block + max_block) // 2
                
                try:
                    block_data = self.rpc_client.get_block(mid_block)
                    block_timestamp = block_data['timestamp']
                    
                    if block_timestamp == target_timestamp:
                        return mid_block
                    elif block_timestamp < target_timestamp:
                        min_block = mid_block + 1
                    else:
                        max_block = mid_block - 1
                        
                except Exception as e:
                    log_with_context(
                        self.logger, logging.WARNING, "Failed to get block during timestamp search",
                        block_number=mid_block,
                        error=str(e)
                    )
                    # Narrow search range and continue
                    if mid_block < target_timestamp:
                        min_block = mid_block + 1
                    else:
                        max_block = mid_block - 1
            
            # Return the closest block based on find_after preference
            if find_after:
                return min_block if min_block <= max_block + 100 else None
            else:
                return max_block if max_block >= 0 else None
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block timestamp search failed",
                target_timestamp=target_timestamp,
                max_block=max_block,
                error=str(e)
            )
            return None
    
    def _round_to_period_boundary(self, timestamp: int, period_type: PeriodType) -> int:
        """Round timestamp to the nearest period boundary"""
        period_seconds = period_type.seconds()
        return (timestamp // period_seconds) * period_seconds
    
    def backfill_periods(
        self, 
        period_type: PeriodType, 
        days_back: int = 7
    ) -> Dict[str, int]:
        """
        Backfill periods for a specific time range.
        
        Args:
            period_type: Type of period to backfill
            days_back: Number of days to backfill
            
        Returns:
            Dict with backfill statistics
        """
        log_with_context(
            self.logger, logging.INFO, "Starting period backfill",
            period_type=period_type.value,
            days_back=days_back
        )
        
        stats = {
            'periods_created': 0,
            'periods_skipped': 0,
            'errors': []
        }
        
        try:
            # Calculate time range
            latest_block_number = self.rpc_client.get_latest_block_number()
            latest_block = self.rpc_client.get_block(latest_block_number)
            end_timestamp = latest_block['timestamp']
            start_timestamp = end_timestamp - (days_back * 24 * 3600)
            
            # Round to period boundaries
            start_timestamp = self._round_to_period_boundary(start_timestamp, period_type)
            end_timestamp = self._round_to_period_boundary(end_timestamp, period_type)
            
            with self.shared_db_manager.get_session() as session:
                time_cursor = start_timestamp
                
                while time_cursor < end_timestamp:
                    period_end = time_cursor + period_type.seconds()
                    
                    # Check if period already exists
                    existing_period = self.periods_repo.get_period(session, period_type, time_cursor)
                    
                    if existing_period:
                        stats['periods_skipped'] += 1
                        time_cursor = period_end
                        continue
                    
                    # Find block range for this period
                    start_block = self._find_block_by_timestamp(time_cursor, latest_block_number)
                    end_block = self._find_block_by_timestamp(period_end, latest_block_number)
                    
                    if start_block and end_block:
                        period = self.periods_repo.create_period(
                            session=session,
                            period_type=period_type,
                            time_open=time_cursor,
                            time_close=period_end,
                            block_open=start_block,
                            block_close=end_block
                        )
                        
                        if period:
                            stats['periods_created'] += 1
                        else:
                            stats['periods_skipped'] += 1
                    
                    time_cursor = period_end
                
                session.commit()
            
            log_with_context(
                self.logger, logging.INFO, "Period backfill completed",
                **stats
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Period backfill failed",
                error=str(e)
            )
            stats['errors'].append(f"Backfill failed: {str(e)}")
            return stats
    
    def backfill_minute_prices(self, days_back: int = 1) -> Dict[str, int]:
        """
        Backfill minute-by-minute AVAX prices for a specific time range.
        
        Args:
            days_back: Number of days to backfill
            
        Returns:
            Dict with backfill statistics
        """
        log_with_context(
            self.logger, logging.INFO, "Starting minute price backfill",
            days_back=days_back
        )
        
        stats = {
            'prices_created': 0,
            'prices_skipped': 0,
            'errors': []
        }
        
        try:
            # Calculate time range
            latest_block_number = self.rpc_client.get_latest_block_number()
            latest_block = self.rpc_client.get_block(latest_block_number)
            end_timestamp = latest_block['timestamp']
            start_timestamp = end_timestamp - (days_back * 24 * 3600)
            
            # Round to minute boundaries
            start_timestamp = (start_timestamp // 60) * 60
            end_timestamp = (end_timestamp // 60) * 60
            
            # Use shared database for block prices
            with self.shared_db_manager.get_session() as session:
                prices_created, prices_skipped = self._generate_minute_prices_for_range(
                    session, start_timestamp, end_timestamp, latest_block_number
                )
                
                session.commit()
                
                stats['prices_created'] = prices_created
                stats['prices_skipped'] = prices_skipped
            
            log_with_context(
                self.logger, logging.INFO, "Minute price backfill completed",
                **stats
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Minute price backfill failed",
                error=str(e)
            )
            stats['errors'].append(f"Backfill failed: {str(e)}")
            return stats
    
    def get_pricing_status(self) -> Dict:
        """
        Get status of pricing data (both periods and block prices).
        
        Returns:
            Dict with comprehensive pricing status
        """
        status = {
            'periods': {},
            'block_prices': {},
            'blockchain': {}
        }
        
        try:
            # Get blockchain status
            latest_block_number = self.rpc_client.get_latest_block_number()
            latest_block = self.rpc_client.get_block(latest_block_number)
            
            status['blockchain'] = {
                'latest_block': latest_block_number,
                'latest_timestamp': latest_block['timestamp'],
                'latest_datetime': datetime.fromtimestamp(latest_block['timestamp']).isoformat()
            }
            
            # Get periods and prices status (shared database)
            with self.shared_db_manager.get_session() as session:
                # Get periods status
                for period_type in PeriodType:
                    latest_period = self.periods_repo.get_latest_period(session, period_type)
                    period_count = len(self.periods_repo.get_periods_by_type(session, period_type, limit=1000))
                    
                    status['periods'][period_type.value] = {
                        'total_periods': period_count,
                        'latest_period_time': latest_period.time_open if latest_period else None,
                        'latest_period_block': latest_period.block_open if latest_period else None
                    }
                
                # Get block prices status
                price_stats = self.block_prices_repo.get_price_stats(session)
                status['block_prices'] = price_stats
            
            log_with_context(
                self.logger, logging.DEBUG, "Pricing status retrieved successfully"
            )
            
            return status
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to get pricing status",
                error=str(e)
            )
            status['error'] = str(e)
            return status


    def calculate_swap_pricing(
        self,
        indexer_session,  # Session for indexer database (pool swaps)
        shared_session,   # Session for shared database (configs, block prices)
        pool_swap_details_repo,  # PoolSwapDetailRepository 
        pool_pricing_config_repo,  # PoolPricingConfigRepository
        swap_content_id,
        pool_address: str,
        base_amount: Decimal,  # Amount of base token in swap
        quote_amount: Decimal,  # Amount of quote token in swap
        block_number: int,
        model_id: int,
        contract_id: int
    ) -> Dict[str, any]:
        """
        Calculate USD and AVAX pricing for a pool swap using direct pricing configurations.
        
        Logic:
        1. Check if pool has active pricing configuration at this block
        2. If DIRECT_AVAX: Use quote_amount directly as AVAX value, convert to USD
        3. If DIRECT_USD: Use quote_amount directly as USD value, convert to AVAX  
        4. If GLOBAL or no config: Mark as GLOBAL (not implemented yet)
        5. Always create both USD and AVAX detail records for direct pricing
        
        Args:
            indexer_session: Database session for indexer database operations
            shared_session: Database session for shared database operations
            pool_swap_details_repo: Repository for creating swap detail records
            pool_pricing_config_repo: Repository for pool configurations
            swap_content_id: Content ID of the pool swap
            pool_address: Pool contract address
            base_amount: Amount of base token swapped
            quote_amount: Amount of quote token swapped  
            block_number: Block number of the swap
            model_id: Model ID for configuration lookup
            contract_id: Contract ID for configuration lookup
            
        Returns:
            Dict with pricing results and status
        """
        try:
            log_with_context(
                self.logger, logging.DEBUG, "Starting swap pricing calculation",
                swap_content_id=swap_content_id,
                pool_address=pool_address,
                base_amount=str(base_amount),
                quote_amount=str(quote_amount),
                block_number=block_number
            )
            
            # 1. Get active pricing configuration for this pool at this block
            pricing_config = pool_pricing_config_repo.get_active_config_for_pool(
                shared_session, model_id, contract_id, block_number
            )
            
            if not pricing_config or pricing_config.pricing_strategy != 'DIRECT':
                # No configuration or global pricing - defer to future global implementation
                log_with_context(
                    self.logger, logging.DEBUG, "Pool uses global pricing (not implemented)",
                    swap_content_id=swap_content_id,
                    pool_address=pool_address,
                    pricing_strategy=pricing_config.pricing_strategy if pricing_config else 'UNCONFIGURED'
                )
                return {
                    'success': False,
                    'reason': 'global_pricing_not_implemented',
                    'pricing_method': 'GLOBAL' if pricing_config else 'UNCONFIGURED',
                    'records_created': 0
                }
            
            # 2. Get AVAX-USD price at this block for conversion
            avax_price_usd = self._get_avax_price_at_block(shared_session, block_number)
            if avax_price_usd is None:
                log_with_context(
                    self.logger, logging.WARNING, "No AVAX price available for block",
                    swap_content_id=swap_content_id,
                    block_number=block_number
                )
                return {
                    'success': False,
                    'reason': 'missing_avax_price',
                    'pricing_method': 'ERROR',
                    'records_created': 0
                }
            
            # 3. Calculate pricing based on quote token type
            current_timestamp = int(datetime.now(timezone.utc).timestamp())
            records_created = 0
            
            if pricing_config.quote_token_type == 'AVAX':
                # DIRECT_AVAX: quote_amount is in AVAX, convert to USD
                avax_value = quote_amount
                usd_value = quote_amount * avax_price_usd
                
                # Per-unit prices (value / base_amount)
                avax_price_per_base = avax_value / base_amount if base_amount > 0 else Decimal('0')
                usd_price_per_base = usd_value / base_amount if base_amount > 0 else Decimal('0')
                
                # Create AVAX detail record (direct from quote)
                avax_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.AVAX,
                    value=float(avax_value),
                    price=float(avax_price_per_base),
                    price_method=PricingMethod.DIRECT_AVAX,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                # Create USD detail record (converted)
                usd_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.USD,
                    value=float(usd_value),
                    price=float(usd_price_per_base),
                    price_method=PricingMethod.DIRECT_AVAX,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "DIRECT_AVAX pricing completed",
                    swap_content_id=swap_content_id,
                    avax_value=str(avax_value),
                    usd_value=str(usd_value),
                    avax_price_used=str(avax_price_usd)
                )
                
            elif pricing_config.quote_token_type == 'USD':
                # DIRECT_USD: quote_amount is in USD equivalent, convert to AVAX
                usd_value = quote_amount
                avax_value = quote_amount / avax_price_usd
                
                # Per-unit prices (value / base_amount)
                usd_price_per_base = usd_value / base_amount if base_amount > 0 else Decimal('0')
                avax_price_per_base = avax_value / base_amount if base_amount > 0 else Decimal('0')
                
                # Create USD detail record (direct from quote)
                usd_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.USD,
                    value=float(usd_value),
                    price=float(usd_price_per_base),
                    price_method=PricingMethod.DIRECT_USD,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                # Create AVAX detail record (converted)
                avax_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.AVAX,
                    value=float(avax_value),
                    price=float(avax_price_per_base),
                    price_method=PricingMethod.DIRECT_USD,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "DIRECT_USD pricing completed",
                    swap_content_id=swap_content_id,
                    usd_value=str(usd_value),
                    avax_value=str(avax_value),
                    avax_price_used=str(avax_price_usd)
                )
                
            else:
                # OTHER quote token type - use global pricing (not implemented)
                log_with_context(
                    self.logger, logging.DEBUG, "Quote token type OTHER requires global pricing",
                    swap_content_id=swap_content_id,
                    quote_token_type=pricing_config.quote_token_type
                )
                return {
                    'success': False,
                    'reason': 'quote_type_other_not_implemented',
                    'pricing_method': 'GLOBAL',
                    'records_created': 0
                }
            
            # Flush the detail records
            indexer_session.flush()
            
            return {
                'success': True,
                'pricing_method': f"DIRECT_{pricing_config.quote_token_type}",
                'records_created': records_created,
                'avax_price_used': float(avax_price_usd),
                'config_id': pricing_config.id
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error calculating swap pricing",
                swap_content_id=swap_content_id,
                pool_address=pool_address,
                error=str(e),
                exception_type=type(e).__name__
            )
            return {
                'success': False,
                'reason': 'calculation_error',
                'pricing_method': 'ERROR',
                'records_created': 0,
                'error': str(e)
            }


    def _get_avax_price_at_block(self, session, block_number: int) -> Optional[Decimal]:
        """
        Get AVAX-USD price at a specific block.
        
        Uses the shared database block_prices table.
        """
        try:
            block_price = self.block_prices_repo.get_price_at_block(session, block_number)
            if block_price:
                return block_price.price_usd
            
            # Try to find the closest earlier block price
            closest_price = self.block_prices_repo.get_price_before_block(session, block_number)
            if closest_price:
                log_with_context(
                    self.logger, logging.DEBUG, "Using closest earlier block price",
                    requested_block=block_number,
                    price_block=closest_price.block_number,
                    price_usd=str(closest_price.price_usd)
                )
                return closest_price.price_usd
            
            log_with_context(
                self.logger, logging.WARNING, "No AVAX price found for block or earlier",
                block_number=block_number
            )
            return None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error fetching AVAX price",
                block_number=block_number,
                error=str(e)
            )
            return None


    def calculate_missing_swap_pricing(
        self,
        indexer_session,
        shared_session,
        pool_swap_repo,  # PoolSwapRepository
        pool_swap_details_repo,  # PoolSwapDetailRepository
        pool_pricing_config_repo,  # PoolPricingConfigRepository
        model_id: int,
        limit: int = 1000
    ) -> Dict[str, int]:
        """
        Batch calculation of pricing for swaps that don't have detail records yet.
        
        This method can be used for:
        1. Backfilling pricing for existing swaps
        2. Processing swaps that had pricing errors
        3. Periodic cleanup to ensure all swaps have pricing
        
        Args:
            indexer_session: Session for indexer database
            shared_session: Session for shared database  
            pool_swap_repo: Repository for pool swaps
            pool_swap_details_repo: Repository for swap details
            pool_pricing_config_repo: Repository for configurations
            model_id: Model ID to process swaps for
            limit: Maximum number of swaps to process in this batch
            
        Returns:
            Dict with batch processing statistics
        """
        try:
            log_with_context(
                self.logger, logging.INFO, "Starting batch swap pricing calculation",
                model_id=model_id,
                limit=limit
            )
            
            # Get recent swaps that don't have pricing details yet
            recent_swaps = pool_swap_repo.get_recent(indexer_session, limit=limit * 2)  # Get more to filter
            
            # Filter to swaps missing USD pricing (assuming if USD exists, AVAX exists too)
            swap_content_ids = [swap.content_id for swap in recent_swaps]
            missing_usd_ids = pool_swap_details_repo.get_missing_valuations(
                indexer_session, swap_content_ids, PricingDenomination.USD
            )
            
            swaps_to_process = [swap for swap in recent_swaps if swap.content_id in missing_usd_ids[:limit]]
            
            log_with_context(
                self.logger, logging.INFO, "Found swaps missing pricing",
                total_recent_swaps=len(recent_swaps),
                missing_pricing_count=len(swaps_to_process)
            )
            
            # Process each swap
            stats = {
                'processed': 0,
                'success': 0,
                'failed': 0,
                'skipped': 0,
                'errors': []
            }
            
            for swap in swaps_to_process:
                try:
                    stats['processed'] += 1
                    
                    # Get contract ID for this pool (needed for configuration lookup)
                    # This assumes you have a way to map pool address to contract_id
                    # You might need to add a method to get this mapping
                    contract_id = self._get_contract_id_for_pool(shared_session, swap.pool)
                    if not contract_id:
                        stats['skipped'] += 1
                        continue
                    
                    # Calculate pricing for this swap
                    result = self.calculate_swap_pricing(
                        indexer_session=indexer_session,
                        shared_session=shared_session,
                        pool_swap_details_repo=pool_swap_details_repo,
                        pool_pricing_config_repo=pool_pricing_config_repo,
                        swap_content_id=swap.content_id,
                        pool_address=swap.pool,
                        base_amount=Decimal(str(swap.base_amount)),
                        quote_amount=Decimal(str(swap.quote_amount)),
                        block_number=swap.block_number,
                        model_id=model_id,
                        contract_id=contract_id
                    )
                    
                    if result['success']:
                        stats['success'] += 1
                    else:
                        stats['failed'] += 1
                        if 'error' in result:
                            stats['errors'].append(f"Swap {swap.content_id}: {result['error']}")
                    
                except Exception as e:
                    stats['failed'] += 1
                    stats['errors'].append(f"Swap {swap.content_id}: {str(e)}")
                    log_with_context(
                        self.logger, logging.ERROR, "Error processing swap in batch",
                        swap_content_id=swap.content_id,
                        error=str(e)
                    )
            
            # Commit all changes
            indexer_session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Batch swap pricing completed",
                **{k: v for k, v in stats.items() if k != 'errors'},
                error_count=len(stats['errors'])
            )
            
            return stats
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error in batch swap pricing",
                model_id=model_id,
                error=str(e)
            )
            raise


    def _get_contract_id_for_pool(self, session, pool_address: str) -> Optional[int]:
        """
        Get contract ID for a pool address.
        
        This helper method looks up the contract_id needed for configuration queries.
        """
        try:
            from ..database.shared.tables.config import Contract
            
            contract = session.query(Contract).filter(
                Contract.address == pool_address.lower()
            ).first()
            
            return contract.id if contract else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting contract ID for pool",
                pool_address=pool_address,
                error=str(e)
            )
            return None

    def calculate_swap_pricing(
        self,
        indexer_session,  # Session for indexer database (pool swaps)
        shared_session,   # Session for shared database (configs, block prices)
        pool_swap_details_repo,  # PoolSwapDetailRepository 
        pool_pricing_config_repo,  # PoolPricingConfigRepository
        swap_content_id,
        pool_address: str,
        base_amount: Decimal,  # Amount of base token in swap
        quote_amount: Decimal,  # Amount of quote token in swap
        block_number: int,
        model_id: int,
        contract_id: int
    ) -> Dict[str, any]:
        """
        Calculate USD and AVAX pricing for a pool swap using direct pricing configurations.
        
        Logic:
        1. Check if pool has active pricing configuration at this block
        2. If DIRECT_AVAX: Use quote_amount directly as AVAX value, convert to USD
        3. If DIRECT_USD: Use quote_amount directly as USD value, convert to AVAX  
        4. If GLOBAL or no config: Mark as GLOBAL (not implemented yet)
        5. Always create both USD and AVAX detail records for direct pricing
        
        Args:
            indexer_session: Database session for indexer database operations
            shared_session: Database session for shared database operations
            pool_swap_details_repo: Repository for creating swap detail records
            pool_pricing_config_repo: Repository for pool configurations
            swap_content_id: Content ID of the pool swap
            pool_address: Pool contract address
            base_amount: Amount of base token swapped
            quote_amount: Amount of quote token swapped  
            block_number: Block number of the swap
            model_id: Model ID for configuration lookup
            contract_id: Contract ID for configuration lookup
            
        Returns:
            Dict with pricing results and status
        """
        try:
            log_with_context(
                self.logger, logging.DEBUG, "Starting swap pricing calculation",
                swap_content_id=swap_content_id,
                pool_address=pool_address,
                base_amount=str(base_amount),
                quote_amount=str(quote_amount),
                block_number=block_number
            )
            
            # 1. Get active pricing configuration for this pool at this block
            pricing_config = pool_pricing_config_repo.get_active_config_for_pool(
                shared_session, model_id, contract_id, block_number
            )
            
            if not pricing_config or pricing_config.pricing_strategy != 'DIRECT':
                # No configuration or global pricing - defer to future global implementation
                log_with_context(
                    self.logger, logging.DEBUG, "Pool uses global pricing (not implemented)",
                    swap_content_id=swap_content_id,
                    pool_address=pool_address,
                    pricing_strategy=pricing_config.pricing_strategy if pricing_config else 'UNCONFIGURED'
                )
                return {
                    'success': False,
                    'reason': 'global_pricing_not_implemented',
                    'pricing_method': 'GLOBAL' if pricing_config else 'UNCONFIGURED',
                    'records_created': 0
                }
            
            # 2. Get AVAX-USD price at this block for conversion
            avax_price_usd = self._get_avax_price_at_block(shared_session, block_number)
            if avax_price_usd is None:
                log_with_context(
                    self.logger, logging.WARNING, "No AVAX price available for block",
                    swap_content_id=swap_content_id,
                    block_number=block_number
                )
                return {
                    'success': False,
                    'reason': 'missing_avax_price',
                    'pricing_method': 'ERROR',
                    'records_created': 0
                }
            
            # 3. Calculate pricing based on quote token type
            current_timestamp = int(datetime.now(timezone.utc).timestamp())
            records_created = 0
            
            if pricing_config.quote_token_type == 'AVAX':
                # DIRECT_AVAX: quote_amount is in AVAX, convert to USD
                avax_value = quote_amount
                usd_value = quote_amount * avax_price_usd
                
                # Per-unit prices (value / base_amount)
                avax_price_per_base = avax_value / base_amount if base_amount > 0 else Decimal('0')
                usd_price_per_base = usd_value / base_amount if base_amount > 0 else Decimal('0')
                
                # Create AVAX detail record (direct from quote)
                avax_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.AVAX,
                    value=float(avax_value),
                    price=float(avax_price_per_base),
                    price_method=PricingMethod.DIRECT_AVAX,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                # Create USD detail record (converted)
                usd_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.USD,
                    value=float(usd_value),
                    price=float(usd_price_per_base),
                    price_method=PricingMethod.DIRECT_AVAX,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "DIRECT_AVAX pricing completed",
                    swap_content_id=swap_content_id,
                    avax_value=str(avax_value),
                    usd_value=str(usd_value),
                    avax_price_used=str(avax_price_usd)
                )
                
            elif pricing_config.quote_token_type == 'USD':
                # DIRECT_USD: quote_amount is in USD equivalent, convert to AVAX
                usd_value = quote_amount
                avax_value = quote_amount / avax_price_usd
                
                # Per-unit prices (value / base_amount)
                usd_price_per_base = usd_value / base_amount if base_amount > 0 else Decimal('0')
                avax_price_per_base = avax_value / base_amount if base_amount > 0 else Decimal('0')
                
                # Create USD detail record (direct from quote)
                usd_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.USD,
                    value=float(usd_value),
                    price=float(usd_price_per_base),
                    price_method=PricingMethod.DIRECT_USD,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                # Create AVAX detail record (converted)
                avax_detail = pool_swap_details_repo.create_detail(
                    session=indexer_session,
                    content_id=swap_content_id,
                    denom=PricingDenomination.AVAX,
                    value=float(avax_value),
                    price=float(avax_price_per_base),
                    price_method=PricingMethod.DIRECT_USD,
                    price_config_id=pricing_config.id,
                    price_block_number=block_number,
                    calculated_at=current_timestamp
                )
                records_created += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "DIRECT_USD pricing completed",
                    swap_content_id=swap_content_id,
                    usd_value=str(usd_value),
                    avax_value=str(avax_value),
                    avax_price_used=str(avax_price_usd)
                )
                
            else:
                # OTHER quote token type - use global pricing (not implemented)
                log_with_context(
                    self.logger, logging.DEBUG, "Quote token type OTHER requires global pricing",
                    swap_content_id=swap_content_id,
                    quote_token_type=pricing_config.quote_token_type
                )
                return {
                    'success': False,
                    'reason': 'quote_type_other_not_implemented',
                    'pricing_method': 'GLOBAL',
                    'records_created': 0
                }
            
            # Flush the detail records
            indexer_session.flush()
            
            return {
                'success': True,
                'pricing_method': f"DIRECT_{pricing_config.quote_token_type}",
                'records_created': records_created,
                'avax_price_used': float(avax_price_usd),
                'config_id': pricing_config.id
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error calculating swap pricing",
                swap_content_id=swap_content_id,
                pool_address=pool_address,
                error=str(e),
                exception_type=type(e).__name__
            )
            return {
                'success': False,
                'reason': 'calculation_error',
                'pricing_method': 'ERROR',
                'records_created': 0,
                'error': str(e)
            }


    def _get_avax_price_at_block(self, session, block_number: int) -> Optional[Decimal]:
        """
        Get AVAX-USD price at a specific block.
        
        Uses the shared database block_prices table.
        """
        try:
            block_price = self.block_prices_repo.get_price_at_block(session, block_number)
            if block_price:
                return block_price.price_usd
            
            # Try to find the closest earlier block price
            closest_price = self.block_prices_repo.get_price_before_block(session, block_number)
            if closest_price:
                log_with_context(
                    self.logger, logging.DEBUG, "Using closest earlier block price",
                    requested_block=block_number,
                    price_block=closest_price.block_number,
                    price_usd=str(closest_price.price_usd)
                )
                return closest_price.price_usd
            
            log_with_context(
                self.logger, logging.WARNING, "No AVAX price found for block or earlier",
                block_number=block_number
            )
            return None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error fetching AVAX price",
                block_number=block_number,
                error=str(e)
            )
            return None


    def calculate_trade_pricing(
        self,
        indexer_session,  # Session for indexer database (trades, swaps, details)
        shared_session,   # Session for shared database (configs, block prices)
        pool_swap_repo,   # PoolSwapRepository
        pool_swap_details_repo,  # PoolSwapDetailRepository
        trade_details_repo,  # TradeDetailRepository
        trade_content_id,
        trade_base_amount: Decimal,  # Amount of base token in trade
    ) -> Dict[str, any]:
        """
        Calculate USD and AVAX pricing for a trade by aggregating directly priced swaps.
        
        Logic:
        1. Get all pool swaps for this trade using trade_id
        2. Check if all swaps have direct pricing (eligibility check)
        3. If eligible: aggregate swap values using volume weighting
        4. If not eligible: defer to global pricing (not implemented)
        5. Create both USD and AVAX trade detail records
        
        Args:
            indexer_session: Database session for indexer database operations
            shared_session: Database session for shared database operations  
            pool_swap_repo: Repository for pool swap queries
            pool_swap_details_repo: Repository for swap detail queries
            trade_details_repo: Repository for creating trade detail records
            trade_content_id: Content ID of the trade
            trade_base_amount: Total amount of base token in trade
            
        Returns:
            Dict with pricing results and status
        """
        try:
            log_with_context(
                self.logger, logging.DEBUG, "Starting trade pricing calculation",
                trade_content_id=trade_content_id,
                trade_base_amount=str(trade_base_amount)
            )
            
            # 1. Get all pool swaps for this trade
            swaps = pool_swap_repo.get_by_trade_id(indexer_session, trade_content_id)
            
            if not swaps:
                log_with_context(
                    self.logger, logging.DEBUG, "No swaps found for trade",
                    trade_content_id=trade_content_id
                )
                return {
                    'success': False,
                    'reason': 'no_swaps_found',
                    'pricing_method': 'ERROR',
                    'records_created': 0
                }
            
            swap_content_ids = [swap.content_id for swap in swaps]
            
            log_with_context(
                self.logger, logging.DEBUG, "Found swaps for trade",
                trade_content_id=trade_content_id,
                swap_count=len(swaps),
                swap_ids=swap_content_ids[:5]  # Log first 5 swap IDs
            )
            
            # 2. Check eligibility - all swaps must have direct pricing
            is_eligible = pool_swap_details_repo.check_all_swaps_have_direct_pricing(
                indexer_session, swap_content_ids
            )
            
            if not is_eligible:
                log_with_context(
                    self.logger, logging.DEBUG, "Trade not eligible for direct pricing",
                    trade_content_id=trade_content_id,
                    swap_count=len(swaps),
                    reason="some_swaps_missing_direct_pricing"
                )
                return {
                    'success': False,
                    'reason': 'global_pricing_required',
                    'pricing_method': 'GLOBAL',
                    'records_created': 0,
                    'swap_count': len(swaps)
                }
            
            # 3. Get swap details for aggregation
            usd_details = pool_swap_details_repo.get_usd_details_for_swaps(
                indexer_session, swap_content_ids
            )
            avax_details = pool_swap_details_repo.get_avax_details_for_swaps(
                indexer_session, swap_content_ids
            )
            
            # Validation - should have details for all swaps
            if len(usd_details) != len(swaps) or len(avax_details) != len(swaps):
                log_with_context(
                    self.logger, logging.WARNING, "Swap detail count mismatch",
                    trade_content_id=trade_content_id,
                    swap_count=len(swaps),
                    usd_detail_count=len(usd_details),
                    avax_detail_count=len(avax_details)
                )
                return {
                    'success': False,
                    'reason': 'swap_detail_mismatch',
                    'pricing_method': 'ERROR',
                    'records_created': 0
                }
            
            # 4. Volume-weighted aggregation (sum all swap values)
            total_usd_value = sum(Decimal(str(detail.value)) for detail in usd_details)
            total_avax_value = sum(Decimal(str(detail.value)) for detail in avax_details)
            
            # Calculate trade-level per-unit prices
            usd_price_per_base = total_usd_value / trade_base_amount if trade_base_amount > 0 else Decimal('0')
            avax_price_per_base = total_avax_value / trade_base_amount if trade_base_amount > 0 else Decimal('0')
            
            log_with_context(
                self.logger, logging.DEBUG, "Trade aggregation calculated",
                trade_content_id=trade_content_id,
                total_usd_value=str(total_usd_value),
                total_avax_value=str(total_avax_value),
                usd_price_per_base=str(usd_price_per_base),
                avax_price_per_base=str(avax_price_per_base),
                swap_count=len(swaps)
            )
            
            # 5. Create trade detail records
            from ..database.indexer.tables.detail.trade_detail import PricingDenomination, TradePricingMethod
            
            records_created = 0
            
            # Create USD detail record
            usd_detail = trade_details_repo.create_detail(
                session=indexer_session,
                content_id=trade_content_id,
                denom=PricingDenomination.USD,
                value=float(total_usd_value),
                price=float(usd_price_per_base),
                pricing_method=TradePricingMethod.DIRECT
            )
            records_created += 1
            
            # Create AVAX detail record  
            avax_detail = trade_details_repo.create_detail(
                session=indexer_session,
                content_id=trade_content_id,
                denom=PricingDenomination.AVAX,
                value=float(total_avax_value),
                price=float(avax_price_per_base),
                pricing_method=TradePricingMethod.DIRECT
            )
            records_created += 1
            
            # Flush the detail records
            indexer_session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Direct trade pricing completed",
                trade_content_id=trade_content_id,
                records_created=records_created,
                usd_value=str(total_usd_value),
                avax_value=str(total_avax_value),
                swap_count=len(swaps)
            )
            
            return {
                'success': True,
                'pricing_method': 'DIRECT',
                'records_created': records_created,
                'swap_count': len(swaps),
                'total_usd_value': float(total_usd_value),
                'total_avax_value': float(total_avax_value),
                'usd_price_per_base': float(usd_price_per_base),
                'avax_price_per_base': float(avax_price_per_base)
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error calculating trade pricing",
                trade_content_id=trade_content_id,
                error=str(e),
                exception_type=type(e).__name__
            )
            return {
                'success': False,
                'reason': 'calculation_error',
                'pricing_method': 'ERROR',
                'records_created': 0,
                'error': str(e)
            }

    def _get_contract_id_for_pool(self, session, pool_address: str) -> Optional[int]:
        """
        Get contract ID for a pool address.
        
        This helper method looks up the contract_id needed for configuration queries.
        """
        try:
            from ..database.shared.tables.config import Contract
            
            contract = session.query(Contract).filter(
                Contract.address == pool_address.lower()
            ).first()
            
            return contract.id if contract else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting contract ID for pool",
                pool_address=pool_address,
                error=str(e)
            )
            return None