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