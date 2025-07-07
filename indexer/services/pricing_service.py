# indexer/services/pricing_service.py

from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from ..core.logging_config import IndexerLogger, log_with_context
from ..database.repository import RepositoryManager
from ..database.models.pricing.periods import Period, PeriodType
from ..database.models.pricing.block_prices import BlockPrice
from ..database.repositories.block_prices_repository import BlockPricesRepository
from ..clients.quicknode_rpc import QuickNodeRpcClient

import logging


class PricingService:
    """
    Pricing service responsible for maintaining time-based periods and canonical pricing.
    
    This is the start of the pricing service architecture from the valuation design.
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
        repository_manager: RepositoryManager,
        rpc_client: QuickNodeRpcClient
    ):
        self.repository_manager = repository_manager
        self.rpc_client = rpc_client
        self.block_prices_repo = BlockPricesRepository(repository_manager.db_manager)
        self.logger = IndexerLogger.get_logger('services.pricing_service')
        
        log_with_context(
            self.logger, logging.INFO, "PricingService initialized"
        )
    
    def update_periods_to_present(self, period_types: Optional[List[PeriodType]] = None) -> Dict[str, int]:
        """
        Update periods from last recorded period to present time.
        
        This is the main method that would be called by a cron job.
        
        Args:
            period_types: List of period types to update. If None, updates all types.
            
        Returns:
            Dict with statistics about periods created
        """
        if period_types is None:
            period_types = list(PeriodType)
        
        log_with_context(
            self.logger, logging.INFO, "Starting period update to present",
            period_types=[pt.value for pt in period_types]
        )
        
        stats = {
            'total_periods_created': 0,
            'periods_by_type': {},
            'latest_block_processed': None,
            'errors': []
        }
        
        try:
            # Get current blockchain state
            latest_block_number = self.rpc_client.get_latest_block_number()
            latest_block = self.rpc_client.get_block(latest_block_number)
            current_timestamp = latest_block['timestamp']
            
            stats['latest_block_processed'] = latest_block_number
            
            log_with_context(
                self.logger, logging.INFO, "Current blockchain state retrieved",
                latest_block=latest_block_number,
                current_timestamp=current_timestamp
            )
            
            # Update each period type
            for period_type in period_types:
                try:
                    periods_created = self._update_period_type_to_present(
                        period_type, current_timestamp, latest_block_number
                    )
                    
                    stats['periods_by_type'][period_type.value] = periods_created
                    stats['total_periods_created'] += periods_created
                    
                    log_with_context(
                        self.logger, logging.INFO, "Period type updated successfully",
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
        Used for Item #3: period-based AVAX-USD pricing.
        
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
            with self.repository_manager.get_session() as session:
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
    
    def _generate_minute_prices_for_range(
        self,
        session,
        start_timestamp: int,
        end_timestamp: int,
        latest_block_number: int
    ) -> Tuple[int, int]:
        """Generate minute-by-minute prices for a timestamp range"""
        
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
        
        # Commit all prices
        session.flush()
        
        log_with_context(
            self.logger, logging.INFO, "Minute price generation completed",
            prices_created=prices_created,
            prices_skipped=prices_skipped
        )
        
        return prices_created, prices_skipped
        """
        Update periods from last recorded period to present time.
        
        This is the main method that would be called by a cron job.
        
        Args:
            period_types: List of period types to update. If None, updates all types.
            
        Returns:
            Dict with statistics about periods created
        """
        if period_types is None:
            period_types = list(PeriodType)
        
        log_with_context(
            self.logger, logging.INFO, "Starting period update to present",
            period_types=[pt.value for pt in period_types]
        )
        
        stats = {
            'total_periods_created': 0,
            'periods_by_type': {},
            'latest_block_processed': None,
            'errors': []
        }
        
        try:
            # Get current blockchain state
            latest_block_number = self.rpc_client.get_latest_block_number()
            latest_block = self.rpc_client.get_block(latest_block_number)
            current_timestamp = latest_block['timestamp']
            
            stats['latest_block_processed'] = latest_block_number
            
            log_with_context(
                self.logger, logging.INFO, "Current blockchain state retrieved",
                latest_block=latest_block_number,
                current_timestamp=current_timestamp
            )
            
            # Update each period type
            for period_type in period_types:
                try:
                    periods_created = self._update_period_type_to_present(
                        period_type, current_timestamp, latest_block_number
                    )
                    
                    stats['periods_by_type'][period_type.value] = periods_created
                    stats['total_periods_created'] += periods_created
                    
                    log_with_context(
                        self.logger, logging.INFO, "Period type updated successfully",
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
    
    def _update_period_type_to_present(
        self, 
        period_type: PeriodType, 
        current_timestamp: int, 
        latest_block_number: int
    ) -> int:
        """Update a specific period type from its last record to present"""
        
        with self.repository_manager.get_transaction() as session:
            # Get the last period for this type
            last_period = session.query(Period).filter(
                Period.period_type == period_type
            ).order_by(Period.time_open.desc()).first()
            
            if last_period:
                # Start from the next period after the last one
                start_timestamp = last_period.time_close + 1
                log_with_context(
                    self.logger, logging.DEBUG, "Found existing periods, continuing from last",
                    period_type=period_type.value,
                    last_period_close=last_period.time_close,
                    start_timestamp=start_timestamp
                )
            else:
                # No existing periods, start from a reasonable point in the past
                # For now, start from 30 days ago or adjust as needed
                start_timestamp = current_timestamp - (30 * 24 * 3600)  # 30 days ago
                log_with_context(
                    self.logger, logging.INFO, "No existing periods found, starting from 30 days ago",
                    period_type=period_type.value,
                    start_timestamp=start_timestamp
                )
            
            # Generate periods from start_timestamp to current_timestamp
            periods_created = self._generate_periods_for_range(
                session, period_type, start_timestamp, current_timestamp, latest_block_number
            )
            
            return periods_created
    
    def _generate_periods_for_range(
        self,
        session,
        period_type: PeriodType,
        start_timestamp: int,
        end_timestamp: int,
        latest_block_number: int
    ) -> int:
        """Generate periods for a timestamp range"""
        
        periods_created = 0
        current_timestamp = Period.get_period_start_timestamp(start_timestamp, period_type)
        
        log_with_context(
            self.logger, logging.DEBUG, "Generating periods for range",
            period_type=period_type.value,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            aligned_start=current_timestamp
        )
        
        while current_timestamp < end_timestamp:
            period_end = Period.get_period_end_timestamp(current_timestamp, period_type)
            
            # Don't create periods that extend beyond current time
            if current_timestamp >= end_timestamp:
                break
            
            # Check if period already exists
            existing_period = session.query(Period).filter(
                Period.period_type == period_type,
                Period.time_open == current_timestamp
            ).first()
            
            if existing_period:
                log_with_context(
                    self.logger, logging.DEBUG, "Period already exists, skipping",
                    period_type=period_type.value,
                    time_open=current_timestamp
                )
                current_timestamp += period_type.seconds
                continue
            
            # Find block range for this period
            try:
                block_open, block_close = self._find_block_range_for_period(
                    current_timestamp, period_end, latest_block_number
                )
                
                if block_open is None or block_close is None:
                    log_with_context(
                        self.logger, logging.WARNING, "Could not determine block range for period",
                        period_type=period_type.value,
                        time_open=current_timestamp,
                        time_close=period_end
                    )
                    current_timestamp += period_type.seconds
                    continue
                
                # Create the period
                period = Period.create_period(
                    period_type=period_type,
                    time_open=current_timestamp,
                    time_close=period_end,
                    block_open=block_open,
                    block_close=block_close,
                    is_complete=period_end < end_timestamp  # Only complete if period fully in past
                )
                
                session.add(period)
                periods_created += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "Period created",
                    period_type=period_type.value,
                    time_open=current_timestamp,
                    time_close=period_end,
                    block_open=block_open,
                    block_close=block_close,
                    is_complete=period.is_complete
                )
                
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to create period",
                    period_type=period_type.value,
                    time_open=current_timestamp,
                    error=str(e)
                )
            
            # Move to next period
            current_timestamp += period_type.seconds
        
        # Commit all periods for this type
        session.flush()
        
        log_with_context(
            self.logger, logging.INFO, "Period generation completed",
            period_type=period_type.value,
            periods_created=periods_created
        )
        
        return periods_created
    
    def _find_block_range_for_period(
        self, 
        time_open: int, 
        time_close: int,
        latest_block_number: int
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Find the block range that corresponds to a time period.
        
        Uses QuickNode's block-timestamp lookup to find blocks closest to period boundaries.
        """
        try:
            # Find block closest to time_open
            block_open = self._find_block_by_timestamp(time_open, latest_block_number, find_after=True)
            
            # Find block closest to time_close  
            block_close = self._find_block_by_timestamp(time_close, latest_block_number, find_after=False)
            
            if block_open is None or block_close is None:
                return None, None
            
            # Ensure block_close >= block_open
            if block_close < block_open:
                block_close = block_open
            
            log_with_context(
                self.logger, logging.DEBUG, "Block range found for period",
                time_open=time_open,
                time_close=time_close,
                block_open=block_open,
                block_close=block_close
            )
            
            return block_open, block_close
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Failed to find block range",
                time_open=time_open,
                time_close=time_close,
                error=str(e)
            )
            return None, None
    
    def _find_block_by_timestamp(
        self, 
        target_timestamp: int, 
        latest_block_number: int,
        find_after: bool = True
    ) -> Optional[int]:
        """
        Find block number closest to target timestamp using binary search.
        
        Args:
            target_timestamp: Unix timestamp to find
            latest_block_number: Latest known block number
            find_after: If True, find first block at or after timestamp.
                       If False, find last block at or before timestamp.
        """
        try:
            # Binary search range - you may need to adjust these bounds
            # based on your blockchain's block history
            min_block = max(1, latest_block_number - 1000000)  # Search last ~1M blocks
            max_block = latest_block_number
            
            # Binary search for the target timestamp
            while min_block <= max_block:
                mid_block = (min_block + max_block) // 2
                
                try:
                    block_info = self.rpc_client.get_block(mid_block, full_transactions=False)
                    block_timestamp = block_info['timestamp']
                    
                    if block_timestamp == target_timestamp:
                        return mid_block
                    elif block_timestamp < target_timestamp:
                        min_block = mid_block + 1
                    else:
                        max_block = mid_block - 1
                        
                except Exception as e:
                    log_with_context(
                        self.logger, logging.WARNING, "Failed to get block during search",
                        block_number=mid_block,
                        error=str(e)
                    )
                    # Skip this block and continue search
                    if block_timestamp < target_timestamp:
                        min_block = mid_block + 1
                    else:
                        max_block = mid_block - 1
                    continue
            
            # Return the appropriate boundary block
            if find_after:
                return min_block if min_block <= latest_block_number else None
            else:
                return max_block if max_block >= 1 else None
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Block timestamp search failed",
                target_timestamp=target_timestamp,
                error=str(e)
            )
            return None
    
    def backfill_missing_periods(
        self, 
        period_type: PeriodType,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None
    ) -> int:
        """
        Backfill any missing periods in a time range.
        
        Useful for fixing gaps in period data.
        """
        if end_timestamp is None:
            latest_block = self.rpc_client.get_block(self.rpc_client.get_latest_block_number())
            end_timestamp = latest_block['timestamp']
        
        if start_timestamp is None:
            # Default to 30 days ago
            start_timestamp = end_timestamp - (30 * 24 * 3600)
        
        log_with_context(
            self.logger, logging.INFO, "Starting period backfill",
            period_type=period_type.value,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp
        )
        
        with self.repository_manager.get_transaction() as session:
            periods_created = self._generate_periods_for_range(
                session, period_type, start_timestamp, end_timestamp, 
                self.rpc_client.get_latest_block_number()
            )
        
        log_with_context(
            self.logger, logging.INFO, "Period backfill completed",
            period_type=period_type.value,
            periods_created=periods_created
        )
        
        return periods_created