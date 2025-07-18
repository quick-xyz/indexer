# indexer/database/shared/repositories/periods_repository.py

from typing import List, Optional, Tuple, Dict
from datetime import datetime, timezone, timedelta

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc, func
from sqlalchemy.exc import IntegrityError

from ..tables.periods import Period, PeriodType
from ...base_repository import BaseRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL


class PeriodsRepository(BaseRepository):
    """
    Repository for time period data.
    
    Uses shared database connection since periods are chain-level time infrastructure
    shared across all indexers.
    """
    
    def __init__(self, db_manager):
        super().__init__(db_manager, Period)
        self.logger = IndexerLogger.get_logger('database.repository.periods')
    
    def create_period(
        self,
        session: Session,
        period_type: PeriodType,
        time_open: int,
        time_close: int,
        block_open: int,
        block_close: int,
        is_complete: bool = True
    ) -> Optional[Period]:
        """Create a new period record."""
        try:
            period = Period.create_period(
                period_type=period_type,
                time_open=time_open,
                time_close=time_close,
                block_open=block_open,
                block_close=block_close,
                is_complete=is_complete
            )
            
            session.add(period)
            session.flush()
            
            log_with_context(
                self.logger, DEBUG, "Period created",
                period_type=period_type.value,
                time_open=time_open,
                block_range=f"{block_open}-{block_close}",
                is_complete=is_complete
            )
            
            return period
            
        except IntegrityError:
            # Period already exists
            log_with_context(
                self.logger, DEBUG, "Period already exists",
                period_type=period_type.value,
                time_open=time_open
            )
            session.rollback()
            return None
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error creating period",
                period_type=period_type.value,
                time_open=time_open,
                error=str(e)
            )
            raise
    
    def get_period(
        self, 
        session: Session, 
        period_type: PeriodType, 
        time_open: int
    ) -> Optional[Period]:
        """Get a specific period by its composite key."""
        return session.query(Period).filter(
            and_(
                Period.period_type == period_type.value,
                Period.time_open == time_open
            )
        ).first()
    
    def get_periods_by_type(
        self, 
        session: Session, 
        period_type: PeriodType,
        limit: Optional[int] = None,
        order_desc: bool = False
    ) -> List[Period]:
        """Get all periods for a specific type."""
        query = session.query(Period).filter(
            Period.period_type == period_type.value
        )
        
        if order_desc:
            query = query.order_by(Period.time_open.desc())
        else:
            query = query.order_by(Period.time_open.asc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_latest_period(self, session: Session, period_type: PeriodType) -> Optional[Period]:
        """Get the most recent period for a specific type."""
        return session.query(Period).filter(
            Period.period_type == period_type.value
        ).order_by(Period.time_open.desc()).first()
    
    def get_earliest_period(self, session: Session, period_type: PeriodType) -> Optional[Period]:
        """Get the earliest period for a specific type."""
        return session.query(Period).filter(
            Period.period_type == period_type.value
        ).order_by(Period.time_open.asc()).first()
    
    def get_periods_in_time_range(
        self, 
        session: Session, 
        period_type: PeriodType,
        start_time: int,
        end_time: int
    ) -> List[Period]:
        """Get all periods within a time range."""
        return session.query(Period).filter(
            Period.period_type == period_type.value,
            Period.time_open >= start_time,
            Period.time_close <= end_time
        ).order_by(Period.time_open).all()
    
    def get_periods_in_block_range(
        self, 
        session: Session, 
        period_type: PeriodType,
        start_block: int,
        end_block: int
    ) -> List[Period]:
        """Get all periods that overlap with a block range."""
        return session.query(Period).filter(
            Period.period_type == period_type.value,
            Period.block_open <= end_block,
            Period.block_close >= start_block
        ).order_by(Period.time_open).all()
    
    def get_period_containing_block(
        self, 
        session: Session, 
        period_type: PeriodType, 
        block_number: int
    ) -> Optional[Period]:
        """Get the period that contains a specific block number."""
        return session.query(Period).filter(
            Period.period_type == period_type.value,
            Period.block_open <= block_number,
            Period.block_close >= block_number
        ).first()
    
    def get_period_containing_timestamp(
        self, 
        session: Session, 
        period_type: PeriodType, 
        timestamp: int
    ) -> Optional[Period]:
        """Get the period that contains a specific timestamp."""
        return session.query(Period).filter(
            Period.period_type == period_type.value,
            Period.time_open <= timestamp,
            Period.time_close >= timestamp
        ).first()
    
    def get_incomplete_periods(
        self, 
        session: Session, 
        period_type: Optional[PeriodType] = None
    ) -> List[Period]:
        """Get all incomplete periods, optionally filtered by type."""
        query = session.query(Period).filter(Period.is_complete == False)
        
        if period_type:
            query = query.filter(Period.period_type == period_type.value)
        
        return query.order_by(Period.time_open).all()
    
    def mark_period_complete(
        self, 
        session: Session, 
        period_type: PeriodType, 
        time_open: int
    ) -> bool:
        """Mark a period as complete."""
        try:
            period = self.get_period(session, period_type, time_open)
            if not period:
                log_with_context(
                    self.logger, WARNING, "Period not found for completion",
                    period_type=period_type.value,
                    time_open=time_open
                )
                return False
            
            period.is_complete = True
            session.flush()
            
            log_with_context(
                self.logger, DEBUG, "Period marked complete",
                period_type=period_type.value,
                time_open=time_open
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error marking period complete",
                period_type=period_type.value,
                time_open=time_open,
                error=str(e)
            )
            return False
    
    def get_period_gaps(
        self, 
        session: Session, 
        period_type: PeriodType,
        start_time: int,
        end_time: int
    ) -> List[Tuple[int, int]]:
        """
        Find gaps in period coverage within a time range.
        
        Returns:
            List of (gap_start_time, gap_end_time) tuples
        """
        # Get all periods in range
        periods = session.query(Period).filter(
            Period.period_type == period_type.value,
            Period.time_open <= end_time,
            Period.time_close >= start_time
        ).order_by(Period.time_open).all()
        
        if not periods:
            return [(start_time, end_time)]
        
        gaps = []
        current_time = start_time
        
        for period in periods:
            # Check for gap before this period
            if period.time_open > current_time:
                gaps.append((current_time, period.time_open - 1))
            
            # Move current time to end of this period
            current_time = max(current_time, period.time_close + 1)
        
        # Check for gap after last period
        if current_time <= end_time:
            gaps.append((current_time, end_time))
        
        log_with_context(
            self.logger, DEBUG, "Period gaps identified",
            period_type=period_type.value,
            start_time=start_time,
            end_time=end_time,
            gap_count=len(gaps),
            total_periods=len(periods)
        )
        
        return gaps
    
    def get_period_stats(self, session: Session, period_type: Optional[PeriodType] = None) -> Dict:
        """Get statistics about period data."""
        query = session.query(
            func.count(Period.period_type).label('total_periods'),
            func.min(Period.time_open).label('earliest_time'),
            func.max(Period.time_close).label('latest_time'),
            func.min(Period.block_open).label('earliest_block'),
            func.max(Period.block_close).label('latest_block'),
            func.count().filter(Period.is_complete == True).label('complete_periods'),
            func.count().filter(Period.is_complete == False).label('incomplete_periods')
        )
        
        if period_type:
            query = query.filter(Period.period_type == period_type.value)
        
        stats = query.first()
        
        result = {
            'total_periods': stats.total_periods or 0,
            'complete_periods': stats.complete_periods or 0,
            'incomplete_periods': stats.incomplete_periods or 0,
            'earliest_time': stats.earliest_time,
            'latest_time': stats.latest_time,
            'earliest_block': stats.earliest_block,
            'latest_block': stats.latest_block
        }
        
        # Add period type breakdown if not filtered
        if not period_type:
            type_stats = session.query(
                Period.period_type,
                func.count(Period.period_type).label('count')
            ).group_by(Period.period_type).all()
            
            result['by_type'] = {stat.period_type: stat.count for stat in type_stats}
        
        return result
    
    def bulk_create_periods(
        self, 
        session: Session, 
        period_data: List[Dict]
    ) -> Tuple[int, int]:
        """
        Bulk create multiple period records.
        
        Args:
            session: Database session
            period_data: List of dicts with period information
            
        Returns:
            Tuple of (created_count, skipped_count)
        """
        created_count = 0
        skipped_count = 0
        
        for data in period_data:
            try:
                period = Period.create_period(
                    period_type=data['period_type'],
                    time_open=data['time_open'],
                    time_close=data['time_close'],
                    block_open=data['block_open'],
                    block_close=data['block_close'],
                    is_complete=data.get('is_complete', True)
                )
                
                session.add(period)
                created_count += 1
                
            except IntegrityError:
                # Skip duplicates
                session.rollback()
                skipped_count += 1
                continue
            except Exception as e:
                log_with_context(
                    self.logger, ERROR, "Error in bulk period creation",
                    period_type=data.get('period_type'),
                    time_open=data.get('time_open'),
                    error=str(e)
                )
                session.rollback()
                skipped_count += 1
                continue
        
        try:
            session.flush()
            log_with_context(
                self.logger, INFO, "Bulk period creation completed",
                total_attempted=len(period_data),
                created_count=created_count,
                skipped_count=skipped_count
            )
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error committing bulk periods",
                error=str(e)
            )
            raise
        
        return created_count, skipped_count

    def get_periods_in_timeframe(
        self,
        session: Session,
        start_time: datetime,
        end_time: datetime,
        period_type: PeriodType
    ) -> List[Period]:
        """
        Get periods within a datetime timeframe.
        
        Used by PricingService.update_canonical_pricing() and CalculationService methods
        to get periods for processing within specific time ranges.
        """
        try:
            # Convert datetime to timestamp
            start_timestamp = int(start_time.timestamp())
            end_timestamp = int(end_time.timestamp())
            
            periods = session.query(Period).filter(
                and_(
                    Period.period_type == period_type,
                    Period.timestamp >= start_time,
                    Period.timestamp <= end_time
                )
            ).order_by(Period.timestamp).all()
            
            log_with_context(
                self.logger, DEBUG, "Periods retrieved in timeframe",
                period_type=period_type.value,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                periods_found=len(periods)
            )
            
            return periods
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting periods in timeframe",
                period_type=period_type.value,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                error=str(e)
            )
            return []

    def get_periods_since(
        self,
        session: Session,
        cutoff_time: datetime,
        period_type: PeriodType
    ) -> List[Period]:
        """
        Get all periods since a cutoff time.
        
        Used by CalculationService.update_event_valuations() and update_analytics()
        to get recent periods for processing.
        """
        try:
            periods = session.query(Period).filter(
                and_(
                    Period.period_type == period_type,
                    Period.timestamp >= cutoff_time
                )
            ).order_by(Period.timestamp).all()
            
            log_with_context(
                self.logger, DEBUG, "Periods retrieved since cutoff",
                period_type=period_type.value,
                cutoff_time=cutoff_time.isoformat(),
                periods_found=len(periods)
            )
            
            return periods
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting periods since cutoff",
                period_type=period_type.value,
                cutoff_time=cutoff_time.isoformat(),
                error=str(e)
            )
            return []

    def get_periods_by_ids(
        self,
        session: Session,
        period_ids: List[int]
    ) -> List[Period]:
        """
        Get periods by their IDs.
        
        Used by CalculationService.update_analytics() to get periods
        for missing analytics processing.
        """
        try:
            periods = session.query(Period).filter(
                Period.id.in_(period_ids)
            ).order_by(Period.timestamp).all()
            
            log_with_context(
                self.logger, DEBUG, "Periods retrieved by IDs",
                requested_ids=len(period_ids),
                periods_found=len(periods)
            )
            
            return periods
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting periods by IDs",
                requested_ids=len(period_ids),
                error=str(e)
            )
            return []

    def get_recent_periods(
        self,
        session: Session,
        period_type: PeriodType,
        limit: int = 1000
    ) -> List[Period]:
        """
        Get recent periods of a specific type.
        
        Used by various service methods for gap detection and recent processing.
        """
        try:
            periods = session.query(Period).filter(
                Period.period_type == period_type
            ).order_by(desc(Period.timestamp)).limit(limit).all()
            
            log_with_context(
                self.logger, DEBUG, "Recent periods retrieved",
                period_type=period_type.value,
                limit=limit,
                periods_found=len(periods)
            )
            
            return periods
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting recent periods",
                period_type=period_type.value,
                limit=limit,
                error=str(e)
            )
            return []

    def create_periods_to_present(
        self,
        session: Session,
        period_type: PeriodType,
        rpc_client
    ) -> int:
        """
        Create periods from the latest existing period to present time.
        
        Enhanced version that integrates with RPC client for block-timestamp lookup.
        Used by PricingService.update_periods_to_present().
        """
        try:
            # Get the latest existing period
            latest_period = self.get_latest_period(session, period_type)
            
            if latest_period:
                start_time = latest_period.time_close + 1
                start_block = latest_period.block_close + 1
            else:
                # No periods exist - start from a reasonable genesis point
                # You may want to customize this based on your chain
                start_time = 1640995200  # Jan 1, 2022 as example
                start_block = 1
            
            # Get current block info from RPC
            current_block_info = rpc_client.get_latest_block()
            current_time = current_block_info.get('timestamp', int(datetime.now(timezone.utc).timestamp()))
            current_block = current_block_info.get('number', start_block)
            
            # Calculate period duration based on type
            period_duration = self._get_period_duration(period_type)
            
            # Generate periods
            created_count = 0
            period_start_time = start_time
            period_start_block = start_block
            
            while period_start_time < current_time:
                period_end_time = period_start_time + period_duration - 1
                
                # Estimate end block (simplified - you may want more sophisticated block time calculation)
                estimated_block_duration = max(1, (current_block - start_block) * period_duration // (current_time - start_time))
                period_end_block = period_start_block + estimated_block_duration
                
                # Create period
                period = self.create_period(
                    session,
                    period_type=period_type,
                    time_open=period_start_time,
                    time_close=period_end_time,
                    block_open=period_start_block,
                    block_close=period_end_block,
                    is_complete=period_end_time < current_time
                )
                
                if period:
                    created_count += 1
                
                # Move to next period
                period_start_time = period_end_time + 1
                period_start_block = period_end_block + 1
            
            log_with_context(
                self.logger, INFO, "Periods created to present",
                period_type=period_type.value,
                created_count=created_count
            )
            
            return created_count
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error creating periods to present",
                period_type=period_type.value,
                error=str(e)
            )
            return 0

    def _get_period_duration(self, period_type: PeriodType) -> int:
        """Get duration in seconds for a period type"""
        durations = {
            PeriodType.ONE_MINUTE: 60,
            PeriodType.FIVE_MINUTE: 300,
            PeriodType.ONE_HOUR: 3600,
            PeriodType.ONE_DAY: 86400
        }
        return durations.get(period_type, 300)  # Default to 5 minutes

    def get_period_gaps(
        self,
        session: Session,
        period_type: PeriodType,
        start_time: datetime,
        end_time: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """
        Find gaps in period coverage within a time range.
        
        Returns list of (gap_start, gap_end) datetime tuples.
        Used for gap detection and backfill operations.
        """
        try:
            # Convert to timestamps for comparison
            start_timestamp = int(start_time.timestamp())
            end_timestamp = int(end_time.timestamp())
            
            # Get existing periods in range
            periods = session.query(Period).filter(
                and_(
                    Period.period_type == period_type,
                    Period.timestamp >= start_time,
                    Period.timestamp <= end_time
                )
            ).order_by(Period.timestamp).all()
            
            if not periods:
                return [(start_time, end_time)]
            
            gaps = []
            current_time = start_time
            
            for period in periods:
                # Check for gap before this period
                if period.timestamp > current_time:
                    gaps.append((current_time, period.timestamp))
                
                # Move current time to end of this period
                period_duration = self._get_period_duration(period_type)
                period_end = period.timestamp + timedelta(seconds=period_duration)
                current_time = max(current_time, period_end)
            
            # Check for gap after last period
            if current_time < end_time:
                gaps.append((current_time, end_time))
            
            log_with_context(
                self.logger, DEBUG, "Period gaps identified",
                period_type=period_type.value,
                gaps_found=len(gaps)
            )
            
            return gaps
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error finding period gaps",
                period_type=period_type.value,
                error=str(e)
            )
            return []