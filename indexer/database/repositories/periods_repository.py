# indexer/database/repositories/periods_repository.py

from typing import List, Optional, Tuple
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc, func
from sqlalchemy.exc import IntegrityError

from ..models.pricing.periods import Period, PeriodType
from ..repository import BaseRepository
from ...core.logging_config import IndexerLogger, log_with_context

import logging


class PeriodsRepository(BaseRepository):
    """Repository for time period data."""
    
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
                self.logger, logging.DEBUG, "Period created",
                period_type=period_type.value,
                time_open=time_open,
                block_range=f"{block_open}-{block_close}"
            )
            
            return period
            
        except IntegrityError:
            # Period already exists
            log_with_context(
                self.logger, logging.DEBUG, "Period already exists",
                period_type=period_type.value,
                time_open=time_open
            )
            session.rollback()
            return None
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating period",
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
                Period.period_type == period_type,
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
        query = session.query(Period).filter(Period.period_type == period_type)
        
        if order_desc:
            query = query.order_by(Period.time_open.desc())
        else:
            query = query.order_by(Period.time_open.asc())
        
        if limit:
            query = query.limit(limit)
        
        return query.all()
    
    def get_periods_in_time_range(
        self,
        session: Session,
        period_type: PeriodType,
        start_timestamp: int,
        end_timestamp: int
    ) -> List[Period]:
        """Get periods that overlap with a time range."""
        return session.query(Period).filter(
            and_(
                Period.period_type == period_type,
                Period.time_open < end_timestamp,
                Period.time_close >= start_timestamp
            )
        ).order_by(Period.time_open).all()
    
    def get_periods_in_block_range(
        self,
        session: Session,
        period_type: PeriodType,
        start_block: int,
        end_block: int
    ) -> List[Period]:
        """Get periods that contain blocks in the specified range."""
        return session.query(Period).filter(
            and_(
                Period.period_type == period_type,
                Period.block_open <= end_block,
                Period.block_close >= start_block
            )
        ).order_by(Period.time_open).all()
    
    def get_latest_period(
        self, 
        session: Session, 
        period_type: PeriodType
    ) -> Optional[Period]:
        """Get the most recent period for a type."""
        return session.query(Period).filter(
            Period.period_type == period_type
        ).order_by(Period.time_open.desc()).first()
    
    def get_earliest_period(
        self, 
        session: Session, 
        period_type: PeriodType
    ) -> Optional[Period]:
        """Get the oldest period for a type."""
        return session.query(Period).filter(
            Period.period_type == period_type
        ).order_by(Period.time_open.asc()).first()
    
    def get_incomplete_periods(
        self, 
        session: Session, 
        period_type: Optional[PeriodType] = None
    ) -> List[Period]:
        """Get periods that are marked as incomplete."""
        query = session.query(Period).filter(Period.is_complete == 0)
        
        if period_type:
            query = query.filter(Period.period_type == period_type)
        
        return query.order_by(Period.time_open).all()
    
    def get_period_gaps(
        self,
        session: Session,
        period_type: PeriodType,
        start_timestamp: Optional[int] = None,
        end_timestamp: Optional[int] = None
    ) -> List[Tuple[int, int]]:
        """
        Find gaps in period coverage.
        
        Returns list of (gap_start_timestamp, gap_end_timestamp) tuples.
        """
        query = session.query(Period).filter(Period.period_type == period_type)
        
        if start_timestamp:
            query = query.filter(Period.time_close >= start_timestamp)
        if end_timestamp:
            query = query.filter(Period.time_open <= end_timestamp)
        
        periods = query.order_by(Period.time_open).all()
        
        gaps = []
        for i in range(1, len(periods)):
            prev_close = periods[i-1].time_close
            curr_open = periods[i].time_open
            
            # Check if there's a gap (more than 1 second difference)
            if curr_open > prev_close + 1:
                gaps.append((prev_close + 1, curr_open - 1))
        
        return gaps
    
    def mark_period_complete(
        self,
        session: Session,
        period_type: PeriodType,
        time_open: int
    ) -> bool:
        """Mark a period as complete."""
        try:
            period = self.get_period(session, period_type, time_open)
            if period:
                period.is_complete = 1
                session.flush()
                
                log_with_context(
                    self.logger, logging.DEBUG, "Period marked complete",
                    period_type=period_type.value,
                    time_open=time_open
                )
                return True
            return False
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error marking period complete",
                period_type=period_type.value,
                time_open=time_open,
                error=str(e)
            )
            raise
    
    def get_period_stats(self, session: Session) -> dict:
        """Get statistics about period data."""
        stats = {}
        
        for period_type in PeriodType:
            type_stats = session.query(
                func.count(Period.time_open).label('total_periods'),
                func.sum(Period.is_complete).label('complete_periods'),
                func.min(Period.time_open).label('earliest_time'),
                func.max(Period.time_close).label('latest_time'),
                func.min(Period.block_open).label('earliest_block'),
                func.max(Period.block_close).label('latest_block')
            ).filter(Period.period_type == period_type).first()
            
            stats[period_type.value] = {
                'total_periods': type_stats.total_periods or 0,
                'complete_periods': type_stats.complete_periods or 0,
                'incomplete_periods': (type_stats.total_periods or 0) - (type_stats.complete_periods or 0),
                'earliest_timestamp': type_stats.earliest_time,
                'latest_timestamp': type_stats.latest_time,
                'earliest_block': type_stats.earliest_block,
                'latest_block': type_stats.latest_block
            }
        
        return stats
    
    def delete_periods_before(
        self,
        session: Session,
        period_type: PeriodType,
        before_timestamp: int
    ) -> int:
        """
        Delete periods before a certain timestamp.
        Useful for data retention policies.
        """
        try:
            deleted_count = session.query(Period).filter(
                and_(
                    Period.period_type == period_type,
                    Period.time_close < before_timestamp
                )
            ).delete(synchronize_session=False)
            
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Old periods deleted",
                period_type=period_type.value,
                before_timestamp=before_timestamp,
                deleted_count=deleted_count
            )
            
            return deleted_count
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error deleting old periods",
                period_type=period_type.value,
                before_timestamp=before_timestamp,
                error=str(e)
            )
            raise