# indexer/database/repositories/block_prices_repository.py

from typing import List, Optional, Tuple
from decimal import Decimal
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc
from sqlalchemy.exc import IntegrityError

from ..models.pricing.block_prices import BlockPrice
from ..repository import BaseRepository
from ...core.logging_config import IndexerLogger, log_with_context

import logging


class BlockPricesRepository(BaseRepository):
    """Repository for AVAX block-level and time-based pricing data."""
    
    def __init__(self, db_manager):
        super().__init__(db_manager, BlockPrice)
        self.logger = IndexerLogger.get_logger('database.repository.block_prices')
    
    def create_block_price(
        self, 
        session: Session, 
        block_number: int, 
        timestamp: int, 
        price_usd: Decimal,
        chainlink_round_id: Optional[int] = None,
        chainlink_updated_at: Optional[int] = None
    ) -> Optional[BlockPrice]:
        """
        Create a new block price record.
        
        Returns None if the block already has a price (no duplicate handling).
        """
        try:
            price_record = BlockPrice(
                block_number=block_number,
                timestamp=timestamp,
                price_usd=price_usd,
                chainlink_round_id=chainlink_round_id,
                chainlink_updated_at=chainlink_updated_at
            )
            
            session.add(price_record)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Block price created",
                block_number=block_number,
                price_usd=str(price_usd),
                timestamp=timestamp
            )
            
            return price_record
            
        except IntegrityError:
            # Block already has a price - this is expected in some cases
            log_with_context(
                self.logger, logging.DEBUG, "Block price already exists",
                block_number=block_number
            )
            session.rollback()
            return None
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating block price",
                block_number=block_number,
                error=str(e)
            )
            raise
    
    def get_price_at_block(self, session: Session, block_number: int) -> Optional[BlockPrice]:
        """Get the exact price for a specific block."""
        return session.query(BlockPrice).filter(
            BlockPrice.block_number == block_number
        ).first()
    
    def get_price_near_timestamp(self, session: Session, timestamp: int, tolerance_seconds: int = 300) -> Optional[BlockPrice]:
        """
        Get price closest to a timestamp within tolerance.
        
        Args:
            timestamp: Target timestamp
            tolerance_seconds: Maximum time difference allowed (default 5 minutes)
        """
        return session.query(BlockPrice).filter(
            and_(
                BlockPrice.timestamp >= timestamp - tolerance_seconds,
                BlockPrice.timestamp <= timestamp + tolerance_seconds
            )
        ).order_by(
            # Order by closest timestamp first
            (BlockPrice.timestamp - timestamp).abs()
        ).first()
    
    def get_price_at_or_before_timestamp(self, session: Session, timestamp: int) -> Optional[BlockPrice]:
        """Get the most recent price at or before the given timestamp."""
        return session.query(BlockPrice).filter(
            BlockPrice.timestamp <= timestamp
        ).order_by(BlockPrice.timestamp.desc()).first()
    
    def get_price_range(
        self, 
        session: Session, 
        start_block: int, 
        end_block: int
    ) -> List[BlockPrice]:
        """Get all prices in a block range."""
        return session.query(BlockPrice).filter(
            BlockPrice.block_number.between(start_block, end_block)
        ).order_by(BlockPrice.block_number).all()
    
    def get_price_time_series(
        self, 
        session: Session, 
        start_timestamp: int, 
        end_timestamp: int,
        limit: Optional[int] = None
    ) -> List[BlockPrice]:
        """Get price time series for a timestamp range."""
        query = session.query(BlockPrice).filter(
            BlockPrice.timestamp.between(start_timestamp, end_timestamp)
        ).order_by(BlockPrice.timestamp)
        
        if limit:
            query = query.limit(limit)
            
        return query.all()
    
    def get_latest_price(self, session: Session) -> Optional[BlockPrice]:
        """Get the most recent price record."""
        return session.query(BlockPrice).order_by(
            BlockPrice.block_number.desc()
        ).first()
    
    def get_missing_blocks(
        self, 
        session: Session, 
        start_block: int, 
        end_block: int
    ) -> List[int]:
        """
        Find blocks in range that don't have prices.
        Useful for backfilling missing data.
        """
        existing_blocks = session.query(BlockPrice.block_number).filter(
            BlockPrice.block_number.between(start_block, end_block)
        ).all()
        
        existing_set = {block[0] for block in existing_blocks}
        all_blocks = set(range(start_block, end_block + 1))
        missing_blocks = sorted(all_blocks - existing_set)
        
        return missing_blocks
    
    def get_price_gaps(
        self, 
        session: Session, 
        start_timestamp: int, 
        end_timestamp: int, 
        expected_interval_seconds: int = 60
    ) -> List[Tuple[int, int]]:
        """
        Find time gaps larger than expected interval in price data.
        Returns list of (gap_start_timestamp, gap_end_timestamp) tuples.
        """
        prices = session.query(BlockPrice).filter(
            BlockPrice.timestamp.between(start_timestamp, end_timestamp)
        ).order_by(BlockPrice.timestamp).all()
        
        gaps = []
        for i in range(1, len(prices)):
            time_diff = prices[i].timestamp - prices[i-1].timestamp
            if time_diff > expected_interval_seconds * 2:  # Allow some tolerance
                gaps.append((prices[i-1].timestamp, prices[i].timestamp))
        
        return gaps
    
    def bulk_create_prices(
        self, 
        session: Session, 
        price_data: List[dict]
    ) -> Tuple[int, int]:
        """
        Bulk insert price records.
        
        Args:
            price_data: List of dicts with keys: block_number, timestamp, price_usd, etc.
            
        Returns:
            Tuple of (successful_inserts, failed_inserts)
        """
        successful = 0
        failed = 0
        
        for data in price_data:
            try:
                price_record = BlockPrice(**data)
                session.add(price_record)
                successful += 1
            except Exception as e:
                log_with_context(
                    self.logger, logging.WARNING, "Failed to create price record",
                    block_number=data.get('block_number'),
                    error=str(e)
                )
                failed += 1
        
        try:
            session.flush()
            log_with_context(
                self.logger, logging.INFO, "Bulk price insert completed",
                successful=successful,
                failed=failed
            )
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Bulk price insert failed",
                error=str(e)
            )
            session.rollback()
            raise
        
        return successful, failed
    
    def get_price_stats(self, session: Session) -> dict:
        """Get basic statistics about price data."""
        from sqlalchemy import func
        
        stats = session.query(
            func.count(BlockPrice.block_number).label('total_records'),
            func.min(BlockPrice.block_number).label('earliest_block'),
            func.max(BlockPrice.block_number).label('latest_block'),
            func.min(BlockPrice.timestamp).label('earliest_timestamp'),
            func.max(BlockPrice.timestamp).label('latest_timestamp'),
            func.avg(BlockPrice.price_usd).label('avg_price'),
            func.min(BlockPrice.price_usd).label('min_price'),
            func.max(BlockPrice.price_usd).label('max_price')
        ).first()
        
        return {
            'total_records': stats.total_records or 0,
            'earliest_block': stats.earliest_block,
            'latest_block': stats.latest_block,
            'earliest_timestamp': stats.earliest_timestamp,
            'latest_timestamp': stats.latest_timestamp,
            'avg_price_usd': float(stats.avg_price) if stats.avg_price else None,
            'min_price_usd': float(stats.min_price) if stats.min_price else None,
            'max_price_usd': float(stats.max_price) if stats.max_price else None,
        }