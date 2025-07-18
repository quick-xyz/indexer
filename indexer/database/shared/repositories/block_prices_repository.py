# indexer/database/shared/repositories/block_prices_repository.py

from typing import List, Optional, Tuple, Dict
from decimal import Decimal
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, asc, func
from sqlalchemy.exc import IntegrityError

from ..tables.block_prices import BlockPrice
from ...base_repository import BaseRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL


class BlockPricesRepository(BaseRepository):
    """
    Repository for AVAX block-level and time-based pricing data.
    
    Uses shared database connection since BlockPrice is chain-level data
    shared across all indexers.
    """
    
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
                self.logger, DEBUG, "Block price created",
                block_number=block_number,
                price_usd=str(price_usd),
                timestamp=timestamp
            )
            
            return price_record
            
        except IntegrityError:
            # Block already has a price - this is expected in some cases
            log_with_context(
                self.logger, DEBUG, "Block price already exists",
                block_number=block_number
            )
            session.rollback()
            return None
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error creating block price",
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
            session: Database session
            timestamp: Target timestamp
            tolerance_seconds: Maximum time difference allowed
            
        Returns:
            BlockPrice object or None if no price within tolerance
        """
        # Find the closest price within tolerance
        closest_price = session.query(BlockPrice).filter(
            BlockPrice.timestamp.between(
                timestamp - tolerance_seconds,
                timestamp + tolerance_seconds
            )
        ).order_by(
            # Order by absolute difference from target timestamp
            func.abs(BlockPrice.timestamp - timestamp)
        ).first()
        
        if closest_price:
            log_with_context(
                self.logger, DEBUG, "Found price near timestamp",
                target_timestamp=timestamp,
                found_timestamp=closest_price.timestamp,
                time_difference=abs(closest_price.timestamp - timestamp),
                block_number=closest_price.block_number
            )
        
        return closest_price
    
    def get_price_before_timestamp(self, session: Session, timestamp: int) -> Optional[BlockPrice]:
        """Get the most recent price before or at a specific timestamp."""
        return session.query(BlockPrice).filter(
            BlockPrice.timestamp <= timestamp
        ).order_by(BlockPrice.timestamp.desc()).first()
    
    def get_price_range(self, session: Session, start_block: int, end_block: int) -> List[BlockPrice]:
        """Get AVAX prices for a range of blocks."""
        return session.query(BlockPrice).filter(
            BlockPrice.block_number.between(start_block, end_block)
        ).order_by(BlockPrice.block_number).all()
    
    def get_latest_price(self, session: Session) -> Optional[BlockPrice]:
        """Get the most recent AVAX price."""
        return session.query(BlockPrice).order_by(
            BlockPrice.block_number.desc()
        ).first()
    
    def get_earliest_price(self, session: Session) -> Optional[BlockPrice]:
        """Get the earliest AVAX price."""
        return session.query(BlockPrice).order_by(
            BlockPrice.block_number.asc()
        ).first()
    
    def get_price_gaps(self, session: Session, start_block: int, end_block: int) -> List[Tuple[int, int]]:
        """
        Find gaps in price data within a block range.
        
        Returns:
            List of (gap_start_block, gap_end_block) tuples
        """
        # Get all prices in range
        prices = session.query(BlockPrice.block_number).filter(
            BlockPrice.block_number.between(start_block, end_block)
        ).order_by(BlockPrice.block_number).all()
        
        if not prices:
            return [(start_block, end_block)]
        
        gaps = []
        price_blocks = [p.block_number for p in prices]
        
        # Check for gap before first price
        if price_blocks[0] > start_block:
            gaps.append((start_block, price_blocks[0] - 1))
        
        # Check for gaps between prices
        for i in range(len(price_blocks) - 1):
            current_block = price_blocks[i]
            next_block = price_blocks[i + 1]
            
            if next_block > current_block + 1:
                gaps.append((current_block + 1, next_block - 1))
        
        # Check for gap after last price
        if price_blocks[-1] < end_block:
            gaps.append((price_blocks[-1] + 1, end_block))
        
        log_with_context(
            self.logger, DEBUG, "Price gaps identified",
            start_block=start_block,
            end_block=end_block,
            gap_count=len(gaps),
            total_prices=len(price_blocks)
        )
        
        return gaps
    
    def get_price_stats(self, session: Session) -> Dict:
        """Get statistics about price data."""
        stats = session.query(
            func.count(BlockPrice.block_number).label('total_records'),
            func.min(BlockPrice.block_number).label('earliest_block'),
            func.max(BlockPrice.block_number).label('latest_block'),
            func.min(BlockPrice.price_usd).label('min_price_usd'),
            func.max(BlockPrice.price_usd).label('max_price_usd'),
            func.avg(BlockPrice.price_usd).label('avg_price_usd')
        ).first()
        
        return {
            'total_records': stats.total_records or 0,
            'earliest_block': stats.earliest_block,
            'latest_block': stats.latest_block,
            'min_price_usd': float(stats.min_price_usd) if stats.min_price_usd else None,
            'max_price_usd': float(stats.max_price_usd) if stats.max_price_usd else None,
            'avg_price_usd': float(stats.avg_price_usd) if stats.avg_price_usd else None
        }
    
    def bulk_create_prices(
        self, 
        session: Session, 
        price_data: List[Dict]
    ) -> Tuple[int, int]:
        """
        Bulk create multiple price records.
        
        Args:
            session: Database session
            price_data: List of dicts with keys: block_number, timestamp, price_usd
            
        Returns:
            Tuple of (created_count, skipped_count)
        """
        created_count = 0
        skipped_count = 0
        
        for data in price_data:
            try:
                price_record = BlockPrice(
                    block_number=data['block_number'],
                    timestamp=data['timestamp'],
                    price_usd=data['price_usd'],
                    chainlink_round_id=data.get('chainlink_round_id'),
                    chainlink_updated_at=data.get('chainlink_updated_at')
                )
                
                session.add(price_record)
                created_count += 1
                
            except IntegrityError:
                # Skip duplicates
                session.rollback()
                skipped_count += 1
                continue
            except Exception as e:
                log_with_context(
                    self.logger, ERROR, "Error in bulk price creation",
                    block_number=data.get('block_number'),
                    error=str(e)
                )
                session.rollback()
                skipped_count += 1
                continue
        
        try:
            session.flush()
            log_with_context(
                self.logger, INFO, "Bulk price creation completed",
                total_attempted=len(price_data),
                created_count=created_count,
                skipped_count=skipped_count
            )
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error committing bulk prices",
                error=str(e)
            )
            raise
        
        return created_count, skipped_count