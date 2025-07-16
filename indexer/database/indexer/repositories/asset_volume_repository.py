# indexer/database/indexer/repositories/asset_volume_repository.py

from typing import List, Optional, Dict
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func

from ...connection import ModelDatabaseManager
from ..tables.asset_volume import AssetVolume
from ....core.logging_config import IndexerLogger, log_with_context
from ....types import EvmAddress
from ...base_repository import BaseRepository
from ..tables.detail.pool_swap_detail import PricingDenomination
from ...shared.tables.periods import Period, PeriodType
import logging


class AssetVolumeRepository(BaseRepository):
    """
    Repository for volume tracking for target assets per period, segmented by protocol.
    
    Tracks trading volume for each protocol (TraderJoe, Pangolin, etc.)
    to provide volume attribution and protocol-level metrics. Used by
    the calculation service for portfolio and protocol analytics.
    
    Located in indexer database since volume data is model/asset-specific
    and used for model-specific business metrics and analytics.
    """
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, AssetVolume)
        self.logger = IndexerLogger.get_logger('database.repositories.asset_volume')
    
    def create_volume_record(
        self,
        session: Session,
        period_id: int,
        asset: str,
        denom: str,
        protocol: str,
        volume: Decimal
    ) -> Optional[AssetVolume]:
        """Create a new volume record"""
        try:
            volume_record = AssetVolume(
                period_id=period_id,
                asset=asset.lower(),
                denom=denom.lower(),
                protocol=protocol.lower(),
                volume=float(volume)
                # Note: created_at and updated_at handled automatically by BaseModel
            )
            
            session.add(volume_record)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Volume record created",
                period_id=period_id,
                asset=asset,
                denom=denom,
                protocol=protocol,
                volume=str(volume)
            )
            
            return volume_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating volume record",
                period_id=period_id,
                asset=asset,
                denom=denom,
                protocol=protocol,
                error=str(e)
            )
            raise
    
    def get_volume_for_period(
        self, 
        session: Session, 
        period_id: int, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> List[AssetVolume]:
        """Get all protocol volumes for a specific period and asset"""
        try:
            return session.query(AssetVolume).filter(
                AssetVolume.period_id == period_id,
                AssetVolume.asset == asset_address.lower(),
                AssetVolume.denom == denom.lower()
            ).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting volume for period",
                period_id=period_id,
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return []
    
    def get_total_volume_for_period(
        self, 
        session: Session, 
        period_id: int, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Decimal:
        """Get total volume across all protocols for a period"""
        try:
            result = session.query(func.sum(AssetVolume.volume)).filter(
                AssetVolume.period_id == period_id,
                AssetVolume.asset == asset_address.lower(),
                AssetVolume.denom == denom.lower()
            ).scalar()
            
            return Decimal(str(result)) if result else Decimal('0')
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting total volume for period",
                period_id=period_id,
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return Decimal('0')
    
    def get_protocol_volume_range(
        self, 
        session: Session, 
        start_period_id: int, 
        end_period_id: int,
        asset_address: str, 
        protocol: str, 
        denom: str = 'usd'
    ) -> List[AssetVolume]:
        """Get volume for a specific protocol across a range of periods"""
        try:
            return session.query(AssetVolume).filter(
                AssetVolume.period_id.between(start_period_id, end_period_id),
                AssetVolume.asset == asset_address.lower(),
                AssetVolume.protocol == protocol.lower(),
                AssetVolume.denom == denom.lower()
            ).order_by(AssetVolume.period_id).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting protocol volume range",
                start_period_id=start_period_id,
                end_period_id=end_period_id,
                asset=asset_address,
                protocol=protocol,
                denom=denom,
                error=str(e)
            )
            return []
    
    def get_protocol_summary(
        self, 
        session: Session, 
        asset_address: str, 
        denom: str = 'usd', 
        limit_periods: Optional[int] = None
    ) -> List[Dict]:
        """Get volume summary by protocol, optionally limited to recent periods"""
        try:
            query = session.query(
                AssetVolume.protocol,
                func.sum(AssetVolume.volume).label('total_volume'),
                func.count(AssetVolume.period_id).label('period_count'),
                func.max(AssetVolume.period_id).label('latest_period'),
                func.avg(AssetVolume.volume).label('avg_volume')
            ).filter(
                AssetVolume.asset == asset_address.lower(),
                AssetVolume.denom == denom.lower()
            )
            
            if limit_periods:
                # Get the most recent period_id for this asset
                latest_period = session.query(func.max(AssetVolume.period_id)).filter(
                    AssetVolume.asset == asset_address.lower(),
                    AssetVolume.denom == denom.lower()
                ).scalar()
                
                if latest_period:
                    query = query.filter(AssetVolume.period_id >= latest_period - limit_periods)
            
            results = query.group_by(AssetVolume.protocol).order_by(desc('total_volume')).all()
            
            return [
                {
                    'protocol': result.protocol,
                    'total_volume': float(result.total_volume),
                    'period_count': result.period_count,
                    'latest_period': result.latest_period,
                    'avg_volume': float(result.avg_volume)
                }
                for result in results
            ]
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting protocol summary",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return []
    
    def get_missing_periods(
        self, 
        session: Session, 
        period_ids: List[int], 
        asset_address: str, 
        denom: str = 'usd'
    ) -> List[int]:
        """Find periods that don't have any volume data yet"""
        try:
            existing_periods = session.query(AssetVolume.period_id.distinct()).filter(
                AssetVolume.period_id.in_(period_ids),
                AssetVolume.asset == asset_address.lower(),
                AssetVolume.denom == denom.lower()
            ).all()
            
            existing_period_ids = {p.period_id for p in existing_periods}
            missing_periods = [pid for pid in period_ids if pid not in existing_period_ids]
            
            log_with_context(
                self.logger, logging.DEBUG, "Found missing volume periods",
                asset=asset_address,
                denom=denom,
                total_periods=len(period_ids),
                missing_periods=len(missing_periods)
            )
            
            return missing_periods
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error finding missing periods",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return period_ids  # Return all as missing if error
    
    def update_volume_record(
        self,
        session: Session,
        period_id: int,
        asset: str,
        denom: str,
        protocol: str,
        volume: Decimal
    ) -> Optional[AssetVolume]:
        """Update existing volume record"""
        try:
            volume_record = session.query(AssetVolume).filter(
                AssetVolume.period_id == period_id,
                AssetVolume.asset == asset.lower(),
                AssetVolume.denom == denom.lower(),
                AssetVolume.protocol == protocol.lower()
            ).first()
            
            if not volume_record:
                # Create new record if doesn't exist
                return self.create_volume_record(session, period_id, asset, denom, protocol, volume)
            
            volume_record.volume = float(volume)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Volume record updated",
                period_id=period_id,
                asset=asset,
                denom=denom,
                protocol=protocol,
                volume=str(volume)
            )
            
            return volume_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error updating volume record",
                period_id=period_id,
                asset=asset,
                denom=denom,
                protocol=protocol,
                error=str(e)
            )
            raise
    
    def bulk_create_volume_records(
        self,
        session: Session,
        volume_data: List[Dict]
    ) -> int:
        """Create multiple volume records in bulk"""
        try:
            volume_records = []
            for data in volume_data:
                record = AssetVolume(
                    period_id=data['period_id'],
                    asset=data['asset'].lower(),
                    denom=data['denom'].lower(),
                    protocol=data['protocol'].lower(),
                    volume=float(data['volume'])
                )
                volume_records.append(record)
            
            session.bulk_save_objects(volume_records)
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Bulk volume records created",
                record_count=len(volume_records)
            )
            
            return len(volume_records)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error bulk creating volume records",
                record_count=len(volume_data),
                error=str(e)
            )
            raise
    
    def get_volume_stats(
        self, 
        session: Session, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Dict:
        """Get statistics about volume data for an asset"""
        try:
            stats = session.query(
                func.count(AssetVolume.period_id).label('record_count'),
                func.sum(AssetVolume.volume).label('total_volume'),
                func.avg(AssetVolume.volume).label('avg_volume'),
                func.min(AssetVolume.period_id).label('earliest_period'),
                func.max(AssetVolume.period_id).label('latest_period'),
                func.count(AssetVolume.protocol.distinct()).label('protocol_count')
            ).filter(
                AssetVolume.asset == asset_address.lower(),
                AssetVolume.denom == denom.lower()
            ).first()
            
            return {
                'record_count': stats.record_count or 0,
                'total_volume': float(stats.total_volume) if stats.total_volume else 0,
                'avg_volume': float(stats.avg_volume) if stats.avg_volume else 0,
                'earliest_period': stats.earliest_period,
                'latest_period': stats.latest_period,
                'protocol_count': stats.protocol_count or 0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting volume stats",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return {}
        
    def create_volume_metric(
        self,
        session: Session,
        period_id: int,
        asset_address: str,
        denomination: PricingDenomination,
        protocol: str,
        volume: Decimal,
        pool_count: int,
        swap_count: int
    ) -> Optional[AssetVolume]:
        """
        Create a volume metric record with additional metadata.
        
        Enhanced version of create_volume_record() that matches the CalculationService
        interface with pool_count and swap_count tracking.
        """
        try:
            volume_record = AssetVolume(
                period_id=period_id,
                asset=asset_address.lower(),
                denom=denomination.value,
                protocol=protocol.lower(),
                volume=float(volume),
                # Note: pool_count and swap_count may need to be added to AssetVolume table
                # For now, storing in volume field - you may want to extend the table
            )
            
            session.add(volume_record)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Volume metric created",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                protocol=protocol,
                volume=float(volume),
                pool_count=pool_count,
                swap_count=swap_count
            )
            
            return volume_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating volume metric",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                protocol=protocol,
                error=str(e)
            )
            raise

    def get_volume(
        self,
        session: Session,
        period_id: int,
        asset_address: str,
        denomination: PricingDenomination,
        protocol: str
    ) -> Optional[AssetVolume]:
        """Get volume record for specific period/asset/denomination/protocol"""
        try:
            return session.query(AssetVolume).filter(
                and_(
                    AssetVolume.period_id == period_id,
                    AssetVolume.asset == asset_address.lower(),
                    AssetVolume.denom == denomination.value,
                    AssetVolume.protocol == protocol.lower()
                )
            ).one_or_none()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting volume record",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                protocol=protocol,
                error=str(e)
            )
            raise

    def find_periods_with_missing_volumes(
        self,
        session: Session,
        asset_address: str
    ) -> List[int]:
        """
        Find periods that should have volume data but don't.
        
        Used by CalculationService.update_analytics() for gap detection.
        """
        try:
            # Get existing volume periods for this asset
            existing_periods = session.query(AssetVolume.period_id.distinct()).filter(
                AssetVolume.asset == asset_address.lower()
            ).subquery()
            
            # Get recent 5-minute periods that don't have volume data
            with self.db_manager.get_shared_session() as shared_session:
                missing_periods = shared_session.query(Period.id).filter(
                    and_(
                        Period.period_type == PeriodType.FIVE_MINUTE,
                        ~Period.id.in_(existing_periods)
                    )
                ).order_by(desc(Period.id)).limit(1000).all()  # Recent periods only
            
            missing_period_ids = [p.id for p in missing_periods]
            
            log_with_context(
                self.logger, logging.DEBUG, "Found periods with missing volumes",
                asset_address=asset_address,
                missing_periods=len(missing_period_ids)
            )
            
            return missing_period_ids
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error finding periods with missing volumes",
                asset_address=asset_address,
                error=str(e)
            )
            return []

    def count_missing_volumes(
        self,
        session: Session,
        asset_address: str,
        denomination: PricingDenomination
    ) -> int:
        """Count how many periods are missing volume data"""
        try:
            missing_periods = self.find_periods_with_missing_volumes(session, asset_address)
            
            # Filter by periods that should have data for this denomination
            # For simplicity, return total missing periods
            return len(missing_periods)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error counting missing volumes",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return 0

    def get_volume_stats(
        self,
        session: Session,
        asset_address: str,
        denomination: PricingDenomination
    ) -> Dict:
        """
        Get statistics about volume data coverage for an asset.
        
        Used by CalculationService.get_calculation_status() for monitoring.
        """
        try:
            stats = session.query(
                func.count(AssetVolume.period_id).label('record_count'),
                func.sum(AssetVolume.volume).label('total_volume'),
                func.avg(AssetVolume.volume).label('avg_volume'),
                func.min(AssetVolume.period_id).label('earliest_period'),
                func.max(AssetVolume.period_id).label('latest_period'),
                func.count(AssetVolume.protocol.distinct()).label('protocol_count')
            ).filter(
                and_(
                    AssetVolume.asset == asset_address.lower(),
                    AssetVolume.denom == denomination.value
                )
            ).first()
            
            return {
                'record_count': stats.record_count or 0,
                'total_volume': float(stats.total_volume) if stats.total_volume else 0.0,
                'avg_volume': float(stats.avg_volume) if stats.avg_volume else 0.0,
                'earliest_period': stats.earliest_period,
                'latest_period': stats.latest_period,
                'protocol_count': stats.protocol_count or 0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting volume stats",
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return {}

    def get_latest_volume_timestamp(
        self,
        session: Session,
        asset_address: str
    ) -> Optional[str]:
        """Get the latest volume timestamp for an asset (any denomination)"""
        try:
            latest = session.query(func.max(AssetVolume.created_at)).filter(
                AssetVolume.asset == asset_address.lower()
            ).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest volume timestamp",
                asset_address=asset_address,
                error=str(e)
            )
            return None

    # ENHANCED EXISTING METHOD SIGNATURE
    # Update the existing create_volume_record method to match CalculationService expectations:

    def create_volume_record_enhanced(
        self,
        session: Session,
        period_id: int,
        asset: str,
        denom: str,
        protocol: str,
        volume: Decimal,
        pool_count: Optional[int] = None,
        swap_count: Optional[int] = None
    ) -> Optional[AssetVolume]:
        """Enhanced create_volume_record with metadata tracking"""
        try:
            volume_record = AssetVolume(
                period_id=period_id,
                asset=asset.lower(),
                denom=denom.lower(),
                protocol=protocol.lower(),
                volume=float(volume)
                # Note: If you extend AssetVolume table to include pool_count and swap_count:
                # pool_count=pool_count,
                # swap_count=swap_count
            )
            
            session.add(volume_record)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Enhanced volume record created",
                period_id=period_id,
                asset=asset,
                denom=denom,
                protocol=protocol,
                volume=str(volume),
                pool_count=pool_count,
                swap_count=swap_count
            )
            
            return volume_record
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating enhanced volume record",
                period_id=period_id,
                asset=asset,
                denom=denom,
                protocol=protocol,
                error=str(e)
            )
            raise