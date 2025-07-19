# indexer/database/model/repositories/asset_volume_repository.py

from typing import List, Optional, Dict
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func

from ...connection import ModelDatabaseManager
from ...base_repository import BaseRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

from ..tables import DBAssetVolume
from ...types import PricingDenomination


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
        super().__init__(db_manager, DBAssetVolume)
        self.logger = IndexerLogger.get_logger('database.repositories.asset_volume')
    
    def create_volume_record(
        self,
        session: Session,
        period_id: int,
        asset_address: str,
        denomination: str,
        protocol: str,
        volume: Decimal
    ) -> Optional[DBAssetVolume]:
        """Create a new volume record - MAIN METHOD"""
        try:
            volume_record = DBAssetVolume(
                period_id=period_id,
                asset=asset_address.lower(),             # ✅ Correct: Table uses 'asset'
                denom=denomination.lower(),              # ✅ Correct: Table uses 'denom'
                protocol=protocol.lower(),               # ✅ Correct: Table uses 'protocol'
                volume=float(volume)                     # ✅ Correct: Table uses 'volume'
            )
            
            session.add(volume_record)
            session.flush()
            
            log_with_context(
                self.logger, DEBUG, "Volume record created",
                period_id=period_id,
                asset=asset_address,
                denom=denomination,
                protocol=protocol,
                volume=str(volume)
            )
            
            return volume_record
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error creating volume record",
                period_id=period_id,
                asset=asset_address,
                denom=denomination,
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
    ) -> List[DBAssetVolume]:
        """Get all protocol volumes for a specific period and asset"""
        try:
            return session.query(DBAssetVolume).filter(
                DBAssetVolume.period_id == period_id,
                DBAssetVolume.asset == asset_address.lower(),    # ✅ Correct: Table uses 'asset'
                DBAssetVolume.denom == denom.lower()             # ✅ Correct: Table uses 'denom'
            ).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting volume for period",
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
            result = session.query(func.sum(DBAssetVolume.volume)).filter(
                DBAssetVolume.period_id == period_id,
                DBAssetVolume.asset == asset_address.lower(),    # ✅ Correct: Table uses 'asset'
                DBAssetVolume.denom == denom.lower()             # ✅ Correct: Table uses 'denom'
            ).scalar()
            
            return Decimal(str(result)) if result else Decimal('0')
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting total volume for period",
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
    ) -> List[DBAssetVolume]:
        """Get volume for a specific protocol across a range of periods"""
        try:
            return session.query(DBAssetVolume).filter(
                DBAssetVolume.period_id.between(start_period_id, end_period_id),
                DBAssetVolume.asset == asset_address.lower(),    # ✅ Correct: Table uses 'asset'
                DBAssetVolume.protocol == protocol.lower(),      # ✅ Correct: Table uses 'protocol'
                DBAssetVolume.denom == denom.lower()             # ✅ Correct: Table uses 'denom'
            ).order_by(DBAssetVolume.period_id).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting protocol volume range",
                start_period_id=start_period_id,
                end_period_id=end_period_id,
                asset=asset_address,
                protocol=protocol,
                denom=denom,
                error=str(e)
            )
            return []
    
    def get_volume_by_protocol(
        self, 
        session: Session, 
        period_id: int, 
        asset_address: str, 
        denom: str = 'usd'
    ) -> Dict[str, float]:
        """Get volume breakdown by protocol for a specific period"""
        try:
            volumes = session.query(
                DBAssetVolume.protocol, 
                DBAssetVolume.volume
            ).filter(
                DBAssetVolume.period_id == period_id,
                DBAssetVolume.asset == asset_address.lower(),    # ✅ Correct: Table uses 'asset'
                DBAssetVolume.denom == denom.lower()             # ✅ Correct: Table uses 'denom'
            ).all()
            
            return {protocol: float(volume) for protocol, volume in volumes}
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting volume by protocol",
                period_id=period_id,
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return {}
    
    def update_volume_record(
        self,
        session: Session,
        period_id: int,
        asset: str,
        denom: str,
        protocol: str,
        volume: Decimal
    ) -> Optional[DBAssetVolume]:
        """Update or create volume record"""
        try:
            # Try to find existing record
            volume_record = session.query(DBAssetVolume).filter(
                DBAssetVolume.period_id == period_id,
                DBAssetVolume.asset == asset.lower(),           # ✅ Correct: Table uses 'asset'
                DBAssetVolume.denom == denom.lower(),           # ✅ Correct: Table uses 'denom'
                DBAssetVolume.protocol == protocol.lower()      # ✅ Correct: Table uses 'protocol'
            ).first()
            
            if not volume_record:
                # Create new record if doesn't exist
                return self.create_volume_record(session, period_id, asset, denom, protocol, volume)
            
            # Update existing record
            volume_record.volume = float(volume)
            session.flush()
            
            log_with_context(
                self.logger, DEBUG, "Volume record updated",
                period_id=period_id,
                asset=asset,
                denom=denom,
                protocol=protocol,
                volume=str(volume)
            )
            
            return volume_record
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error updating volume record",
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
                record = DBAssetVolume(
                    period_id=data['period_id'],
                    asset=data['asset_address'].lower(),      # ✅ Correct: Table uses 'asset'
                    denom=data['denomination'].lower(),       # ✅ Correct: Table uses 'denom'
                    protocol=data['protocol'].lower(),        # ✅ Correct: Table uses 'protocol'
                    volume=float(data['volume'])              # ✅ Correct: Table uses 'volume'
                )
                volume_records.append(record)
            
            session.bulk_save_objects(volume_records)
            session.flush()
            
            log_with_context(
                self.logger, INFO, "Bulk volume records created",
                record_count=len(volume_records)
            )
            
            return len(volume_records)
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error bulk creating volume records",
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
                func.count(DBAssetVolume.period_id).label('record_count'),
                func.sum(DBAssetVolume.volume).label('total_volume'),
                func.avg(DBAssetVolume.volume).label('avg_volume'),
                func.min(DBAssetVolume.period_id).label('earliest_period'),
                func.max(DBAssetVolume.period_id).label('latest_period'),
                func.count(DBAssetVolume.protocol.distinct()).label('protocol_count')
            ).filter(
                DBAssetVolume.asset == asset_address.lower(),    # ✅ Correct: Table uses 'asset'
                DBAssetVolume.denom == denom.lower()             # ✅ Correct: Table uses 'denom'
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
                self.logger, ERROR, "Error getting volume stats",
                asset=asset_address,
                denom=denom,
                error=str(e)
            )
            return {}
    
    def get_volume_record(
        self,
        session: Session,
        period_id: int,
        asset_address: str,
        denomination: str,
        protocol: str
    ) -> Optional[DBAssetVolume]:
        """Get specific volume record by all identifying fields"""
        try:
            return session.query(DBAssetVolume).filter(
                and_(
                    DBAssetVolume.period_id == period_id,
                    DBAssetVolume.asset == asset_address.lower(),     # ✅ Correct: Table uses 'asset'
                    DBAssetVolume.denom == denomination.lower(),      # ✅ Correct: Table uses 'denom'
                    DBAssetVolume.protocol == protocol.lower()        # ✅ Correct: Table uses 'protocol'
                )
            ).one_or_none()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting volume record",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination,
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
        This is a placeholder - implement based on your business logic.
        """
        try:
            # Placeholder implementation
            # You would implement logic to check against trade_details or pool_swap_details
            # to find periods that have trading activity but missing volume records
            
            log_with_context(
                self.logger, DEBUG, "Finding missing volume periods - implementation needed",
                asset_address=asset_address
            )
            
            return []
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error finding periods with missing volumes",
                asset_address=asset_address,
                error=str(e)
            )
            return []

    # =====================================================================
    # CONVENIENCE METHODS FOR CALCULATION SERVICE
    # =====================================================================
    
    def create_volume_for_denomination(
        self,
        session: Session,
        period_id: int,
        asset_address: str,
        denomination: PricingDenomination,
        protocol: str,
        volume: Decimal
    ) -> Optional[DBAssetVolume]:
        """
        Convenience method that accepts PricingDenomination enum.
        Maps to the main create_volume_record method.
        """
        return self.create_volume_record(
            session=session,
            period_id=period_id,
            asset_address=asset_address,
            denomination=denomination.value,  # Convert enum to string
            protocol=protocol,
            volume=volume
        )