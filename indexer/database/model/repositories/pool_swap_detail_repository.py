# indexer/database/model/repositories/pool_swap_detail_repository.py

from typing import List, Optional, Dict
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, case, func
from decimal import Decimal

from ....types import DomainEventId
from ...connection import ModelDatabaseManager
from ...base_repository import BaseRepository
from ....core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL

from ..tables import DBPoolSwap, DBPoolSwapDetail
from ...shared.tables import DBContract, DBPeriod, DBPricing
from ...types import PricingDenomination, PricingMethod



class PoolSwapDetailRepository(BaseRepository):
    """Repository for pool swap pricing details"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, DBPoolSwapDetail)
        self.logger = IndexerLogger.get_logger('database.repositories.pool_swap_detail')

    def create_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        value: float,
        price: float,
        price_method: PricingMethod,
        price_config_id: Optional[int] = None
    ) -> DBPoolSwapDetail:
        """Create a new pool swap detail record"""
        try:
            detail = DBPoolSwapDetail(
                content_id=content_id,           # ✅ Correct field name
                denom=denom,                     # ✅ Correct field name
                value=value,                     # ✅ Correct field name
                price=price,                     # ✅ Correct field name
                price_method=price_method,       # ✅ Correct field name
                price_config_id=price_config_id  # ✅ Correct field name
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(self.logger, DEBUG, "Pool swap detail created",
                            content_id=content_id,
                            denom=denom.value,
                            value=value,
                            price_method=price_method.value)
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error creating pool swap detail",
                            content_id=content_id,
                            denom=denom.value if denom else None,
                            error=str(e))
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[DBPoolSwapDetail]:
        """Get all pricing details for a pool swap"""
        try:
            return session.query(DBPoolSwapDetail).filter(
                DBPoolSwapDetail.content_id == content_id
            ).order_by(DBPoolSwapDetail.denom).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting details by content_id",
                            content_id=content_id,
                            error=str(e))
            raise
    
    def get_by_content_id_and_denom(
        self, 
        session: Session, 
        content_id: DomainEventId, 
        denom: PricingDenomination
    ) -> Optional[DBPoolSwapDetail]:
        """Get specific denomination detail for a pool swap"""
        try:
            return session.query(DBPoolSwapDetail).filter(
                and_(
                    DBPoolSwapDetail.content_id == content_id,
                    DBPoolSwapDetail.denom == denom               # ✅ Correct field name
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting detail by content_id and denom",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_by_pricing_method(
        self, 
        session: Session, 
        price_method: PricingMethod, 
        limit: int = 100
    ) -> List[DBPoolSwapDetail]:
        """Get details by pricing method"""
        try:
            return session.query(DBPoolSwapDetail).filter(
                DBPoolSwapDetail.price_method == price_method  # ✅ Correct field name
            ).order_by(desc(DBPoolSwapDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting details by pricing method",
                            price_method=price_method.value,
                            error=str(e))
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[DBPoolSwapDetail]:
        """Get USD valuation details"""
        try:
            return session.query(DBPoolSwapDetail).filter(
                DBPoolSwapDetail.denom == PricingDenomination.USD  # ✅ Correct field name
            ).order_by(desc(DBPoolSwapDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting USD valuations",
                            error=str(e))
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[DBPoolSwapDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(DBPoolSwapDetail).filter(
                DBPoolSwapDetail.denom == PricingDenomination.AVAX  # ✅ Correct field name
            ).order_by(desc(DBPoolSwapDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting AVAX valuations",
                            error=str(e))
            raise
    
    def update_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        **updates
    ) -> Optional[DBPoolSwapDetail]:
        """Update existing pool swap detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denom)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(self.logger, DEBUG, "Pool swap detail updated",
                            content_id=content_id,
                            denom=denom.value,
                            updates=list(updates.keys()))
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error updating pool swap detail",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_usd_details_for_swaps(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId]
    ) -> List[DBPoolSwapDetail]:
        """Get USD valuation details for multiple swaps"""
        try:
            return session.query(DBPoolSwapDetail).filter(
                and_(
                    DBPoolSwapDetail.content_id.in_(swap_content_ids),
                    DBPoolSwapDetail.denom == PricingDenomination.USD  # ✅ Correct field name
                )
            ).order_by(DBPoolSwapDetail.content_id).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting USD details for swaps",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            raise
    
    def get_avax_details_for_swaps(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId]
    ) -> List[DBPoolSwapDetail]:
        """Get AVAX valuation details for multiple swaps"""
        try:
            return session.query(DBPoolSwapDetail).filter(
                and_(
                    DBPoolSwapDetail.content_id.in_(swap_content_ids),
                    DBPoolSwapDetail.denom == PricingDenomination.AVAX  # ✅ Correct field name
                )
            ).order_by(DBPoolSwapDetail.content_id).all()
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting AVAX details for swaps",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            raise
    
    def check_all_swaps_have_direct_pricing(
        self,
        session: Session,
        swap_content_ids: List[DomainEventId]
    ) -> bool:
        """Check if all swaps have direct pricing (not global)"""
        try:
            # Count how many swaps have direct pricing (DIRECT_AVAX or DIRECT_USD)
            direct_pricing_count = session.query(DBPoolSwapDetail.content_id.distinct()).filter(
                and_(
                    DBPoolSwapDetail.content_id.in_(swap_content_ids),
                    DBPoolSwapDetail.price_method.in_([PricingMethod.DIRECT_AVAX, PricingMethod.DIRECT_USD])  # ✅ Correct field name
                )
            ).count()
            
            # All swaps should have direct pricing
            return direct_pricing_count == len(swap_content_ids)
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error checking direct pricing eligibility",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            return False
    
    def get_pricing_method_stats(self, session: Session) -> Dict[str, int]:
        """Get statistics about pricing methods used"""
        try:           
            stats = session.query(
                DBPoolSwapDetail.price_method,                    # ✅ Correct field name
                func.count(DBPoolSwapDetail.content_id.distinct()).label('swap_count')
            ).group_by(DBPoolSwapDetail.price_method).all()        # ✅ Correct field name
            
            return {method.value: count for method, count in stats}
            
        except Exception as e:
            log_with_context(self.logger, ERROR, "Error getting pricing method stats",
                            error=str(e))
            return {}
        
    def get_pricing_pool_swaps_in_timeframe(
        self,
        session: Session,
        asset_address: str,
        start_time: datetime,
        end_time: datetime,
        denomination: PricingDenomination
    ) -> List[DBPoolSwapDetail]:
        """
        Get pool swap details from pricing pools within a timeframe.
        
        Used by PricingService.generate_canonical_prices() to get VWAP data
        from designated pricing pools for canonical price calculation.
        """
        try:
            # Join with pool pricing config to find pricing pools
            return session.query(DBPoolSwapDetail).join(
                DBPoolSwap, DBPoolSwapDetail.content_id == DBPoolSwap.content_id
            ).join(
                DBContract, DBPoolSwap.pool == DBContract.address
            ).join(
                DBPricing, 
                and_(
                    DBPricing.contract_id == DBContract.id,
                    DBPricing.pricing_pool == True  # Only pricing pools
                )
            ).filter(
                and_(
                    DBContract.base_token_address == asset_address.lower(),  # Pool configured for this base token
                    DBPoolSwapDetail.denom == denomination,                   # ✅ Fixed: Use correct field name
                    DBPoolSwap.timestamp >= start_time,
                    DBPoolSwap.timestamp <= end_time,
                    DBPoolSwapDetail.price.isnot(None),                      # ✅ Fixed: Use actual price field
                    DBPoolSwapDetail.value.isnot(None)                       # ✅ Fixed: Use actual value field
                )
            ).order_by(DBPoolSwap.timestamp).all()
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting pricing pool swaps in timeframe",
                base_token_address=asset_address,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                denomination=denomination.value,
                error=str(e)
            )
            return []

    def create_global_pricing_detail(
        self,
        session: Session,
        swap: DBPoolSwap,
        denomination: PricingDenomination,
        canonical_price: Decimal,
        pricing_method: PricingMethod
    ) -> Optional[DBPoolSwapDetail]:
        """
        Create a pool swap detail record using canonical pricing.
        
        Used by PricingService.apply_canonical_pricing_to_global_events()
        to price swaps that couldn't be directly priced.
        """
        try:
            # Calculate value using canonical price and swap base amount
            # Assumes swap.base_amount is in human-readable format
            swap_value = float(swap.base_amount) * float(canonical_price)
            
            detail = DBPoolSwapDetail(
                content_id=swap.content_id,      # ✅ Correct field name
                denom=denomination,              # ✅ Fixed: Correct field name
                value=swap_value,                # ✅ Fixed: Use actual table field
                price=float(canonical_price),    # ✅ Fixed: Use actual table field
                price_method=pricing_method      # ✅ Fixed: Correct field name
                # ✅ Removed: pool_address doesn't exist, price_config_id is optional
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, DEBUG, "Global pricing detail created",
                content_id=swap.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                pricing_method=pricing_method.value,
                value=swap_value
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error creating global pricing detail",
                content_id=swap.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                error=str(e)
            )
            raise

    def get_direct_pricing_stats(
        self,
        session: Session,
        asset_address: str
    ) -> Dict:
        """
        Get statistics about direct pricing coverage for an asset.
        
        Used by PricingService.get_pricing_status() for monitoring.
        """
        try:
            # Get stats for swaps involving this asset
            stats = session.query(
                func.count(DBPoolSwapDetail.content_id.distinct()).label('total_swaps'),
                func.count(case(
                    [(DBPoolSwapDetail.price_method.in_([PricingMethod.DIRECT_AVAX, PricingMethod.DIRECT_USD]), 1)],
                    else_=None
                )).label('direct_priced_swaps'),
                func.count(case(
                    [(DBPoolSwapDetail.price_method == PricingMethod.GLOBAL, 1)],
                    else_=None
                )).label('global_priced_swaps'),
                func.count(case(
                    [(DBPoolSwapDetail.denom == PricingDenomination.USD, 1)],  # ✅ Fixed: Correct field name
                    else_=None
                )).label('usd_details'),
                func.count(case(
                    [(DBPoolSwapDetail.denom == PricingDenomination.AVAX, 1)], # ✅ Fixed: Correct field name
                    else_=None
                )).label('avax_details')
            ).join(
                DBPoolSwap, DBPoolSwapDetail.content_id == DBPoolSwap.content_id
            ).filter(
                # Filter for swaps involving the asset (either asset_in or asset_out)
                (DBPoolSwap.asset_in == asset_address.lower()) | 
                (DBPoolSwap.asset_out == asset_address.lower())
            ).first()
            
            return {
                'total_swaps': stats.total_swaps or 0,
                'direct_priced_swaps': stats.direct_priced_swaps or 0,
                'global_priced_swaps': stats.global_priced_swaps or 0,
                'usd_details': stats.usd_details or 0,
                'avax_details': stats.avax_details or 0
            }
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting direct pricing stats",
                asset_address=asset_address,
                error=str(e)
            )
            return {'total_swaps': 0, 'direct_priced_swaps': 0, 'global_priced_swaps': 0, 'usd_details': 0, 'avax_details': 0}

    def get_latest_pricing_timestamp(
        self,
        session: Session,
        asset_address: str
    ) -> Optional[str]:
        """Get the latest pricing timestamp for an asset (any denomination)"""
        try:
            latest = session.query(func.max(DBPoolSwap.timestamp)).join(
                DBPoolSwap, DBPoolSwapDetail.content_id == DBPoolSwap.content_id
            ).filter(
                (DBPoolSwap.asset_in == asset_address.lower()) | 
                (DBPoolSwap.asset_out == asset_address.lower())
            ).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting latest pricing timestamp",
                asset_address=asset_address,
                error=str(e)
            )
            return None

    def get_protocol_volume_aggregation(
        self,
        model_session: Session,
        shared_session: Session,
        period_id: int,
        asset_address: str,
        denomination: PricingDenomination
    ) -> Dict[str, Dict]:
        """
        Get protocol-level volume aggregation for asset_volume calculation.
        
        Used by CalculationService.calculate_asset_volume_by_protocol()
        to aggregate swap volume by protocol using DBContract.project.
        """
        try:
            # Join swap details with DBContracts to get protocol info
            results = model_session.query(
                DBContract.project.label('protocol'),
                func.sum(DBPoolSwapDetail.value).label('total_volume'),   # ✅ Fixed: Use actual field name
                func.count(DBPoolSwapDetail.content_id.distinct()).label('swap_count'),
                func.count(DBPoolSwap.pool.distinct()).label('pool_count')
            ).join(
                DBPoolSwap, DBPoolSwapDetail.content_id == DBPoolSwap.content_id
            ).join(
                DBContract, DBPoolSwap.pool == DBContract.address
            ).join(
                Period, 
                and_(
                    DBPoolSwap.timestamp >= Period.time_open,
                    DBPoolSwap.timestamp <= Period.time_close
                )
            ).filter(
                and_(
                    Period.id == period_id,
                    DBPoolSwapDetail.denom == denomination,               # ✅ Fixed: Correct field name
                    (DBPoolSwap.asset_in == asset_address.lower()) | 
                    (DBPoolSwap.asset_out == asset_address.lower()),
                    DBContract.project.isnot(None)  # Only pools with protocol info
                )
            ).group_by(DBContract.project).all()
            
            protocol_volumes = {}
            for protocol, volume, swap_count, pool_count in results:
                protocol_volumes[protocol] = {
                    'volume': float(volume) if volume else 0.0,
                    'swap_count': swap_count or 0,
                    'pool_count': pool_count or 0
                }
            
            return protocol_volumes
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error getting protocol volume aggregation",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return {}