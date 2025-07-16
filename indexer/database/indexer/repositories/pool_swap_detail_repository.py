# indexer/database/indexer/repositories/pool_swap_detail_repository.py

from typing import List, Optional, Dict
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, case, func
from decimal import Decimal

from ...connection import ModelDatabaseManager
from ..tables.detail.pool_swap_detail import PoolSwapDetail, PricingDenomination, PricingMethod
from ....core.logging_config import log_with_context
from ....types.new import DomainEventId
from ...base_repository import BaseRepository
from ..tables.events.trade import PoolSwap
from ...shared.tables.config import Contract
from ...shared.tables.periods import Period
from ...shared.tables.pool_pricing_config import PoolPricingConfig

import logging


class PoolSwapDetailRepository(BaseRepository):
    """Repository for pool swap pricing details"""
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, PoolSwapDetail)
    
    def create_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        value: float,
        price: float,
        price_method: PricingMethod,
        price_config_id: Optional[int] = None
    ) -> PoolSwapDetail:
        """Create a new pool swap detail record"""
        try:
            detail = PoolSwapDetail(
                content_id=content_id,
                denom=denom,
                value=value,
                price=price,
                price_method=price_method,
                price_config_id=price_config_id
                # Note: created_at and updated_at handled automatically by BaseModel
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Pool swap detail created",
                            content_id=content_id,
                            denom=denom.value,
                            value=value,
                            price_method=price_method.value)
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error creating pool swap detail",
                            content_id=content_id,
                            denom=denom.value if denom else None,
                            error=str(e))
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[PoolSwapDetail]:
        """Get all pricing details for a pool swap"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.content_id == content_id
            ).order_by(PoolSwapDetail.denom).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting details by content_id",
                            content_id=content_id,
                            error=str(e))
            raise
    
    def get_by_content_id_and_denom(
        self, 
        session: Session, 
        content_id: DomainEventId, 
        denom: PricingDenomination
    ) -> Optional[PoolSwapDetail]:
        """Get specific denomination detail for a pool swap"""
        try:
            return session.query(PoolSwapDetail).filter(
                and_(
                    PoolSwapDetail.content_id == content_id,
                    PoolSwapDetail.denom == denom
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting detail by content_id and denom",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_by_pricing_method(
        self, 
        session: Session, 
        price_method: PricingMethod, 
        limit: int = 100
    ) -> List[PoolSwapDetail]:
        """Get details by pricing method"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.price_method == price_method
            ).order_by(desc(PoolSwapDetail.created_at)).limit(limit).all()  # FIXED: created_at instead of calculated_at
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting details by pricing method",
                            price_method=price_method.value,
                            error=str(e))
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[PoolSwapDetail]:
        """Get USD valuation details"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.denom == PricingDenomination.USD
            ).order_by(desc(PoolSwapDetail.created_at)).limit(limit).all()  # FIXED: created_at instead of calculated_at
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting USD valuations",
                            error=str(e))
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[PoolSwapDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(PoolSwapDetail).filter(
                PoolSwapDetail.denom == PricingDenomination.AVAX
            ).order_by(desc(PoolSwapDetail.created_at)).limit(limit).all()  # FIXED: created_at instead of calculated_at
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting AVAX valuations",
                            error=str(e))
            raise
    
    def get_missing_valuations(
        self, 
        session: Session, 
        denom: PricingDenomination,
        limit: int = 1000
    ) -> List[DomainEventId]:
        """Get pool swaps missing valuation details for a denomination"""
        try:           
            # Find pool swaps that don't have detail records for this denomination
            subquery = session.query(PoolSwapDetail.content_id).filter(
                PoolSwapDetail.denom == denom
            ).subquery()
            
            missing_swaps = session.query(PoolSwap.content_id).filter(
                ~PoolSwap.content_id.in_(subquery)
            ).order_by(desc(PoolSwap.created_at)).limit(limit).all()  # FIXED: created_at instead of timestamp
            
            return [swap.content_id for swap in missing_swaps]
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting missing valuations",
                            denom=denom.value,
                            error=str(e))
            raise
    
    def update_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denom: PricingDenomination,
        **updates
    ) -> Optional[PoolSwapDetail]:
        """Update existing pool swap detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denom)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(self.logger, logging.DEBUG, "Pool swap detail updated",
                            content_id=content_id,
                            denom=denom.value,
                            updates=list(updates.keys()))
            
            return detail
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error updating pool swap detail",
                            content_id=content_id,
                            denom=denom.value,
                            error=str(e))
            raise
    
    def get_usd_details_for_swaps(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId]
    ) -> List[PoolSwapDetail]:
        """Get USD valuation details for multiple swaps"""
        try:
            return session.query(PoolSwapDetail).filter(
                and_(
                    PoolSwapDetail.content_id.in_(swap_content_ids),
                    PoolSwapDetail.denom == PricingDenomination.USD
                )
            ).order_by(PoolSwapDetail.content_id).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting USD details for swaps",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            raise
    
    def get_avax_details_for_swaps(
        self, 
        session: Session, 
        swap_content_ids: List[DomainEventId]
    ) -> List[PoolSwapDetail]:
        """Get AVAX valuation details for multiple swaps"""
        try:
            return session.query(PoolSwapDetail).filter(
                and_(
                    PoolSwapDetail.content_id.in_(swap_content_ids),
                    PoolSwapDetail.denom == PricingDenomination.AVAX
                )
            ).order_by(PoolSwapDetail.content_id).all()
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting AVAX details for swaps",
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
            direct_pricing_count = session.query(PoolSwapDetail.content_id.distinct()).filter(
                and_(
                    PoolSwapDetail.content_id.in_(swap_content_ids),
                    PoolSwapDetail.price_method.in_([PricingMethod.DIRECT_AVAX, PricingMethod.DIRECT_USD])
                )
            ).count()
            
            # All swaps should have direct pricing
            return direct_pricing_count == len(swap_content_ids)
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error checking direct pricing eligibility",
                            swap_count=len(swap_content_ids),
                            error=str(e))
            return False
    
    def get_pricing_method_stats(self, session: Session) -> Dict[str, int]:
        """Get statistics about pricing methods used"""
        try:           
            stats = session.query(
                PoolSwapDetail.price_method,
                func.count(PoolSwapDetail.content_id.distinct()).label('swap_count')
            ).group_by(PoolSwapDetail.price_method).all()
            
            return {method.value: count for method, count in stats}
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Error getting pricing method stats",
                            error=str(e))
            return {}
        
    def get_pricing_pool_swaps_in_timeframe(
        self,
        session: Session,
        asset_address: str,
        start_time: datetime,
        end_time: datetime,
        denomination: PricingDenomination
    ) -> List[PoolSwapDetail]:
        """
        Get pool swap details from pricing pools within a timeframe.
        
        Used by PricingService.generate_canonical_prices() to get VWAP data
        from designated pricing pools for canonical price calculation.
        """
        try:
            # Join with pool pricing config to find pricing pools
            # Note: This assumes pool_pricing_config has a pricing_pool boolean field
            return session.query(PoolSwapDetail).join(
                PoolSwap, PoolSwapDetail.content_id == PoolSwap.content_id
            ).join(
                Contract, PoolSwap.pool == Contract.address
            ).join(
                PoolPricingConfig, 
                and_(
                    PoolPricingConfig.contract_id == Contract.id,
                    PoolPricingConfig.pricing_pool == True  # Only pricing pools
                )
            ).filter(
                and_(
                    PoolSwap.asset_in == asset_address.lower(),  # Asset being priced
                    PoolSwapDetail.denomination == denomination,
                    PoolSwap.timestamp >= start_time,
                    PoolSwap.timestamp <= end_time,
                    PoolSwapDetail.price_usd.isnot(None) if denomination == PricingDenomination.USD else PoolSwapDetail.price_avax.isnot(None),
                    PoolSwapDetail.volume_usd.isnot(None) if denomination == PricingDenomination.USD else PoolSwapDetail.volume_avax.isnot(None)
                )
            ).order_by(PoolSwap.timestamp).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting pricing pool swaps in timeframe",
                asset_address=asset_address,
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                denomination=denomination.value,
                error=str(e)
            )
            return []

    def create_global_pricing_detail(
        self,
        session: Session,
        swap,  # PoolSwap object
        denomination: PricingDenomination,
        canonical_price: Decimal,
        pricing_method: PricingMethod
    ) -> Optional[PoolSwapDetail]:
        """
        Create a pool swap detail record using canonical pricing.
        
        Used by PricingService.apply_canonical_pricing_to_global_events()
        to price swaps that couldn't be directly priced.
        """
        try:
            # Calculate volume using canonical price and swap amounts
            # This assumes the swap has amount_in/amount_out fields
            if denomination == PricingDenomination.USD:
                # For USD denomination, calculate volume using canonical USD price
                volume_usd = swap.amount_out * canonical_price  # Assumes amount_out is in human-readable format
                price_usd = canonical_price
                volume_avax = None
                price_avax = None
            else:
                # For AVAX denomination, calculate volume using canonical AVAX price  
                volume_avax = swap.amount_out * canonical_price
                price_avax = canonical_price
                volume_usd = None
                price_usd = None
            
            detail = PoolSwapDetail(
                content_id=swap.content_id,
                denomination=denomination,
                volume_usd=float(volume_usd) if volume_usd else None,
                volume_avax=float(volume_avax) if volume_avax else None,
                price_usd=float(price_usd) if price_usd else None,
                price_avax=float(price_avax) if price_avax else None,
                pricing_method=pricing_method,
                pool_address=swap.pool
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Global pricing detail created",
                content_id=swap.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                pricing_method=pricing_method.value,
                volume=float(volume_usd) if volume_usd else float(volume_avax)
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating global pricing detail",
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
            # Get stats for USD denomination
            usd_stats = session.query(
                func.count(PoolSwapDetail.content_id).label('detail_count'),
                func.count(case([(PoolSwapDetail.pricing_method == PricingMethod.DIRECT, 1)])).label('direct_count'),
                func.count(case([(PoolSwapDetail.pricing_method == PricingMethod.GLOBAL, 1)])).label('global_count'),
                func.sum(PoolSwapDetail.volume_usd).label('total_volume'),
                func.avg(PoolSwapDetail.price_usd).label('avg_price')
            ).join(
                PoolSwap, PoolSwapDetail.content_id == PoolSwap.content_id
            ).filter(
                and_(
                    PoolSwap.asset_in == asset_address.lower(),
                    PoolSwapDetail.denomination == PricingDenomination.USD
                )
            ).first()
            
            # Get stats for AVAX denomination
            avax_stats = session.query(
                func.count(PoolSwapDetail.content_id).label('detail_count'),
                func.count(case([(PoolSwapDetail.pricing_method == PricingMethod.DIRECT, 1)])).label('direct_count'),
                func.count(case([(PoolSwapDetail.pricing_method == PricingMethod.GLOBAL, 1)])).label('global_count'),
                func.sum(PoolSwapDetail.volume_avax).label('total_volume'),
                func.avg(PoolSwapDetail.price_avax).label('avg_price')
            ).join(
                PoolSwap, PoolSwapDetail.content_id == PoolSwap.content_id
            ).filter(
                and_(
                    PoolSwap.asset_in == asset_address.lower(),
                    PoolSwapDetail.denomination == PricingDenomination.AVAX
                )
            ).first()
            
            return {
                'usd': {
                    'detail_count': usd_stats.detail_count or 0,
                    'direct_count': usd_stats.direct_count or 0,
                    'global_count': usd_stats.global_count or 0,
                    'total_volume': float(usd_stats.total_volume) if usd_stats.total_volume else 0.0,
                    'avg_price': float(usd_stats.avg_price) if usd_stats.avg_price else 0.0
                },
                'avax': {
                    'detail_count': avax_stats.detail_count or 0,
                    'direct_count': avax_stats.direct_count or 0,
                    'global_count': avax_stats.global_count or 0,
                    'total_volume': float(avax_stats.total_volume) if avax_stats.total_volume else 0.0,
                    'avg_price': float(avax_stats.avg_price) if avax_stats.avg_price else 0.0
                }
            }
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting direct pricing stats",
                asset_address=asset_address,
                error=str(e)
            )
            return {'usd': {}, 'avax': {}}

    def get_latest_pricing_timestamp(
        self,
        session: Session,
        asset_address: str
    ) -> Optional[str]:
        """Get the latest pricing timestamp for an asset (any denomination)"""
        try:
            latest = session.query(func.max(PoolSwap.timestamp)).join(
                PoolSwap, PoolSwapDetail.content_id == PoolSwap.content_id
            ).filter(
                PoolSwap.asset_in == asset_address.lower()
            ).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest pricing timestamp",
                asset_address=asset_address,
                error=str(e)
            )
            return None

    def get_protocol_volume_aggregation(
        self,
        indexer_session: Session,
        shared_session: Session,
        period_id: int,
        asset_address: str,
        denomination: PricingDenomination
    ) -> Dict[str, Dict]:
        """
        Get protocol-level volume aggregation for asset_volume calculation.
        
        Used by CalculationService.calculate_asset_volume_by_protocol()
        to aggregate swap volume by protocol using contract.project.
        """
        try:
            # Get the period timeframe
            period = shared_session.query(Period).filter(Period.id == period_id).first()
            if not period:
                return {}
            
            # Calculate period start/end times (assuming period represents a time range)
            period_start = period.timestamp
            # For simplicity, assuming 5-minute periods - adjust based on your period structure
            period_end = period_start + timedelta(minutes=5)
            
            # Aggregate volume by protocol
            volume_by_protocol = indexer_session.query(
                Contract.project.label('protocol'),
                func.sum(
                    PoolSwapDetail.volume_usd if denomination == PricingDenomination.USD 
                    else PoolSwapDetail.volume_avax
                ).label('total_volume'),
                func.count(func.distinct(PoolSwap.pool)).label('pool_count'),
                func.count(PoolSwapDetail.content_id).label('swap_count')
            ).join(
                PoolSwap, PoolSwapDetail.content_id == PoolSwap.content_id
            ).join(
                Contract, PoolSwap.pool == Contract.address
            ).filter(
                and_(
                    PoolSwap.asset_in == asset_address.lower(),
                    PoolSwapDetail.denomination == denomination,
                    PoolSwap.timestamp >= period_start,
                    PoolSwap.timestamp < period_end,
                    Contract.project.isnot(None)  # Only contracts with project assigned
                )
            ).group_by(Contract.project).all()
            
            # Convert to dictionary format
            result = {}
            for row in volume_by_protocol:
                result[row.protocol] = {
                    'total_volume': Decimal(str(row.total_volume)) if row.total_volume else Decimal('0'),
                    'pool_count': row.pool_count or 0,
                    'swap_count': row.swap_count or 0
                }
            
            log_with_context(
                self.logger, logging.DEBUG, "Protocol volume aggregation calculated",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                protocols_found=len(result)
            )
            
            return result
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting protocol volume aggregation",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return {}