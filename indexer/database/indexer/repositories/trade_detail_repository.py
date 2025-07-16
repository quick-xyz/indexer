# indexer/database/indexer/repositories/trade_detail_repository.py

from typing import List, Optional, Dict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func, case, exists

from ...connection import ModelDatabaseManager
from ..tables.detail.trade_detail import TradeDetail
from ..tables.detail.pool_swap_detail import PricingDenomination, PricingMethod, PoolSwapDetail
from ..tables.events.trade import Trade, PoolSwap
from ....core.logging_config import IndexerLogger, log_with_context
from ....types.new import DomainEventId
from ...base_repository import BaseRepository
from ...shared.tables.periods import Period

import logging


class TradeDetailRepository(BaseRepository):
    """
    Repository for trade pricing and valuation details.
    
    Manages pricing information for trades aggregated from constituent pool swaps.
    Supports both direct pricing (volume-weighted from swaps) and global pricing 
    (using canonical prices) with dual denomination support (USD/AVAX).
    
    Located in indexer database since trade details are model-specific
    and derived from model-specific event data.
    """
    
    def __init__(self, db_manager: ModelDatabaseManager):
        super().__init__(db_manager, TradeDetail)
        self.logger = IndexerLogger.get_logger('database.repositories.trade_detail')
    
    def create_detail(
        self,
        session: Session,
        content_id: DomainEventId,
        denomination: PricingDenomination,
        volume: Optional[Decimal] = None,
        price: Optional[Decimal] = None,
        pricing_method: PricingMethod = PricingMethod.DIRECT,
        swap_count: Optional[int] = None
    ) -> Optional[TradeDetail]:
        """Create a new trade detail record"""
        try:
            detail = TradeDetail(
                content_id=content_id,
                denomination=denomination,
                volume_usd=float(volume) if denomination == PricingDenomination.USD and volume else None,
                volume_avax=float(volume) if denomination == PricingDenomination.AVAX and volume else None,
                price_usd=float(price) if denomination == PricingDenomination.USD and price else None,
                price_avax=float(price) if denomination == PricingDenomination.AVAX and price else None,
                pricing_method=pricing_method,
                swap_count=swap_count
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Trade detail created",
                content_id=content_id,
                denomination=denomination.value,
                volume=float(volume) if volume else None,
                price=float(price) if price else None,
                pricing_method=pricing_method.value
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating trade detail",
                content_id=content_id,
                denomination=denomination.value,
                error=str(e)
            )
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[TradeDetail]:
        """Get all pricing details for a trade"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.content_id == content_id
            ).order_by(TradeDetail.denomination).all()
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting details by content_id",
                content_id=content_id,
                error=str(e)
            )
            raise
    
    def get_by_content_id_and_denom(
        self, 
        session: Session, 
        content_id: DomainEventId, 
        denomination: PricingDenomination
    ) -> Optional[TradeDetail]:
        """Get specific denomination detail for a trade"""
        try:
            return session.query(TradeDetail).filter(
                and_(
                    TradeDetail.content_id == content_id,
                    TradeDetail.denomination == denomination
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting detail by content_id and denomination",
                content_id=content_id,
                denomination=denomination.value,
                error=str(e)
            )
            raise
    
    def get_by_pricing_method(
        self, 
        session: Session, 
        pricing_method: PricingMethod, 
        limit: int = 100
    ) -> List[TradeDetail]:
        """Get details by pricing method"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.pricing_method == pricing_method
            ).order_by(desc(TradeDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting details by pricing method",
                pricing_method=pricing_method.value,
                error=str(e)
            )
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[TradeDetail]:
        """Get USD valuation details"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.denomination == PricingDenomination.USD
            ).order_by(desc(TradeDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting USD valuations",
                error=str(e)
            )
            raise
    
    def get_avax_valuations(self, session: Session, limit: int = 100) -> List[TradeDetail]:
        """Get AVAX valuation details"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.denomination == PricingDenomination.AVAX
            ).order_by(desc(TradeDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting AVAX valuations",
                error=str(e)
            )
            raise
    
    def update_detail(
        self, 
        session: Session, 
        content_id: DomainEventId,
        denomination: PricingDenomination,
        **updates
    ) -> Optional[TradeDetail]:
        """Update existing trade detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denomination)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Trade detail updated",
                content_id=content_id,
                denomination=denomination.value,
                updates=list(updates.keys())
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error updating trade detail",
                content_id=content_id,
                denomination=denomination.value,
                error=str(e)
            )
            raise
    
    def bulk_create_details(
        self,
        session: Session,
        details_data: List[Dict]
    ) -> int:
        """Bulk create multiple trade detail records"""
        try:
            details = []
            for data in details_data:
                detail = TradeDetail(
                    content_id=data['content_id'],
                    denomination=data['denomination'],
                    volume_usd=data.get('volume_usd'),
                    volume_avax=data.get('volume_avax'),
                    price_usd=data.get('price_usd'),
                    price_avax=data.get('price_avax'),
                    pricing_method=data.get('pricing_method', PricingMethod.DIRECT),
                    swap_count=data.get('swap_count')
                )
                details.append(detail)
            
            session.add_all(details)
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Bulk trade details created",
                detail_count=len(details)
            )
            
            return len(details)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error bulk creating trade details",
                detail_count=len(details_data),
                error=str(e)
            )
            raise

    # =====================================================================
    # NEW CANONICAL PRICING METHODS
    # =====================================================================

    def create_global_pricing_detail(
        self,
        session: Session,
        trade,  # Trade object
        denomination: PricingDenomination,
        canonical_price: Decimal,
        pricing_method: PricingMethod
    ) -> Optional[TradeDetail]:
        """
        Create a trade detail record using canonical pricing.
        
        Used by PricingService.apply_canonical_pricing_to_global_events()
        to price trades that couldn't be directly priced.
        """
        try:
            # Calculate volume using canonical price and trade amounts
            # This assumes the trade has total_amount_out field
            if denomination == PricingDenomination.USD:
                # For USD denomination, calculate volume using canonical USD price
                volume_usd = trade.total_amount_out * canonical_price
                price_usd = canonical_price
                volume_avax = None
                price_avax = None
            else:
                # For AVAX denomination, calculate volume using canonical AVAX price
                volume_avax = trade.total_amount_out * canonical_price
                price_avax = canonical_price
                volume_usd = None
                price_usd = None
            
            detail = TradeDetail(
                content_id=trade.content_id,
                denomination=denomination,
                volume_usd=float(volume_usd) if volume_usd else None,
                volume_avax=float(volume_avax) if volume_avax else None,
                price_usd=float(price_usd) if price_usd else None,
                price_avax=float(price_avax) if price_avax else None,
                pricing_method=pricing_method,
                swap_count=trade.swap_count if hasattr(trade, 'swap_count') else 1
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Global trade pricing detail created",
                content_id=trade.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                pricing_method=pricing_method.value,
                volume=float(volume_usd) if volume_usd else float(volume_avax)
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating global trade pricing detail",
                content_id=trade.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                error=str(e)
            )
            raise

    def get_trades_in_period(
        self,
        session: Session,
        period_id: int,
        asset_address: str,
        denomination: PricingDenomination
    ) -> List[TradeDetail]:
        """
        Get trade details for a specific period and asset.
        
        Used by CalculationService.generate_asset_ohlc_candles()
        to get trade data for OHLC calculation.
        """
        try:            
            # Get the period timeframe
            with self.db_manager.get_shared_session() as shared_session:
                period = shared_session.query(Period).filter(Period.id == period_id).first()
                if not period:
                    return []
                
                # Calculate period start/end times
                period_start = period.timestamp
                # Assuming 5-minute periods - adjust based on your period structure
                period_end = period_start + timedelta(minutes=5)
            
            return session.query(TradeDetail).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).filter(
                and_(
                    Trade.asset_in == asset_address.lower(),
                    TradeDetail.denomination == denomination,
                    Trade.timestamp >= period_start,
                    Trade.timestamp < period_end,
                    TradeDetail.price_usd.isnot(None) if denomination == PricingDenomination.USD else TradeDetail.price_avax.isnot(None),
                    TradeDetail.volume_usd.isnot(None) if denomination == PricingDenomination.USD else TradeDetail.volume_avax.isnot(None)
                )
            ).order_by(Trade.timestamp).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting trades in period",
                period_id=period_id,
                asset_address=asset_address,
                denomination=denomination.value,
                error=str(e)
            )
            return []

    def get_direct_pricing_stats(
        self,
        session: Session,
        asset_address: str
    ) -> Dict:
        """
        Get statistics about direct pricing coverage for trades.
        
        Used by PricingService.get_pricing_status() for monitoring.
        """
        try:
            # Get stats for USD denomination
            usd_stats = session.query(
                func.count(TradeDetail.content_id).label('detail_count'),
                func.count(case([(TradeDetail.pricing_method == PricingMethod.DIRECT, 1)])).label('direct_count'),
                func.count(case([(TradeDetail.pricing_method == PricingMethod.GLOBAL, 1)])).label('global_count'),
                func.sum(TradeDetail.volume_usd).label('total_volume'),
                func.avg(TradeDetail.price_usd).label('avg_price')
            ).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).filter(
                and_(
                    Trade.asset_in == asset_address.lower(),
                    TradeDetail.denomination == PricingDenomination.USD
                )
            ).first()
            
            # Get stats for AVAX denomination
            avax_stats = session.query(
                func.count(TradeDetail.content_id).label('detail_count'),
                func.count(case([(TradeDetail.pricing_method == PricingMethod.DIRECT, 1)])).label('direct_count'),
                func.count(case([(TradeDetail.pricing_method == PricingMethod.GLOBAL, 1)])).label('global_count'),
                func.sum(TradeDetail.volume_avax).label('total_volume'),
                func.avg(TradeDetail.price_avax).label('avg_price')
            ).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).filter(
                and_(
                    Trade.asset_in == asset_address.lower(),
                    TradeDetail.denomination == PricingDenomination.AVAX
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
                self.logger, logging.ERROR, "Error getting direct trade pricing stats",
                asset_address=asset_address,
                error=str(e)
            )
            return {'usd': {}, 'avax': {}}

    def get_latest_pricing_timestamp(
        self,
        session: Session,
        asset_address: str
    ) -> Optional[str]:
        """Get the latest pricing timestamp for trades (any denomination)"""
        try:
            latest = session.query(func.max(Trade.timestamp)).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).filter(
                Trade.asset_in == asset_address.lower()
            ).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest trade pricing timestamp",
                asset_address=asset_address,
                error=str(e)
            )
            return None

    def calculate_direct_pricing(
        self,
        session: Session,
        asset_address: str,
        days_back: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Calculate direct pricing for trades using volume-weighted aggregation.
        
        Enhanced version of existing method to support the new pricing service.
        This method should aggregate pricing from constituent pool swap details.
        """
        try:
            # Build base query for trades without direct pricing
            query = session.query(Trade).filter(
                Trade.asset_in == asset_address.lower()
            )
            
            # Add time filter if specified
            if days_back:
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_back)
                query = query.filter(Trade.timestamp >= cutoff_time)
            
            # Find trades without direct pricing (no TradeDetail with DIRECT method)
            trades_without_direct_pricing = query.filter(
                ~exists().where(
                    and_(
                        TradeDetail.content_id == Trade.content_id,
                        TradeDetail.pricing_method == PricingMethod.DIRECT
                    )
                )
            ).all()
            
            results = {'trades_priced': 0, 'errors': 0}
            
            for trade in trades_without_direct_pricing:
                try:
                    # Get constituent pool swap details for this trade
                    swap_details = session.query(PoolSwapDetail).join(
                        PoolSwap, PoolSwapDetail.content_id == PoolSwap.content_id
                    ).filter(
                        PoolSwap.trade_id == trade.content_id
                    ).all()
                    
                    if not swap_details:
                        continue
                    
                    # Aggregate pricing by denomination
                    for denomination in [PricingDenomination.USD, PricingDenomination.AVAX]:
                        denom_details = [d for d in swap_details if d.denomination == denomination]
                        
                        if not denom_details:
                            continue
                        
                        # Calculate volume-weighted average price
                        total_volume = sum(d.volume_usd if denomination == PricingDenomination.USD else d.volume_avax 
                                         for d in denom_details if d.volume_usd or d.volume_avax)
                        
                        if total_volume == 0:
                            continue
                        
                        weighted_price_sum = sum(
                            (d.volume_usd if denomination == PricingDenomination.USD else d.volume_avax) * 
                            (d.price_usd if denomination == PricingDenomination.USD else d.price_avax)
                            for d in denom_details 
                            if (d.volume_usd or d.volume_avax) and (d.price_usd or d.price_avax)
                        )
                        
                        vwap_price = weighted_price_sum / total_volume
                        
                        # Create or update trade detail with direct pricing
                        existing_detail = self.get_by_content_id_and_denom(
                            session, trade.content_id, denomination
                        )
                        
                        if existing_detail:
                            # Update existing
                            if denomination == PricingDenomination.USD:
                                existing_detail.volume_usd = float(total_volume)
                                existing_detail.price_usd = float(vwap_price)
                            else:
                                existing_detail.volume_avax = float(total_volume)
                                existing_detail.price_avax = float(vwap_price)
                            existing_detail.pricing_method = PricingMethod.DIRECT
                        else:
                            # Create new
                            detail = TradeDetail(
                                content_id=trade.content_id,
                                denomination=denomination,
                                volume_usd=float(total_volume) if denomination == PricingDenomination.USD else None,
                                volume_avax=float(total_volume) if denomination == PricingDenomination.AVAX else None,
                                price_usd=float(vwap_price) if denomination == PricingDenomination.USD else None,
                                price_avax=float(vwap_price) if denomination == PricingDenomination.AVAX else None,
                                pricing_method=PricingMethod.DIRECT,
                                swap_count=len(denom_details)
                            )
                            session.add(detail)
                    
                    results['trades_priced'] += 1
                    
                except Exception as e:
                    results['errors'] += 1
                    log_with_context(
                        self.logger, logging.ERROR, "Error calculating direct pricing for trade",
                        content_id=trade.content_id,
                        error=str(e)
                    )
                    continue
            
            session.flush()
            
            log_with_context(
                self.logger, logging.INFO, "Direct trade pricing calculation complete",
                asset_address=asset_address,
                **results
            )
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error calculating direct trade pricing",
                asset_address=asset_address,
                error=str(e)
            )
            return {'trades_priced': 0, 'errors': 1}