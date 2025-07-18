# indexer/database/indexer/repositories/trade_detail_repository.py

from typing import List, Optional, Dict
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func, case, exists

from ...connection import ModelDatabaseManager
from ..tables.detail.trade_detail import TradeDetail, TradePricingMethod
from ..tables.detail.pool_swap_detail import PricingDenomination, PoolSwapDetail
from ..tables.events.trade import Trade, PoolSwap
from ....core.logging import IndexerLogger, log_with_context
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
        denom: PricingDenomination,
        value: Decimal,
        price: Decimal,
        price_method: TradePricingMethod
    ) -> Optional[TradeDetail]:
        """Create a new trade detail record"""
        try:
            detail = TradeDetail(
                content_id=content_id,           # ✅ Correct field name
                denom=denom,                     # ✅ Fixed: Correct field name
                value=float(value),              # ✅ Fixed: Use actual table field
                price=float(price),              # ✅ Fixed: Use actual table field
                price_method=price_method        # ✅ Fixed: Correct field name
                # ✅ Removed: swap_count doesn't exist in table
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Trade detail created",
                content_id=content_id,
                denom=denom.value,
                value=float(value),
                price=float(price),
                price_method=price_method.value
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error creating trade detail",
                content_id=content_id,
                denom=denom.value,
                error=str(e)
            )
            raise
    
    def get_by_content_id(self, session: Session, content_id: DomainEventId) -> List[TradeDetail]:
        """Get all pricing details for a trade"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.content_id == content_id
            ).order_by(TradeDetail.denom).all()                    # ✅ Fixed: Correct field name
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
        denom: PricingDenomination
    ) -> Optional[TradeDetail]:
        """Get specific denomination detail for a trade"""
        try:
            return session.query(TradeDetail).filter(
                and_(
                    TradeDetail.content_id == content_id,
                    TradeDetail.denom == denom                      # ✅ Fixed: Correct field name
                )
            ).one_or_none()
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting detail by content_id and denomination",
                content_id=content_id,
                denom=denom.value,
                error=str(e)
            )
            raise
    
    def get_by_pricing_method(
        self, 
        session: Session, 
        price_method: TradePricingMethod, 
        limit: int = 100
    ) -> List[TradeDetail]:
        """Get details by pricing method"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.price_method == price_method           # ✅ Fixed: Correct field name
            ).order_by(desc(TradeDetail.created_at)).limit(limit).all()
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting details by pricing method",
                price_method=price_method.value,
                error=str(e)
            )
            raise
    
    def get_usd_valuations(self, session: Session, limit: int = 100) -> List[TradeDetail]:
        """Get USD valuation details"""
        try:
            return session.query(TradeDetail).filter(
                TradeDetail.denom == PricingDenomination.USD       # ✅ Fixed: Correct field name
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
                TradeDetail.denom == PricingDenomination.AVAX      # ✅ Fixed: Correct field name
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
        denom: PricingDenomination,
        **updates
    ) -> Optional[TradeDetail]:
        """Update existing trade detail"""
        try:
            detail = self.get_by_content_id_and_denom(session, content_id, denom)
            if not detail:
                return None
            
            for key, value in updates.items():
                if hasattr(detail, key):
                    setattr(detail, key, value)
            
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Trade detail updated",
                content_id=content_id,
                denom=denom.value,
                updates=list(updates.keys())
            )
            
            return detail
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error updating trade detail",
                content_id=content_id,
                denom=denom.value,
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
                    denom=data['denomination'],              # ✅ Fixed: Correct field name
                    value=float(data['value']),              # ✅ Fixed: Use actual table field
                    price=float(data['price']),              # ✅ Fixed: Use actual table field
                    price_method=data.get('price_method', TradePricingMethod.DIRECT)  # ✅ Fixed: Correct field name
                    # ✅ Removed: swap_count doesn't exist in table
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
    # CANONICAL PRICING METHODS
    # =====================================================================

    def create_global_pricing_detail(
        self,
        session: Session,
        trade,  # Trade object
        denomination: PricingDenomination,
        canonical_price: Decimal,
        pricing_method: TradePricingMethod
    ) -> Optional[TradeDetail]:
        """
        Create a trade detail record using canonical pricing.
        
        Used by PricingService.apply_canonical_pricing_to_global_events()
        to price trades that couldn't be directly priced.
        """
        try:
            # Calculate value using canonical price and trade base amount
            # Assumes trade.base_amount is in human-readable format
            trade_value = float(trade.base_amount) * float(canonical_price)
            
            detail = TradeDetail(
                content_id=trade.content_id,     # ✅ Correct field name
                denom=denomination,              # ✅ Fixed: Correct field name
                value=trade_value,               # ✅ Fixed: Use actual table field
                price=float(canonical_price),    # ✅ Fixed: Use actual table field
                price_method=pricing_method      # ✅ Fixed: Correct field name
            )
            
            session.add(detail)
            session.flush()
            
            log_with_context(
                self.logger, logging.DEBUG, "Global trade pricing detail created",
                content_id=trade.content_id,
                denomination=denomination.value,
                canonical_price=float(canonical_price),
                pricing_method=pricing_method.value,
                value=trade_value
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
            return session.query(TradeDetail).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).join(
                Period, 
                and_(
                    Trade.timestamp >= Period.time_open,
                    Trade.timestamp <= Period.time_close
                )
            ).filter(
                and_(
                    Period.id == period_id,
                    Trade.base_token == asset_address.lower(),
                    TradeDetail.denom == denomination           # ✅ Fixed: Correct field name
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

    def calculate_direct_pricing(
        self,
        session: Session,
        asset_address: str,
        days_back: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Calculate direct pricing for trades using volume-weighted aggregation from constituent swaps.
        
        Enhanced version of existing method to support the new pricing service.
        This method should aggregate pricing from constituent pool swap details.
        """
        try:
            # Build base query for trades without direct pricing
            query = session.query(Trade).filter(
                Trade.base_token == asset_address.lower()  # Use base_token instead of asset_in
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
                        TradeDetail.price_method == TradePricingMethod.DIRECT  # ✅ Fixed: Correct field name
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
                        denom_details = [d for d in swap_details if d.denom == denomination]  # ✅ Fixed: Correct field name
                        
                        if not denom_details:
                            continue
                        
                        # Calculate volume-weighted average price
                        total_value = sum(float(detail.value) for detail in denom_details)  # ✅ Fixed: Use actual field
                        total_volume = sum(float(detail.value) / float(detail.price) for detail in denom_details if detail.price > 0)  # ✅ Fixed: Use actual fields
                        
                        if total_volume > 0:
                            volume_weighted_price = total_value / total_volume
                            
                            # Create trade detail record
                            self.create_detail(
                                session=session,
                                content_id=trade.content_id,
                                denom=denomination,
                                value=Decimal(str(total_value)),
                                price=Decimal(str(volume_weighted_price)),
                                price_method=TradePricingMethod.DIRECT
                            )
                            
                            results['trades_priced'] += 1
                    
                except Exception as e:
                    results['errors'] += 1
                    log_with_context(
                        self.logger, logging.ERROR, "Error pricing individual trade",
                        trade_id=trade.content_id,
                        error=str(e)
                    )
                    continue
            
            return results
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error calculating direct pricing",
                asset_address=asset_address,
                error=str(e)
            )
            return {'trades_priced': 0, 'errors': 1}

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
                func.count(TradeDetail.content_id).label('detail_count'),
                func.count(case([(TradeDetail.price_method == TradePricingMethod.DIRECT, 1)])).label('direct_count'),  # ✅ Fixed: Correct field name
                func.count(case([(TradeDetail.price_method == TradePricingMethod.GLOBAL, 1)])).label('global_count'),  # ✅ Fixed: Correct field name
                func.sum(TradeDetail.value).label('total_volume'),      # ✅ Fixed: Use actual field
                func.avg(TradeDetail.price).label('avg_price')          # ✅ Fixed: Use actual field
            ).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).filter(
                and_(
                    Trade.base_token == asset_address.lower(),
                    TradeDetail.denom == PricingDenomination.USD        # ✅ Fixed: Correct field name
                )
            ).first()
            
            # Get stats for AVAX denomination
            avax_stats = session.query(
                func.count(TradeDetail.content_id).label('detail_count'),
                func.count(case([(TradeDetail.price_method == TradePricingMethod.DIRECT, 1)])).label('direct_count'),  # ✅ Fixed: Correct field name
                func.count(case([(TradeDetail.price_method == TradePricingMethod.GLOBAL, 1)])).label('global_count'),  # ✅ Fixed: Correct field name
                func.sum(TradeDetail.value).label('total_volume'),      # ✅ Fixed: Use actual field
                func.avg(TradeDetail.price).label('avg_price')          # ✅ Fixed: Use actual field
            ).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).filter(
                and_(
                    Trade.base_token == asset_address.lower(),
                    TradeDetail.denom == PricingDenomination.AVAX       # ✅ Fixed: Correct field name
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
            return {
                'usd': {'detail_count': 0, 'direct_count': 0, 'global_count': 0, 'total_volume': 0.0, 'avg_price': 0.0},
                'avax': {'detail_count': 0, 'direct_count': 0, 'global_count': 0, 'total_volume': 0.0, 'avg_price': 0.0}
            }

    def get_trades_without_direct_pricing(
        self,
        session: Session,
        block_number: int,
        asset_address: str
    ) -> List[Trade]:
        """
        Get trades in a block that don't have direct pricing.
        
        Used by PricingService.apply_canonical_pricing_to_global_events()
        to find trades that need global pricing.
        """
        try:
            return session.query(Trade).filter(
                and_(
                    Trade.block_number == block_number,
                    Trade.base_token == asset_address.lower(),
                    ~exists().where(
                        and_(
                            TradeDetail.content_id == Trade.content_id,
                            TradeDetail.price_method == TradePricingMethod.DIRECT  # ✅ Fixed: Correct field name
                        )
                    )
                )
            ).all()
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting trades without direct pricing",
                block_number=block_number,
                asset_address=asset_address,
                error=str(e)
            )
            return []

    def get_latest_pricing_timestamp(
        self,
        session: Session,
        asset_address: str
    ) -> Optional[str]:
        """Get the latest pricing timestamp for an asset (any denomination)"""
        try:
            latest = session.query(func.max(Trade.timestamp)).join(
                Trade, TradeDetail.content_id == Trade.content_id
            ).filter(
                Trade.base_token == asset_address.lower()
            ).scalar()
            
            return latest.isoformat() if latest else None
            
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting latest pricing timestamp",
                asset_address=asset_address,
                error=str(e)
            )
            return None