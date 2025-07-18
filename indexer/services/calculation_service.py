# indexer/services/calculation_service.py

from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from ..core.logging import IndexerLogger, log_with_context
from ..database.repository_manager import RepositoryManager
from ..database.connection import DatabaseManager
from ..database.shared.tables.periods import Period, PeriodType
from ..database.indexer.tables.detail.pool_swap_detail import PricingDenomination, PricingMethod

import logging


class CalculationService:
    """
    Calculation service responsible for event valuations and analytics aggregation.
    
    This service handles derived data calculations independent from pricing authority:
    - Event valuations using canonical prices from shared database
    - OHLC candle generation from trade aggregation
    - Protocol-level volume metrics using contract.project
    - Future materialized view management for balances/valuations
    
    Uses dual database architecture:
    - Reads canonical prices from shared database (price_vwap)
    - Reads events from indexer database (trades, transfers, etc.)
    - Writes analytics to indexer database (asset_price, asset_volume, event_details)
    
    Designed for independent operation with graceful delay handling - processes
    whatever data is available without blocking on pricing service completion.
    """
    
    def __init__(
        self,
        shared_db_manager: DatabaseManager,   # Shared database for canonical prices
        model_db_manager: DatabaseManager,  # Indexer database for events and analytics
        repository_manager: RepositoryManager
    ):
        self.shared_db_manager = shared_db_manager    # For canonical price reads
        self.model_db_manager = model_db_manager  # For event reads and analytics writes
        self.repository_manager = repository_manager
        
        self.logger = IndexerLogger.get_logger('services.calculation_service')
        
        log_with_context(
            self.logger, logging.INFO, "CalculationService initialized",
            shared_database=shared_db_manager.config.url.split('/')[-1],
            model_database=model_db_manager.config.url.split('/')[-1]
        )

    def calculate_event_valuations(
        self, 
        period_ids: List[int], 
        asset_address: str,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Apply canonical pricing to events for valuation (transfers, liquidity, rewards, positions).
        
        Creates event_details records with USD/AVAX valuations using canonical prices.
        Independent from pricing service - processes available canonical prices gracefully.
        
        Args:
            period_ids: List of period IDs to process events for
            asset_address: Asset to calculate event valuations for
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with statistics: {'transfers_valued': 0, 'liquidity_valued': 0, 'rewards_valued': 0, 'positions_valued': 0, 'errors': 0}
        """
        log_with_context(
            self.logger, logging.INFO, "Calculating event valuations",
            asset_address=asset_address,
            periods_count=len(period_ids),
            denomination=denomination.value if denomination else "both"
        )
        
        results = {
            'transfers_valued': 0,
            'liquidity_valued': 0, 
            'rewards_valued': 0,
            'positions_valued': 0,
            'errors': 0
        }
        
        denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
        
        # Get repositories
        price_vwap_repo = self.repository_manager.get_price_vwap_repository()
        event_detail_repo = self.repository_manager.get_event_detail_repository()
        transfer_repo = self.repository_manager.get_transfer_repository()
        liquidity_repo = self.repository_manager.get_liquidity_repository()
        reward_repo = self.repository_manager.get_reward_repository()
        position_repo = self.repository_manager.get_position_repository()
        
        with self.shared_db_manager.get_session() as shared_session, \
             self.model_db_manager.get_session() as model_session:
            
            for period_id in period_ids:
                try:
                    # 1. Process transfers
                    unvalued_transfers = transfer_repo.get_unvalued_events_in_period(
                        model_session, period_id, asset_address
                    )
                    
                    for transfer in unvalued_transfers:
                        transfer_minute = int(transfer.timestamp.timestamp() // 60 * 60)
                        
                        for denom in denominations:
                            # Skip if already valued
                            if event_detail_repo.has_valuation(model_session, transfer.content_id, denom):
                                continue
                                
                            canonical_price = price_vwap_repo.get_canonical_price(
                                shared_session, asset_address, transfer_minute, denom
                            )
                            
                            if canonical_price:
                                event_detail_repo.create_event_valuation(
                                    model_session,
                                    event=transfer,
                                    denomination=denom,
                                    canonical_price=canonical_price.price,
                                    pricing_method=PricingMethod.CANONICAL
                                )
                                results['transfers_valued'] += 1
                    
                    # 2. Process liquidity events
                    unvalued_liquidity = liquidity_repo.get_unvalued_events_in_period(
                        model_session, period_id, asset_address
                    )
                    
                    for liquidity_event in unvalued_liquidity:
                        liquidity_minute = int(liquidity_event.timestamp.timestamp() // 60 * 60)
                        
                        for denom in denominations:
                            if event_detail_repo.has_valuation(model_session, liquidity_event.content_id, denom):
                                continue
                                
                            canonical_price = price_vwap_repo.get_canonical_price(
                                shared_session, asset_address, liquidity_minute, denom
                            )
                            
                            if canonical_price:
                                event_detail_repo.create_event_valuation(
                                    model_session,
                                    event=liquidity_event,
                                    denomination=denom,
                                    canonical_price=canonical_price.price,
                                    pricing_method=PricingMethod.CANONICAL
                                )
                                results['liquidity_valued'] += 1
                    
                    # 3. Process reward events
                    unvalued_rewards = reward_repo.get_unvalued_events_in_period(
                        model_session, period_id, asset_address
                    )
                    
                    for reward in unvalued_rewards:
                        reward_minute = int(reward.timestamp.timestamp() // 60 * 60)
                        
                        for denom in denominations:
                            if event_detail_repo.has_valuation(model_session, reward.content_id, denom):
                                continue
                                
                            canonical_price = price_vwap_repo.get_canonical_price(
                                shared_session, asset_address, reward_minute, denom
                            )
                            
                            if canonical_price:
                                event_detail_repo.create_event_valuation(
                                    model_session,
                                    event=reward,
                                    denomination=denom,
                                    canonical_price=canonical_price.price,
                                    pricing_method=PricingMethod.CANONICAL
                                )
                                results['rewards_valued'] += 1
                    
                    # 4. Process position events
                    unvalued_positions = position_repo.get_unvalued_events_in_period(
                        model_session, period_id, asset_address
                    )
                    
                    for position in unvalued_positions:
                        position_minute = int(position.timestamp.timestamp() // 60 * 60)
                        
                        for denom in denominations:
                            if event_detail_repo.has_valuation(model_session, position.content_id, denom):
                                continue
                                
                            canonical_price = price_vwap_repo.get_canonical_price(
                                shared_session, asset_address, position_minute, denom
                            )
                            
                            if canonical_price:
                                event_detail_repo.create_event_valuation(
                                    model_session,
                                    event=position,
                                    denomination=denom,
                                    canonical_price=canonical_price.price,
                                    pricing_method=PricingMethod.CANONICAL
                                )
                                results['positions_valued'] += 1
                                
                except Exception as e:
                    results['errors'] += 1
                    log_with_context(
                        self.logger, logging.ERROR, "Error calculating event valuations",
                        asset_address=asset_address,
                        period_id=period_id,
                        error=str(e)
                    )
                    continue
        
        log_with_context(
            self.logger, logging.INFO, "Event valuation calculation complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def generate_asset_ohlc_candles(
        self, 
        period_ids: List[int], 
        asset_address: str,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Generate OHLC candles from trade data aggregation per period.
        
        Creates asset_price records with open/high/low/close/volume data from trade_details.
        Uses volume-weighted calculations for accurate price candles.
        
        Args:
            period_ids: List of period IDs to generate candles for
            asset_address: Asset to generate OHLC data for
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with statistics: {'usd_candles_created': 0, 'avax_candles_created': 0, 'errors': 0}
        """
        log_with_context(
            self.logger, logging.INFO, "Generating OHLC candles",
            asset_address=asset_address,
            periods_count=len(period_ids),
            denomination=denomination.value if denomination else "both"
        )
        
        results = {'usd_candles_created': 0, 'avax_candles_created': 0, 'errors': 0}
        denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
        
        # Get repositories
        asset_price_repo = self.repository_manager.get_asset_price_repository()
        trade_detail_repo = self.repository_manager.get_trade_detail_repository()
        
        with self.model_db_manager.get_session() as session:
            for period_id in period_ids:
                try:
                    for denom in denominations:
                        # Check if OHLC candle already exists
                        existing_candle = asset_price_repo.get_candle(
                            session, period_id, asset_address, denom
                        )
                        if existing_candle:
                            continue
                        
                        # Get trade details for this period
                        trade_details = trade_detail_repo.get_trades_in_period(
                            session, period_id, asset_address, denom
                        )
                        
                        if not trade_details:
                            log_with_context(
                                self.logger, logging.DEBUG, "No trade data for OHLC candle",
                                asset_address=asset_address,
                                period_id=period_id,
                                denomination=denom.value
                            )
                            continue
                        
                        # Calculate OHLC from trade data
                        ohlc_data = self._calculate_ohlc_from_trades(trade_details, denom)
                        
                        if ohlc_data:
                            # Create asset_price record
                            asset_price_repo.create_ohlc_candle(
                                session,
                                period_id=period_id,
                                asset_address=asset_address,
                                denomination=denom,
                                **ohlc_data
                            )
                            
                            if denom == PricingDenomination.USD:
                                results['usd_candles_created'] += 1
                            else:
                                results['avax_candles_created'] += 1
                            
                            log_with_context(
                                self.logger, logging.DEBUG, "Created OHLC candle",
                                asset_address=asset_address,
                                period_id=period_id,
                                denomination=denom.value,
                                **{k: float(v) if isinstance(v, Decimal) else v for k, v in ohlc_data.items()}
                            )
                
                except Exception as e:
                    results['errors'] += 1
                    log_with_context(
                        self.logger, logging.ERROR, "Error generating OHLC candle",
                        asset_address=asset_address,
                        period_id=period_id,
                        error=str(e)
                    )
                    continue
        
        log_with_context(
            self.logger, logging.INFO, "OHLC candle generation complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def calculate_asset_volume_by_protocol(
        self, 
        period_ids: List[int], 
        asset_address: str,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Calculate protocol-level volume metrics per period using contract.project.
        
        Creates asset_volume records aggregating swap volume by protocol (Blub, LFJ, Pharaoh, etc.).
        Depends on contract.project field being populated correctly.
        
        Args:
            period_ids: List of period IDs to calculate volume for
            asset_address: Asset to calculate protocol volume for
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with statistics: {'usd_volumes_created': 0, 'avax_volumes_created': 0, 'errors': 0}
        """
        log_with_context(
            self.logger, logging.INFO, "Calculating protocol volume metrics",
            asset_address=asset_address,
            periods_count=len(period_ids),
            denomination=denomination.value if denomination else "both"
        )
        
        results = {'usd_volumes_created': 0, 'avax_volumes_created': 0, 'errors': 0}
        denominations = [denomination] if denomination else [PricingDenomination.USD, PricingDenomination.AVAX]
        
        # Get repositories
        asset_volume_repo = self.repository_manager.get_asset_volume_repository()
        pool_swap_detail_repo = self.repository_manager.get_pool_swap_detail_repository()
        
        with self.shared_db_manager.get_session() as shared_session, \
             self.model_db_manager.get_session() as model_session:
            
            for period_id in period_ids:
                try:
                    for denom in denominations:
                        # Get protocol volume aggregations for this period
                        protocol_volumes = pool_swap_detail_repo.get_protocol_volume_aggregation(
                            model_session, shared_session, period_id, asset_address, denom
                        )
                        
                        for protocol, volume_data in protocol_volumes.items():
                            # Check if volume record already exists
                            existing_volume = asset_volume_repo.get_volume(
                                model_session, period_id, asset_address, denom, protocol
                            )
                            if existing_volume:
                                continue
                            
                            # Create asset_volume record
                            asset_volume_repo.create_volume_metric(
                                model_session,
                                period_id=period_id,
                                asset_address=asset_address,
                                denomination=denom,
                                protocol=protocol,
                                volume=volume_data['total_volume'],
                                pool_count=volume_data['pool_count'],
                                swap_count=volume_data['swap_count']
                            )
                            
                            if denom == PricingDenomination.USD:
                                results['usd_volumes_created'] += 1
                            else:
                                results['avax_volumes_created'] += 1
                            
                            log_with_context(
                                self.logger, logging.DEBUG, "Created protocol volume metric",
                                asset_address=asset_address,
                                period_id=period_id,
                                denomination=denom.value,
                                protocol=protocol,
                                volume=float(volume_data['total_volume'])
                            )
                
                except Exception as e:
                    results['errors'] += 1
                    log_with_context(
                        self.logger, logging.ERROR, "Error calculating protocol volume",
                        asset_address=asset_address,
                        period_id=period_id,
                        error=str(e)
                    )
                    continue
        
        log_with_context(
            self.logger, logging.INFO, "Protocol volume calculation complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def update_event_valuations(
        self, 
        asset_address: str, 
        days: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive event valuation update for an asset.
        
        Finds periods with unvalued events and applies canonical pricing.
        Main method for scheduled event valuation updates.
        
        Args:
            asset_address: Asset to update event valuations for
            days: Number of days to look back. If None, processes all unvalued events
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with comprehensive valuation statistics
        """
        log_with_context(
            self.logger, logging.INFO, "Starting event valuation update",
            asset_address=asset_address,
            days=days,
            denomination=denomination.value if denomination else "both"
        )
        
        # Get repositories for gap detection
        event_detail_repo = self.repository_manager.get_event_detail_repository()
        periods_repo = self.repository_manager.get_periods_repository()

        with self.model_db_manager.get_session() as model_session, \
             self.shared_db_manager.get_session() as shared_session:
            
            # Determine periods to process
            if days:
                # Process specific number of days back
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
                target_periods = periods_repo.get_periods_since(
                    shared_session, cutoff_time, PeriodType.FIVE_MINUTE
                )
            else:
                # Find periods with unvalued events
                target_periods = event_detail_repo.find_periods_with_unvalued_events(
                    model_session, asset_address
                )
            
            period_ids = [p.id for p in target_periods]
        
        if not period_ids:
            log_with_context(
                self.logger, logging.INFO, "No event valuation gaps found",
                asset_address=asset_address
            )
            return {'transfers_valued': 0, 'liquidity_valued': 0, 'rewards_valued': 0, 'positions_valued': 0, 'errors': 0}
        
        log_with_context(
            self.logger, logging.INFO, "Processing event valuation gaps",
            asset_address=asset_address,
            periods_count=len(period_ids)
        )
        
        # Calculate event valuations for identified periods
        return self.calculate_event_valuations(period_ids, asset_address, denomination)

    def update_analytics(
        self, 
        asset_address: str, 
        days: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive analytics update for an asset (OHLC + volume metrics).
        
        Generates missing OHLC candles and protocol volume metrics.
        Main method for scheduled analytics updates.
        
        Args:
            asset_address: Asset to update analytics for
            days: Number of days to look back. If None, processes all gaps
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with comprehensive analytics statistics
        """
        log_with_context(
            self.logger, logging.INFO, "Starting analytics update",
            asset_address=asset_address,
            days=days,
            denomination=denomination.value if denomination else "both"
        )
        
        results = {
            'usd_candles_created': 0,
            'avax_candles_created': 0,
            'usd_volumes_created': 0,
            'avax_volumes_created': 0,
            'total_errors': 0
        }
        
        # Get repositories for gap detection
        asset_price_repo = self.repository_manager.get_asset_price_repository()
        asset_volume_repo = self.repository_manager.get_asset_volume_repository()
        periods_repo = self.repository_manager.get_periods_repository()

        with self.model_db_manager.get_session() as model_session, \
             self.shared_db_manager.get_session() as shared_session:
            
            # Determine periods to process
            if days:
                # Process specific number of days back
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=days)
                target_periods = periods_repo.get_periods_since(
                    shared_session, cutoff_time, PeriodType.FIVE_MINUTE
                )
            else:
                # Find periods with missing analytics
                ohlc_gaps = asset_price_repo.find_periods_with_missing_candles(
                    model_session, asset_address
                )
                volume_gaps = asset_volume_repo.find_periods_with_missing_volumes(
                    model_session, asset_address
                )
                
                # Combine gaps
                all_period_ids = set(ohlc_gaps + volume_gaps)
                target_periods = periods_repo.get_periods_by_ids(
                    shared_session, list(all_period_ids)
                )
            
            period_ids = [p.id for p in target_periods]
        
        if not period_ids:
            log_with_context(
                self.logger, logging.INFO, "No analytics gaps found",
                asset_address=asset_address
            )
            return results
        
        log_with_context(
            self.logger, logging.INFO, "Processing analytics gaps",
            asset_address=asset_address,
            periods_count=len(period_ids)
        )
        
        try:
            # Generate OHLC candles
            ohlc_results = self.generate_asset_ohlc_candles(period_ids, asset_address, denomination)
            results['usd_candles_created'] = ohlc_results.get('usd_candles_created', 0)
            results['avax_candles_created'] = ohlc_results.get('avax_candles_created', 0)
            results['total_errors'] += ohlc_results.get('errors', 0)
            
            # Calculate protocol volume metrics
            volume_results = self.calculate_asset_volume_by_protocol(period_ids, asset_address, denomination)
            results['usd_volumes_created'] = volume_results.get('usd_volumes_created', 0)
            results['avax_volumes_created'] = volume_results.get('avax_volumes_created', 0)
            results['total_errors'] += volume_results.get('errors', 0)
            
        except Exception as e:
            results['total_errors'] += 1
            log_with_context(
                self.logger, logging.ERROR, "Error in analytics update",
                asset_address=asset_address,
                error=str(e)
            )
            raise
        
        log_with_context(
            self.logger, logging.INFO, "Analytics update complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def update_all(
        self, 
        asset_address: str,
        days: Optional[int] = None,
        denomination: Optional[PricingDenomination] = None
    ) -> Dict[str, int]:
        """
        Comprehensive calculation update including event valuations and analytics.
        
        Main entry point for complete calculation service updates.
        
        Args:
            asset_address: Asset to update all calculations for
            days: Number of days to look back. If None, processes all gaps
            denomination: usd, avax, or None for both
            
        Returns:
            Dict with comprehensive statistics from all calculation operations
        """
        log_with_context(
            self.logger, logging.INFO, "Starting comprehensive calculation update",
            asset_address=asset_address,
            days=days,
            denomination=denomination.value if denomination else "both"
        )
        
        results = {
            'transfers_valued': 0,
            'liquidity_valued': 0,
            'rewards_valued': 0,
            'positions_valued': 0,
            'usd_candles_created': 0,
            'avax_candles_created': 0,
            'usd_volumes_created': 0,
            'avax_volumes_created': 0,
            'total_errors': 0
        }
        
        try:
            # 1. Event Valuations
            log_with_context(self.logger, logging.INFO, "Updating event valuations", asset_address=asset_address)
            
            valuation_results = self.update_event_valuations(asset_address, days, denomination)
            results['transfers_valued'] = valuation_results.get('transfers_valued', 0)
            results['liquidity_valued'] = valuation_results.get('liquidity_valued', 0)
            results['rewards_valued'] = valuation_results.get('rewards_valued', 0)
            results['positions_valued'] = valuation_results.get('positions_valued', 0)
            results['total_errors'] += valuation_results.get('errors', 0)
            
            # 2. Analytics (OHLC + Volume)
            log_with_context(self.logger, logging.INFO, "Updating analytics", asset_address=asset_address)
            
            analytics_results = self.update_analytics(asset_address, days, denomination)
            results['usd_candles_created'] = analytics_results.get('usd_candles_created', 0)
            results['avax_candles_created'] = analytics_results.get('avax_candles_created', 0)
            results['usd_volumes_created'] = analytics_results.get('usd_volumes_created', 0)
            results['avax_volumes_created'] = analytics_results.get('avax_volumes_created', 0)
            results['total_errors'] += analytics_results.get('total_errors', 0)
            
        except Exception as e:
            results['total_errors'] += 1
            log_with_context(
                self.logger, logging.ERROR, "Error in comprehensive calculation update",
                asset_address=asset_address,
                error=str(e)
            )
            raise
        
        log_with_context(
            self.logger, logging.INFO, "Comprehensive calculation update complete",
            asset_address=asset_address,
            **results
        )
        
        return results

    def get_calculation_status(self, asset_address: str) -> Dict[str, any]:
        """
        Get comprehensive calculation status for an asset.
        
        Provides detailed statistics about calculation coverage and gaps for monitoring.
        
        Args:
            asset_address: Asset to check calculation status for
            
        Returns:
            Dict with comprehensive calculation statistics and gap information
        """
        log_with_context(
            self.logger, logging.INFO, "Getting calculation status",
            asset_address=asset_address
        )
        
        # Get repositories
        event_detail_repo = self.repository_manager.get_event_detail_repository()
        asset_price_repo = self.repository_manager.get_asset_price_repository()
        asset_volume_repo = self.repository_manager.get_asset_volume_repository()
        
        status = {
            'asset_address': asset_address,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'event_valuations': {},
            'analytics': {},
            'gaps': {}
        }
        
        with self.model_db_manager.get_session() as model_session:
            
            # Event valuation status
            for denom in [PricingDenomination.USD, PricingDenomination.AVAX]:
                valuation_stats = event_detail_repo.get_valuation_stats(
                    model_session, asset_address, denom
                )
                status['event_valuations'][denom.value] = valuation_stats
                
                # Find valuation gaps
                unvalued_count = event_detail_repo.count_unvalued_events(
                    model_session, asset_address, denom
                )
                status['gaps'][f'unvalued_{denom.value}'] = unvalued_count
            
            # Analytics status
            for denom in [PricingDenomination.USD, PricingDenomination.AVAX]:
                ohlc_stats = asset_price_repo.get_candle_stats(
                    model_session, asset_address, denom
                )
                volume_stats = asset_volume_repo.get_volume_stats(
                    model_session, asset_address, denom
                )
                
                status['analytics'][denom.value] = {
                    'ohlc_candles': ohlc_stats,
                    'volume_metrics': volume_stats
                }
                
                # Find analytics gaps
                missing_candles = asset_price_repo.count_missing_candles(
                    model_session, asset_address, denom
                )
                missing_volumes = asset_volume_repo.count_missing_volumes(
                    model_session, asset_address, denom
                )
                
                status['gaps'][f'missing_candles_{denom.value}'] = missing_candles
                status['gaps'][f'missing_volumes_{denom.value}'] = missing_volumes
            
            # Recent activity
            status['recent_activity'] = {
                'last_event_valuation': event_detail_repo.get_latest_valuation_timestamp(
                    model_session, asset_address
                ),
                'last_ohlc_candle': asset_price_repo.get_latest_candle_timestamp(
                    model_session, asset_address
                ),
                'last_volume_metric': asset_volume_repo.get_latest_volume_timestamp(
                    model_session, asset_address
                )
            }
        
        return status

    def _calculate_ohlc_from_trades(self, trade_details: List, denomination: PricingDenomination) -> Optional[Dict[str, Decimal]]:
        """
        Calculate OHLC data from trade details.
        
        Args:
            trade_details: List of trade detail records
            denomination: USD or AVAX denomination
            
        Returns:
            Dict with open, high, low, close, volume data or None if insufficient data
        """
        if not trade_details:
            return None
        
        # Sort by timestamp
        sorted_trades = sorted(trade_details, key=lambda t: t.timestamp)
        
        prices = []
        total_volume = Decimal('0')
        
        for trade in sorted_trades:
            if denomination == PricingDenomination.USD:
                price = trade.price_usd
                volume = trade.volume_usd
            else:
                price = trade.price_avax  
                volume = trade.volume_avax
            
            if price and volume:
                prices.append(price)
                total_volume += volume
        
        if not prices:
            return None
        
        return {
            'open_price': prices[0],
            'high_price': max(prices),
            'low_price': min(prices),
            'close_price': prices[-1],
            'volume': total_volume,
            'trade_count': len(prices)
        }