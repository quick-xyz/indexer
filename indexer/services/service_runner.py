# indexer/services/service_runner.py

"""
CLI Runner for Pricing and Calculation Services

Usage:
    python -m indexer.services.service_runner pricing update-canonical --asset 0xToken --minutes 1440
    python -m indexer.services.service_runner pricing update-global --asset 0xToken --days 7
    python -m indexer.services.service_runner pricing update-all --asset 0xToken
    python -m indexer.services.service_runner calculation update-events --asset 0xToken --days 7
    python -m indexer.services.service_runner calculation update-analytics --asset 0xToken --days 7
    python -m indexer.services.service_runner calculation update-all --asset 0xToken
    python -m indexer.services.service_runner update-all --asset 0xToken
    python -m indexer.services.service_runner status --asset 0xToken

This script can be run via cron job for scheduled service updates.
"""

import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional, List

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from indexer.database.repository_manager import RepositoryManager
from indexer.database.connection import ModelDatabaseManager, SharedDatabaseManager
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.services.pricing_service import PricingService
from indexer.services.calculation_service import CalculationService
from indexer.database.indexer.tables.detail.pool_swap_detail import PricingDenomination


class ServiceRunner:
    """CLI runner for pricing and calculation service operations"""
    
    def __init__(self, model_name: Optional[str] = None):
        # Initialize indexer with DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services from container
        self.shared_db_manager = self.container.get(SharedDatabaseManager)
        self.model_db_manager = self.container.get(ModelDatabaseManager)
        self.rpc_client = self.container.get(QuickNodeRpcClient)
        
        # Create service instances
        self.pricing_service = PricingService(
            shared_db_manager=self.shared_db_manager,
            model_db_manager=self.model_db_manager,
            rpc_client=self.rpc_client,
        )
        
        self.calculation_service = CalculationService(
            shared_db_manager=self.shared_db_manager,
            model_db_manager=self.model_db_manager,
        )
        
        self.logger = IndexerLogger.get_logger('services.service_runner')
        
        log_with_context(
            self.logger, INFO, "ServiceRunner initialized",
            model_name=self.config.model_name,
            shared_database=self.shared_db_manager.config.url.split('/')[-1],
            model_database=self.model_db_manager.config.url.split('/')[-1]
        )

    # =====================================================================
    # PRICING SERVICE OPERATIONS
    # =====================================================================

    def run_canonical_pricing_update(
        self, 
        asset_address: str, 
        minutes: Optional[int] = None,
        denomination: Optional[str] = None
    ) -> None:
        """Update canonical pricing for an asset"""
        print(f"🔄 Updating Canonical Pricing - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        if minutes:
            print(f"Minutes: {minutes}")
        if denomination:
            print(f"Denomination: {denomination}")
        print()
        
        try:
            denom = PricingDenomination(denomination) if denomination else None
            
            start_time = datetime.now()
            results = self.pricing_service.update_canonical_pricing(
                asset_address=asset_address,
                minutes=minutes,
                denomination=denom
            )
            end_time = datetime.now()
            
            self._print_pricing_results("Canonical Pricing Update", results, start_time, end_time)
            
        except Exception as e:
            print(f"❌ Canonical pricing update failed: {e}")
            raise

    def run_global_pricing_update(
        self, 
        asset_address: str, 
        days: Optional[int] = None,
        denomination: Optional[str] = None
    ) -> None:
        """Update global pricing for an asset"""
        print(f"🌐 Updating Global Pricing - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        if days:
            print(f"Days: {days}")
        if denomination:
            print(f"Denomination: {denomination}")
        print()
        
        try:
            denom = PricingDenomination(denomination) if denomination else None
            
            start_time = datetime.now()
            results = self.pricing_service.update_global_pricing(
                asset_address=asset_address,
                days=days,
                denomination=denom
            )
            end_time = datetime.now()
            
            self._print_pricing_results("Global Pricing Update", results, start_time, end_time)
            
        except Exception as e:
            print(f"❌ Global pricing update failed: {e}")
            raise

    def run_pricing_update_all(
        self, 
        asset_address: str,
        days: Optional[int] = None,
        denomination: Optional[str] = None
    ) -> None:
        """Comprehensive pricing update for an asset"""
        print(f"💰 Comprehensive Pricing Update - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        if days:
            print(f"Days: {days}")
        if denomination:
            print(f"Denomination: {denomination}")
        print()
        
        try:
            denom = PricingDenomination(denomination) if denomination else None
            
            start_time = datetime.now()
            results = self.pricing_service.update_pricing_all(
                asset_address=asset_address,
                days=days,
                denomination=denom
            )
            end_time = datetime.now()
            
            self._print_comprehensive_pricing_results("Comprehensive Pricing Update", results, start_time, end_time)
            
        except Exception as e:
            print(f"❌ Comprehensive pricing update failed: {e}")
            raise

    # =====================================================================
    # CALCULATION SERVICE OPERATIONS
    # =====================================================================

    def run_event_valuations_update(
        self, 
        asset_address: str, 
        days: Optional[int] = None,
        denomination: Optional[str] = None
    ) -> None:
        """Update event valuations for an asset"""
        print(f"📊 Updating Event Valuations - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        if days:
            print(f"Days: {days}")
        if denomination:
            print(f"Denomination: {denomination}")
        print()
        
        try:
            denom = PricingDenomination(denomination) if denomination else None
            
            start_time = datetime.now()
            results = self.calculation_service.update_event_valuations(
                asset_address=asset_address,
                days=days,
                denomination=denom
            )
            end_time = datetime.now()
            
            self._print_calculation_results("Event Valuations Update", results, start_time, end_time)
            
        except Exception as e:
            print(f"❌ Event valuations update failed: {e}")
            raise

    def run_analytics_update(
        self, 
        asset_address: str, 
        days: Optional[int] = None,
        denomination: Optional[str] = None
    ) -> None:
        """Update analytics (OHLC + volume) for an asset"""
        print(f"📈 Updating Analytics - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        if days:
            print(f"Days: {days}")
        if denomination:
            print(f"Denomination: {denomination}")
        print()
        
        try:
            denom = PricingDenomination(denomination) if denomination else None
            
            start_time = datetime.now()
            results = self.calculation_service.update_analytics(
                asset_address=asset_address,
                days=days,
                denomination=denom
            )
            end_time = datetime.now()
            
            self._print_analytics_results("Analytics Update", results, start_time, end_time)
            
        except Exception as e:
            print(f"❌ Analytics update failed: {e}")
            raise

    def run_calculation_update_all(
        self, 
        asset_address: str,
        days: Optional[int] = None,
        denomination: Optional[str] = None
    ) -> None:
        """Comprehensive calculation update for an asset"""
        print(f"🧮 Comprehensive Calculation Update - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        if days:
            print(f"Days: {days}")
        if denomination:
            print(f"Denomination: {denomination}")
        print()
        
        try:
            denom = PricingDenomination(denomination) if denomination else None
            
            start_time = datetime.now()
            results = self.calculation_service.update_all(
                asset_address=asset_address,
                days=days,
                denomination=denom
            )
            end_time = datetime.now()
            
            self._print_comprehensive_calculation_results("Comprehensive Calculation Update", results, start_time, end_time)
            
        except Exception as e:
            print(f"❌ Comprehensive calculation update failed: {e}")
            raise

    # =====================================================================
    # COMPREHENSIVE OPERATIONS
    # =====================================================================

    def run_update_all_services(
        self, 
        asset_address: str,
        days: Optional[int] = None,
        denomination: Optional[str] = None
    ) -> None:
        """Update all services (pricing + calculation) for an asset"""
        print(f"🚀 Comprehensive Service Update - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        if days:
            print(f"Days: {days}")
        if denomination:
            print(f"Denomination: {denomination}")
        print()
        
        overall_start_time = datetime.now()
        
        try:
            denom = PricingDenomination(denomination) if denomination else None
            
            # 1. Pricing Service Update
            print("🔄 Phase 1: Pricing Service Update")
            print("-" * 40)
            
            pricing_start = datetime.now()
            pricing_results = self.pricing_service.update_pricing_all(
                asset_address=asset_address,
                days=days,
                denomination=denom
            )
            pricing_end = datetime.now()
            
            self._print_comprehensive_pricing_results("Pricing Phase", pricing_results, pricing_start, pricing_end)
            print()
            
            # 2. Calculation Service Update
            print("📊 Phase 2: Calculation Service Update")
            print("-" * 40)
            
            calculation_start = datetime.now()
            calculation_results = self.calculation_service.update_all(
                asset_address=asset_address,
                days=days,
                denomination=denom
            )
            calculation_end = datetime.now()
            
            self._print_comprehensive_calculation_results("Calculation Phase", calculation_results, calculation_start, calculation_end)
            
            # Overall summary
            overall_end_time = datetime.now()
            duration = (overall_end_time - overall_start_time).total_seconds()
            
            print()
            print("🎉 COMPREHENSIVE UPDATE COMPLETE")
            print("=" * 60)
            print(f"Total Duration: {duration:.2f} seconds")
            print(f"Pricing Errors: {pricing_results.get('total_errors', 0)}")
            print(f"Calculation Errors: {calculation_results.get('total_errors', 0)}")
            print("✅ All services updated successfully!")
            
        except Exception as e:
            print(f"❌ Comprehensive service update failed: {e}")
            raise

    # =====================================================================
    # STATUS AND MONITORING
    # =====================================================================

    def show_service_status(self, asset_address: str) -> None:
        """Show comprehensive status for both services"""
        print(f"📋 Service Status - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print()
        
        try:
            # Get pricing status
            print("💰 PRICING SERVICE STATUS")
            print("-" * 40)
            pricing_status = self.pricing_service.get_pricing_status(asset_address)
            self._print_pricing_status(pricing_status)
            print()
            
            # Get calculation status
            print("📊 CALCULATION SERVICE STATUS")
            print("-" * 40)
            calculation_status = self.calculation_service.get_calculation_status(asset_address)
            self._print_calculation_status(calculation_status)
            
        except Exception as e:
            print(f"❌ Failed to get service status: {e}")
            raise

    def show_pricing_status(self, asset_address: str) -> None:
        """Show pricing service status only"""
        print(f"💰 Pricing Service Status - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        print()
        
        try:
            status = self.pricing_service.get_pricing_status(asset_address)
            self._print_pricing_status(status)
            
        except Exception as e:
            print(f"❌ Failed to get pricing status: {e}")
            raise

    def show_calculation_status(self, asset_address: str) -> None:
        """Show calculation service status only"""
        print(f"📊 Calculation Service Status - {self.config.model_name}")
        print("=" * 60)
        print(f"Asset: {asset_address}")
        print()
        
        try:
            status = self.calculation_service.get_calculation_status(asset_address)
            self._print_calculation_status(status)
            
        except Exception as e:
            print(f"❌ Failed to get calculation status: {e}")
            raise

    # =====================================================================
    # UTILITY METHODS
    # =====================================================================

    def _print_pricing_results(self, operation: str, results: dict, start_time: datetime, end_time: datetime) -> None:
        """Print pricing operation results"""
        duration = (end_time - start_time).total_seconds()
        
        print(f"✅ {operation} Complete")
        print("-" * 40)
        print(f"Duration: {duration:.2f} seconds")
        
        if 'usd_created' in results:
            print(f"USD Prices Created: {results['usd_created']}")
        if 'avax_created' in results:
            print(f"AVAX Prices Created: {results['avax_created']}")
        if 'swaps_priced' in results:
            print(f"Swaps Priced: {results['swaps_priced']}")
        if 'trades_priced' in results:
            print(f"Trades Priced: {results['trades_priced']}")
        if 'errors' in results:
            print(f"Errors: {results['errors']}")

    def _print_comprehensive_pricing_results(self, operation: str, results: dict, start_time: datetime, end_time: datetime) -> None:
        """Print comprehensive pricing operation results"""
        duration = (end_time - start_time).total_seconds()
        
        print(f"✅ {operation} Complete")
        print("-" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Infrastructure Updates:")
        print(f"  • Periods Created: {results.get('periods_created', 0)}")
        print(f"  • Block Prices Created: {results.get('block_prices_created', 0)}")
        print(f"Direct Pricing:")
        print(f"  • Swaps Priced: {results.get('swaps_priced_direct', 0)}")
        print(f"  • Trades Priced: {results.get('trades_priced_direct', 0)}")
        print(f"Canonical Pricing:")
        print(f"  • USD Canonical: {results.get('usd_canonical_created', 0)}")
        print(f"  • AVAX Canonical: {results.get('avax_canonical_created', 0)}")
        print(f"Global Pricing:")
        print(f"  • Swaps Priced: {results.get('swaps_priced_global', 0)}")
        print(f"  • Trades Priced: {results.get('trades_priced_global', 0)}")
        print(f"Total Errors: {results.get('total_errors', 0)}")

    def _print_calculation_results(self, operation: str, results: dict, start_time: datetime, end_time: datetime) -> None:
        """Print calculation operation results"""
        duration = (end_time - start_time).total_seconds()
        
        print(f"✅ {operation} Complete")
        print("-" * 40)
        print(f"Duration: {duration:.2f} seconds")
        
        if 'transfers_valued' in results:
            print(f"Transfers Valued: {results['transfers_valued']}")
        if 'liquidity_valued' in results:
            print(f"Liquidity Valued: {results['liquidity_valued']}")
        if 'rewards_valued' in results:
            print(f"Rewards Valued: {results['rewards_valued']}")
        if 'positions_valued' in results:
            print(f"Positions Valued: {results['positions_valued']}")
        if 'errors' in results:
            print(f"Errors: {results['errors']}")

    def _print_analytics_results(self, operation: str, results: dict, start_time: datetime, end_time: datetime) -> None:
        """Print analytics operation results"""
        duration = (end_time - start_time).total_seconds()
        
        print(f"✅ {operation} Complete")
        print("-" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"OHLC Candles:")
        print(f"  • USD: {results.get('usd_candles_created', 0)}")
        print(f"  • AVAX: {results.get('avax_candles_created', 0)}")
        print(f"Volume Metrics:")
        print(f"  • USD: {results.get('usd_volumes_created', 0)}")
        print(f"  • AVAX: {results.get('avax_volumes_created', 0)}")
        print(f"Total Errors: {results.get('total_errors', 0)}")

    def _print_comprehensive_calculation_results(self, operation: str, results: dict, start_time: datetime, end_time: datetime) -> None:
        """Print comprehensive calculation operation results"""
        duration = (end_time - start_time).total_seconds()
        
        print(f"✅ {operation} Complete")
        print("-" * 40)
        print(f"Duration: {duration:.2f} seconds")
        print(f"Event Valuations:")
        print(f"  • Transfers: {results.get('transfers_valued', 0)}")
        print(f"  • Liquidity: {results.get('liquidity_valued', 0)}")
        print(f"  • Rewards: {results.get('rewards_valued', 0)}")
        print(f"  • Positions: {results.get('positions_valued', 0)}")
        print(f"Analytics:")
        print(f"  • USD Candles: {results.get('usd_candles_created', 0)}")
        print(f"  • AVAX Candles: {results.get('avax_candles_created', 0)}")
        print(f"  • USD Volumes: {results.get('usd_volumes_created', 0)}")
        print(f"  • AVAX Volumes: {results.get('avax_volumes_created', 0)}")
        print(f"Total Errors: {results.get('total_errors', 0)}")

    def _print_pricing_status(self, status: dict) -> None:
        """Print pricing service status"""
        canonical = status.get('canonical_pricing', {})
        direct = status.get('direct_pricing', {})
        global_pricing = status.get('global_pricing', {})
        gaps = status.get('gaps', {})
        recent = status.get('recent_activity', {})
        
        print("Canonical Pricing:")
        print(f"  • USD: {canonical.get('usd', {})}")
        print(f"  • AVAX: {canonical.get('avax', {})}")
        print()
        print("Direct Pricing:")
        print(f"  • Swaps: {direct.get('swaps', {})}")
        print(f"  • Trades: {direct.get('trades', {})}")
        print()
        print("Global Pricing Gaps:")
        print(f"  • Unpriced Swaps: {global_pricing.get('unpriced_swaps', 0)}")
        print(f"  • Unpriced Trades: {global_pricing.get('unpriced_trades', 0)}")
        print()
        print("Recent Activity:")
        print(f"  • Last Canonical Price: {recent.get('last_canonical_price', 'None')}")
        print(f"  • Last Direct Swap: {recent.get('last_direct_swap_pricing', 'None')}")
        print(f"  • Last Direct Trade: {recent.get('last_direct_trade_pricing', 'None')}")

    def _print_calculation_status(self, status: dict) -> None:
        """Print calculation service status"""
        valuations = status.get('event_valuations', {})
        analytics = status.get('analytics', {})
        gaps = status.get('gaps', {})
        recent = status.get('recent_activity', {})
        
        print("Event Valuations:")
        print(f"  • USD: {valuations.get('usd', {})}")
        print(f"  • AVAX: {valuations.get('avax', {})}")
        print()
        print("Analytics:")
        for denom in ['usd', 'avax']:
            denom_analytics = analytics.get(denom, {})
            print(f"  • {denom.upper()}:")
            print(f"    - OHLC Candles: {denom_analytics.get('ohlc_candles', {})}")
            print(f"    - Volume Metrics: {denom_analytics.get('volume_metrics', {})}")
        print()
        print("Gaps:")
        for gap_type, count in gaps.items():
            print(f"  • {gap_type}: {count}")
        print()
        print("Recent Activity:")
        print(f"  • Last Event Valuation: {recent.get('last_event_valuation', 'None')}")
        print(f"  • Last OHLC Candle: {recent.get('last_ohlc_candle', 'None')}")
        print(f"  • Last Volume Metric: {recent.get('last_volume_metric', 'None')}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='Service Runner for Pricing and Calculation Services')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    
    subparsers = parser.add_subparsers(dest='command', help='Service commands')
    
    # Pricing commands
    pricing_parser = subparsers.add_parser('pricing', help='Pricing service operations')
    pricing_subparsers = pricing_parser.add_subparsers(dest='pricing_command')
    
    # pricing update-canonical
    canonical_parser = pricing_subparsers.add_parser('update-canonical', help='Update canonical pricing')
    canonical_parser.add_argument('--asset', required=True, help='Asset address')
    canonical_parser.add_argument('--minutes', type=int, help='Number of minutes to process')
    canonical_parser.add_argument('--denomination', choices=['usd', 'avax'], help='Denomination')
    
    # pricing update-global
    global_parser = pricing_subparsers.add_parser('update-global', help='Update global pricing')
    global_parser.add_argument('--asset', required=True, help='Asset address')
    global_parser.add_argument('--days', type=int, help='Number of days to look back')
    global_parser.add_argument('--denomination', choices=['usd', 'avax'], help='Denomination')
    
    # pricing update-all
    pricing_all_parser = pricing_subparsers.add_parser('update-all', help='Comprehensive pricing update')
    pricing_all_parser.add_argument('--asset', required=True, help='Asset address')
    pricing_all_parser.add_argument('--days', type=int, help='Number of days to look back')
    pricing_all_parser.add_argument('--denomination', choices=['usd', 'avax'], help='Denomination')
    
    # pricing status
    pricing_status_parser = pricing_subparsers.add_parser('status', help='Show pricing status')
    pricing_status_parser.add_argument('--asset', required=True, help='Asset address')
    
    # Calculation commands
    calculation_parser = subparsers.add_parser('calculation', help='Calculation service operations')
    calculation_subparsers = calculation_parser.add_subparsers(dest='calculation_command')
    
    # calculation update-events
    events_parser = calculation_subparsers.add_parser('update-events', help='Update event valuations')
    events_parser.add_argument('--asset', required=True, help='Asset address')
    events_parser.add_argument('--days', type=int, help='Number of days to look back')
    events_parser.add_argument('--denomination', choices=['usd', 'avax'], help='Denomination')
    
    # calculation update-analytics
    analytics_parser = calculation_subparsers.add_parser('update-analytics', help='Update analytics')
    analytics_parser.add_argument('--asset', required=True, help='Asset address')
    analytics_parser.add_argument('--days', type=int, help='Number of days to look back')
    analytics_parser.add_argument('--denomination', choices=['usd', 'avax'], help='Denomination')
    
    # calculation update-all
    calc_all_parser = calculation_subparsers.add_parser('update-all', help='Comprehensive calculation update')
    calc_all_parser.add_argument('--asset', required=True, help='Asset address')
    calc_all_parser.add_argument('--days', type=int, help='Number of days to look back')
    calc_all_parser.add_argument('--denomination', choices=['usd', 'avax'], help='Denomination')
    
    # calculation status
    calc_status_parser = calculation_subparsers.add_parser('status', help='Show calculation status')
    calc_status_parser.add_argument('--asset', required=True, help='Asset address')
    
    # Global commands
    update_all_parser = subparsers.add_parser('update-all', help='Update all services')
    update_all_parser.add_argument('--asset', required=True, help='Asset address')
    update_all_parser.add_argument('--days', type=int, help='Number of days to look back')
    update_all_parser.add_argument('--denomination', choices=['usd', 'avax'], help='Denomination')
    
    status_parser = subparsers.add_parser('status', help='Show all service status')
    status_parser.add_argument('--asset', required=True, help='Asset address')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        runner = ServiceRunner(model_name=args.model)
        
        # Route commands
        if args.command == 'pricing':
            if args.pricing_command == 'update-canonical':
                runner.run_canonical_pricing_update(args.asset, args.minutes, args.denomination)
            elif args.pricing_command == 'update-global':
                runner.run_global_pricing_update(args.asset, args.days, args.denomination)
            elif args.pricing_command == 'update-all':
                runner.run_pricing_update_all(args.asset, args.days, args.denomination)
            elif args.pricing_command == 'status':
                runner.show_pricing_status(args.asset)
                
        elif args.command == 'calculation':
            if args.calculation_command == 'update-events':
                runner.run_event_valuations_update(args.asset, args.days, args.denomination)
            elif args.calculation_command == 'update-analytics':
                runner.run_analytics_update(args.asset, args.days, args.denomination)
            elif args.calculation_command == 'update-all':
                runner.run_calculation_update_all(args.asset, args.days, args.denomination)
            elif args.calculation_command == 'status':
                runner.show_calculation_status(args.asset)
                
        elif args.command == 'update-all':
            runner.run_update_all_services(args.asset, args.days, args.denomination)
            
        elif args.command == 'status':
            runner.show_service_status(args.asset)
        
    except Exception as e:
        print(f"\n💥 Service runner failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()