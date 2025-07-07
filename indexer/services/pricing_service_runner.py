#!/usr/bin/env python3
# indexer/services/pricing_service_runner.py

"""
Pricing Service CLI Runner

Usage:
    python -m indexer.services.pricing_service_runner update-periods
    python -m indexer.services.pricing_service_runner update-periods --types 5min,1hr
    python -m indexer.services.pricing_service_runner backfill --type 1hr --days 7
    python -m indexer.services.pricing_service_runner status

This script can be run via cron job for scheduled period updates.
"""

import sys
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.core.logging_config import IndexerLogger, log_with_context
from indexer.database.repository import RepositoryManager
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.services.pricing_service import PricingService
from indexer.database.models.pricing.periods import Period, PeriodType

import logging


class PricingServiceRunner:
    """CLI runner for pricing service operations"""
    
    def __init__(self, model_name: Optional[str] = None):
        # Initialize indexer with DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services from container
        self.repository_manager = self.container.get(RepositoryManager)
        self.rpc_client = self.container.get(QuickNodeRpcClient)
        
        # Create pricing service
        self.pricing_service = PricingService(
            repository_manager=self.repository_manager,
            rpc_client=self.rpc_client
        )
        
        self.logger = IndexerLogger.get_logger('services.pricing_service_runner')
        
        log_with_context(
            self.logger, logging.INFO, "PricingServiceRunner initialized",
            model_name=self.config.model_name
        )
    
    def update_periods(self, period_types: Optional[List[str]] = None) -> None:
        """Update periods to present time"""
        print(f"🕐 Updating periods to present - {self.config.model_name}")
        print("=" * 50)
        
        # Parse period types
        if period_types:
            try:
                parsed_types = []
                for pt_str in period_types:
                    # Map string to enum
                    type_mapping = {
                        '1min': PeriodType.ONE_MINUTE,
                        '5min': PeriodType.FIVE_MINUTES, 
                        '1hr': PeriodType.ONE_HOUR,
                        '4hr': PeriodType.FOUR_HOURS,
                        '1day': PeriodType.ONE_DAY
                    }
                    
                    if pt_str not in type_mapping:
                        print(f"❌ Unknown period type: {pt_str}")
                        print(f"   Available types: {', '.join(type_mapping.keys())}")
                        return
                    
                    parsed_types.append(type_mapping[pt_str])
                
                print(f"📊 Updating period types: {', '.join(period_types)}")
            except Exception as e:
                print(f"❌ Error parsing period types: {e}")
                return
        else:
            parsed_types = None
            print(f"📊 Updating all period types")
        
        # Run the update
        try:
            stats = self.pricing_service.update_periods_to_present(parsed_types)
            
            print(f"\n✅ Period Update Results:")
            print(f"   📈 Total periods created: {stats['total_periods_created']:,}")
            print(f"   🔢 Latest block processed: {stats['latest_block_processed']:,}")
            
            if stats['periods_by_type']:
                print(f"   📋 By period type:")
                for period_type, count in stats['periods_by_type'].items():
                    print(f"      {period_type}: {count:,} periods")
            
            if stats['errors']:
                print(f"   ⚠️  Errors ({len(stats['errors'])}):")
                for error in stats['errors']:
                    print(f"      • {error}")
            
        except Exception as e:
            print(f"❌ Period update failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Period update failed",
                error=str(e)
            )
    
    def update_minute_prices(self) -> None:
        """Update minute-by-minute AVAX prices to present"""
        print(f"💰 Updating minute prices to present - {self.config.model_name}")
        print("=" * 50)
        
        try:
            stats = self.pricing_service.update_minute_prices_to_present()
            
            print(f"\n✅ Minute Price Update Results:")
            print(f"   📈 Prices created: {stats['prices_created']:,}")
            print(f"   ⏭️  Prices skipped (existing): {stats['prices_skipped']:,}")
            print(f"   🔢 Latest block processed: {stats['latest_block_processed']:,}")
            
            if stats['errors']:
                print(f"   ⚠️  Errors ({len(stats['errors'])}):")
                for error in stats['errors']:
                    print(f"      • {error}")
        
        except Exception as e:
            print(f"❌ Minute price update failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Minute price update failed",
                error=str(e)
            )
    
    def update_all(self) -> None:
        """Update both periods and minute prices"""
        print(f"🔄 Full pricing update - {self.config.model_name}")
        print("=" * 60)
        
        # Update periods first (all types)
        period_types = None  # Update all period types
        self.update_periods(period_types)
        
        print("\n" + "=" * 60)
        
        # Then update minute prices
        self.update_minute_prices()
        """Update periods to present time"""
        print(f"🕐 Updating periods to present - {self.config.model_name}")
        print("=" * 50)
        
        # Parse period types
        if period_types:
            try:
                parsed_types = []
                for pt_str in period_types:
                    # Map string to enum
                    type_mapping = {
                        '1min': PeriodType.ONE_MINUTE,
                        '5min': PeriodType.FIVE_MINUTES, 
                        '1hr': PeriodType.ONE_HOUR,
                        '4hr': PeriodType.FOUR_HOURS,
                        '1day': PeriodType.ONE_DAY
                    }
                    
                    if pt_str not in type_mapping:
                        print(f"❌ Unknown period type: {pt_str}")
                        print(f"   Available types: {', '.join(type_mapping.keys())}")
                        return
                    
                    parsed_types.append(type_mapping[pt_str])
                
                print(f"📊 Updating period types: {', '.join(period_types)}")
            except Exception as e:
                print(f"❌ Error parsing period types: {e}")
                return
        else:
            parsed_types = None
            print(f"📊 Updating all period types")
        
        # Run the update
        try:
            stats = self.pricing_service.update_periods_to_present(parsed_types)
            
            print(f"\n✅ Period Update Results:")
            print(f"   📈 Total periods created: {stats['total_periods_created']:,}")
            print(f"   🔢 Latest block processed: {stats['latest_block_processed']:,}")
            
            if stats['periods_by_type']:
                print(f"   📋 By period type:")
                for period_type, count in stats['periods_by_type'].items():
                    print(f"      {period_type}: {count:,} periods")
            
            if stats['errors']:
                print(f"   ⚠️  Errors ({len(stats['errors'])}):")
                for error in stats['errors']:
                    print(f"      • {error}")
            
        except Exception as e:
            print(f"❌ Period update failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Period update failed",
                error=str(e)
            )
    
    def backfill_periods(
        self, 
        period_type: str, 
        days: int = 7,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> None:
        """Backfill missing periods"""
        print(f"🔄 Backfilling {period_type} periods - {self.config.model_name}")
        print("=" * 50)
        
        # Parse period type
        type_mapping = {
            '1min': PeriodType.ONE_MINUTE,
            '5min': PeriodType.FIVE_MINUTES,
            '1hr': PeriodType.ONE_HOUR, 
            '4hr': PeriodType.FOUR_HOURS,
            '1day': PeriodType.ONE_DAY
        }
        
        if period_type not in type_mapping:
            print(f"❌ Unknown period type: {period_type}")
            print(f"   Available types: {', '.join(type_mapping.keys())}")
            return
        
        parsed_type = type_mapping[period_type]
        
        # Calculate time range
        try:
            if start_date and end_date:
                start_timestamp = int(datetime.fromisoformat(start_date).timestamp())
                end_timestamp = int(datetime.fromisoformat(end_date).timestamp())
                print(f"📅 Date range: {start_date} to {end_date}")
            elif days:
                end_timestamp = int(datetime.now().timestamp())
                start_timestamp = end_timestamp - (days * 24 * 3600)
                print(f"📅 Last {days} days")
            else:
                print(f"❌ Must specify either --days or both --start-date and --end-date")
                return
            
            # Run backfill
            periods_created = self.pricing_service.backfill_missing_periods(
                parsed_type, start_timestamp, end_timestamp
            )
            
            print(f"\n✅ Backfill Results:")
            print(f"   📈 Periods created: {periods_created:,}")
            
        except Exception as e:
            print(f"❌ Backfill failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Backfill failed",
                period_type=period_type,
                error=str(e)
            )
    
    def show_status(self) -> None:
        """Show periods status"""
        print(f"📊 Periods Status - {self.config.model_name}")
        print("=" * 50)
        
        try:
            with self.repository_manager.get_session() as session:
                # Get stats for each period type
                for period_type in PeriodType:
                    periods = session.query(Period).filter(
                        Period.period_type == period_type
                    ).order_by(Period.time_open).all()
                    
                    if not periods:
                        print(f"\n🔍 {period_type.display_name}:")
                        print(f"   No periods found")
                        continue
                    
                    first_period = periods[0]
                    last_period = periods[-1]
                    complete_count = sum(1 for p in periods if p.is_complete)
                    
                    print(f"\n🔍 {period_type.display_name}:")
                    print(f"   Total periods: {len(periods):,}")
                    print(f"   Complete periods: {complete_count:,}")
                    print(f"   Incomplete periods: {len(periods) - complete_count:,}")
                    print(f"   First period: {first_period.period_label}")
                    print(f"   Last period: {last_period.period_label}")
                    print(f"   Block range: {first_period.block_open:,} → {last_period.block_close:,}")
                
                # Show latest blockchain state
                latest_block = self.rpc_client.get_latest_block_number()
                print(f"\n🔗 Blockchain State:")
                print(f"   Latest block: {latest_block:,}")
                
                # Show price data stats
                from ..database.repositories.block_prices_repository import BlockPricesRepository
                price_repo = BlockPricesRepository(self.repository_manager.db_manager)
                price_stats = price_repo.get_price_stats(session)
                
                print(f"\n💰 Price Data:")
                print(f"   Total price records: {price_stats['total_records']:,}")
                if price_stats['earliest_block']:
                    print(f"   Block range: {price_stats['earliest_block']:,} → {price_stats['latest_block']:,}")
                if price_stats['avg_price_usd']:
                    print(f"   Price range: ${price_stats['min_price_usd']:.2f} - ${price_stats['max_price_usd']:.2f}")
                    print(f"   Average price: ${price_stats['avg_price_usd']:.2f}")
                
        except Exception as e:
            print(f"❌ Status check failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Status check failed",
                error=str(e)
            )


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Pricing Service CLI")
    parser.add_argument("--model", help="Model name (overrides environment)")
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Update periods command
    update_parser = subparsers.add_parser('update-periods', help='Update periods to present')
    update_parser.add_argument(
        '--types', 
        help='Comma-separated period types (1min,5min,1hr,4hr,1day)'
    )
    
    # Update minute prices command
    subparsers.add_parser('update-prices', help='Update minute-by-minute AVAX prices')
    
    # Update all command
    subparsers.add_parser('update-all', help='Update both periods and prices')
    
    # Backfill command
    backfill_parser = subparsers.add_parser('backfill', help='Backfill missing periods')
    backfill_parser.add_argument('--type', required=True, help='Period type to backfill')
    backfill_parser.add_argument('--days', type=int, default=7, help='Days to backfill (default: 7)')
    backfill_parser.add_argument('--start-date', help='Start date (YYYY-MM-DD)')
    backfill_parser.add_argument('--end-date', help='End date (YYYY-MM-DD)')
    
    # Status command
    subparsers.add_parser('status', help='Show periods and pricing status')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        # Initialize runner
        print(f"🚀 Initializing pricing service...")
        runner = PricingServiceRunner(model_name=args.model)
        
        # Execute command
        if args.command == 'update-periods':
            period_types = None
            if args.types:
                period_types = [t.strip() for t in args.types.split(',')]
            runner.update_periods(period_types)
            
        elif args.command == 'update-prices':
            runner.update_minute_prices()
            
        elif args.command == 'update-all':
            runner.update_all()
            
        elif args.command == 'backfill':
            runner.backfill_periods(
                period_type=args.type,
                days=args.days,
                start_date=args.start_date,
                end_date=args.end_date
            )
            
        elif args.command == 'status':
            runner.show_status()
        
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()