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
from indexer.database.connection import InfrastructureDatabaseManager
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.services.pricing_service import PricingService
from indexer.database.shared.tables.periods import Period, PeriodType
from indexer.database.connection import ModelDatabaseManager
from indexer.database.repository import RepositoryManager

import logging


class PricingServiceRunner:
    """CLI runner for pricing service operations"""
    
    def __init__(self, model_name: Optional[str] = None):
        # Initialize indexer with DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services from container - using shared database for pricing service
        self.infrastructure_db_manager = self.container.get(InfrastructureDatabaseManager)
        self.rpc_client = self.container.get(QuickNodeRpcClient)
        
        # Get indexer database manager and repositories
        self.indexer_db_manager = self.container.get(ModelDatabaseManager)
        self.repository_manager = self.container.get(RepositoryManager)

        # Create pricing service with shared database manager
        self.pricing_service = PricingService(
            shared_db_manager=self.infrastructure_db_manager,
            rpc_client=self.rpc_client
        )
        
        self.logger = IndexerLogger.get_logger('services.pricing_service_runner')
        
        log_with_context(
            self.logger, logging.INFO, "PricingServiceRunner initialized",
            model_name=self.config.model_name,
            shared_database=self.infrastructure_db_manager.config.url.split('/')[-1]
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
            with self.infrastructure_db_manager.get_session() as session:
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
                from indexer.database.shared.repositories.block_prices_repository import BlockPricesRepository
                price_repo = BlockPricesRepository(self.infrastructure_db_manager)
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

    def update_swap_pricing(self, limit: int = 1000) -> None:
        """Update pricing for swaps that don't have detail records yet"""
        print(f"🔄 Updating swap pricing - {self.config.model_name}")
        print(f"📊 Processing up to {limit} swaps")
        print("=" * 50)
        
        try:
            # Get model ID for this model
            model_id = self._get_model_id()
            if not model_id:
                print(f"❌ Model '{self.config.model_name}' not found")
                return
            
            # Get repositories we need
            indexer_db_manager = self.container.get(ModelDatabaseManager)  
            pool_swap_repo = self.container.repository_manager.pool_swaps
            pool_swap_details_repo = self.container.repository_manager.pool_swap_details
            
            from indexer.database.shared.repositories.pool_pricing_config_repository import PoolPricingConfigRepository
            pool_pricing_config_repo = PoolPricingConfigRepository(self.infrastructure_db_manager)
            
            # Run the batch swap pricing
            with indexer_db_manager.get_session() as indexer_session:
                with self.infrastructure_db_manager.get_session() as shared_session:
                    
                    stats = self.pricing_service.calculate_missing_swap_pricing(
                        indexer_session=indexer_session,
                        shared_session=shared_session,
                        pool_swap_repo=pool_swap_repo,
                        pool_swap_details_repo=pool_swap_details_repo,
                        pool_pricing_config_repo=pool_pricing_config_repo,
                        model_id=model_id,
                        limit=limit
                    )
                    
                    # Commit the changes
                    indexer_session.commit()
                    
            # Display results
            print(f"\n📈 Swap Pricing Results:")
            print(f"   Processed: {stats['processed']:,} swaps")
            print(f"   Success: {stats['success']:,} swaps")
            print(f"   Failed: {stats['failed']:,} swaps")
            print(f"   Skipped: {stats['skipped']:,} swaps")
            
            if stats['errors']:
                print(f"\n❌ Errors ({len(stats['errors'])}):")
                for error in stats['errors'][:5]:  # Show first 5 errors
                    print(f"   • {error}")
                if len(stats['errors']) > 5:
                    print(f"   ... and {len(stats['errors']) - 5} more errors")
            
            if stats['success'] > 0:
                print(f"\n✅ Successfully priced {stats['success']:,} swaps")
            
            log_with_context(
                self.logger, logging.INFO, "Swap pricing update completed",
                **{k: v for k, v in stats.items() if k != 'errors'},
                error_count=len(stats['errors'])
            )
            
        except Exception as e:
            print(f"❌ Swap pricing update failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Swap pricing update failed",
                error=str(e)
            )


    def update_all_pricing(self) -> None:
        """Update all pricing components: periods, block prices, and swap pricing"""
        print(f"🔄 Full pricing update - {self.config.model_name}")
        print("=" * 60)
        
        try:
            # 1. Update periods first
            print("1️⃣ Updating periods...")
            self.update_periods()
            
            print("\n" + "=" * 60)
            
            # 2. Update minute prices  
            print("2️⃣ Updating minute prices...")
            self.update_minute_prices()
            
            print("\n" + "=" * 60)
            
            # 3. Update swap pricing
            print("3️⃣ Updating swap pricing...")
            self.update_swap_pricing()
            
            print("\n" + "=" * 60)
            print("✅ Full pricing update completed!")
            
        except Exception as e:
            print(f"❌ Full pricing update failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Full pricing update failed",
                error=str(e)
            )


    def show_pricing_status(self) -> None:
        """Show comprehensive pricing status including swap pricing details"""
        print(f"📊 Comprehensive Pricing Status - {self.config.model_name}")
        print("=" * 60)
        
        try:
            # Show existing period and price status
            self.show_status()
            
            # Add swap pricing status
            print("\n" + "=" * 60)
            print("🔄 Swap Pricing Status:")
            
            model_id = self._get_model_id()
            if not model_id:
                print("❌ Model not found - cannot show swap pricing status")
                return
            
            indexer_db_manager = self.container.get(ModelDatabaseManager)
            pool_swap_details_repo = self.container.repository_manager.pool_swap_details
            pool_swap_repo = self.container.repository_manager.pool_swaps
            
            with indexer_db_manager.get_session() as session:
                # Get recent swap counts
                recent_swaps = pool_swap_repo.get_recent(session, limit=5000)
                total_recent_swaps = len(recent_swaps)
                
                if total_recent_swaps == 0:
                    print("   No recent swaps found")
                    return
                
                # Check pricing coverage
                swap_content_ids = [swap.content_id for swap in recent_swaps]
                
                from indexer.database.indexer.tables.detail.pool_swap_detail import PricingDenomination
                missing_usd_ids = pool_swap_details_repo.get_missing_valuations(
                    session, swap_content_ids, PricingDenomination.USD
                )
                missing_avax_ids = pool_swap_details_repo.get_missing_valuations(
                    session, swap_content_ids, PricingDenomination.AVAX
                )
                
                usd_coverage = ((total_recent_swaps - len(missing_usd_ids)) / total_recent_swaps) * 100
                avax_coverage = ((total_recent_swaps - len(missing_avax_ids)) / total_recent_swaps) * 100
                
                print(f"   Recent swaps (last 5000): {total_recent_swaps:,}")
                print(f"   USD pricing coverage: {usd_coverage:.1f}% ({total_recent_swaps - len(missing_usd_ids):,}/{total_recent_swaps:,})")
                print(f"   AVAX pricing coverage: {avax_coverage:.1f}% ({total_recent_swaps - len(missing_avax_ids):,}/{total_recent_swaps:,})")
                
                if missing_usd_ids:
                    print(f"   Missing USD pricing: {len(missing_usd_ids):,} swaps")
                if missing_avax_ids:
                    print(f"   Missing AVAX pricing: {len(missing_avax_ids):,} swaps")
                
                # Get pricing method statistics
                pricing_stats = pool_swap_details_repo.get_pricing_method_stats(session)
                if pricing_stats:
                    print(f"\n   Pricing method breakdown:")
                    for method, count in pricing_stats.items():
                        print(f"     {method}: {count:,} records")
            
        except Exception as e:
            print(f"❌ Status check failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Pricing status check failed",
                error=str(e)
            )


    def validate_swap_pricing(self, sample_size: int = 1000) -> None:
        """Validate swap pricing accuracy and data quality"""
        print(f"🔍 Validating swap pricing - {self.config.model_name}")
        print(f"📊 Sample size: {sample_size:,}")
        print("=" * 50)
        
        try:
            model_id = self._get_model_id()
            if not model_id:
                print("❌ Model not found - cannot validate")
                return
            
            indexer_db_manager = self.container.get(ModelDatabaseManager)
            pool_swap_details_repo = self.container.repository_manager.pool_swap_details
            
            with indexer_db_manager.get_session() as session:
                # Get sample of USD and AVAX pricing details
                from indexer.database.indexer.tables.detail.pool_swap_detail import PricingDenomination
                
                usd_details = pool_swap_details_repo.get_usd_valuations(session, limit=sample_size)
                avax_details = pool_swap_details_repo.get_avax_valuations(session, limit=sample_size)
                
                print(f"💰 USD Pricing Analysis ({len(usd_details):,} records):")
                if usd_details:
                    usd_values = [float(d.value) for d in usd_details]
                    usd_prices = [float(d.price) for d in usd_details]
                    
                    print(f"   Value range: ${min(usd_values):,.2f} - ${max(usd_values):,.2f}")
                    print(f"   Average value: ${sum(usd_values) / len(usd_values):,.2f}")
                    print(f"   Price range: ${min(usd_prices):,.4f} - ${max(usd_prices):,.4f}")
                    print(f"   Average price: ${sum(usd_prices) / len(usd_prices):,.4f}")
                
                print(f"\n⚡ AVAX Pricing Analysis ({len(avax_details):,} records):")
                if avax_details:
                    avax_values = [float(d.value) for d in avax_details]
                    avax_prices = [float(d.price) for d in avax_details]
                    
                    print(f"   Value range: {min(avax_values):,.4f} - {max(avax_values):,.4f} AVAX")
                    print(f"   Average value: {sum(avax_values) / len(avax_values):,.4f} AVAX")
                    print(f"   Price range: {min(avax_prices):,.8f} - {max(avax_prices):,.8f} AVAX")
                    print(f"   Average price: {sum(avax_prices) / len(avax_prices):,.8f} AVAX")
                
                # Check for data quality issues
                print(f"\n🔍 Data Quality Checks:")
                
                # Check for zero values
                zero_usd = len([d for d in usd_details if float(d.value) == 0])
                zero_avax = len([d for d in avax_details if float(d.value) == 0])
                
                if zero_usd > 0:
                    print(f"   ⚠️  Zero USD values: {zero_usd:,} records")
                if zero_avax > 0:
                    print(f"   ⚠️  Zero AVAX values: {zero_avax:,} records")
                
                # Check for unrealistic values (basic sanity checks)
                high_usd = len([d for d in usd_details if float(d.value) > 1000000])  # > $1M
                high_avax = len([d for d in avax_details if float(d.value) > 10000])  # > 10k AVAX
                
                if high_usd > 0:
                    print(f"   💰 High USD values (>$1M): {high_usd:,} records")
                if high_avax > 0:
                    print(f"   ⚡ High AVAX values (>10k): {high_avax:,} records")
                
                if zero_usd == 0 and zero_avax == 0 and high_usd < sample_size * 0.1:
                    print(f"   ✅ Data quality looks good!")
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            log_with_context(
                self.logger, logging.ERROR, "Swap pricing validation failed",
                error=str(e)
            )


    def _get_model_id(self) -> Optional[int]:
        """Helper to get model ID for the current model name"""
        try:
            with self.infrastructure_db_manager.get_session() as session:
                from indexer.database.shared.tables.config import Model
                from sqlalchemy import and_
                
                model = session.query(Model).filter(
                    and_(
                        Model.name == self.config.model_name,
                        Model.status == 'active'
                    )
                ).first()
                
                return model.id if model else None
                
        except Exception as e:
            log_with_context(
                self.logger, logging.ERROR, "Error getting model ID",
                model_name=self.config.model_name,
                error=str(e)
            )
            return None

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

    # Update swap pricing command
    update_swaps_parser = subparsers.add_parser('update-swaps', help='Update swap pricing for missing valuations')
    update_swaps_parser.add_argument('--limit', type=int, default=1000, help='Maximum swaps to process')

    # Update the existing update-all command to include swap limit
    update_all_parser = subparsers.add_parser('update-all', help='Update periods, prices, and swap pricing')
    update_all_parser.add_argument('--swap-limit', type=int, default=1000, help='Maximum swaps to process')

    # Validate swaps command  
    validate_swaps_parser = subparsers.add_parser('validate-swaps', help='Validate swap pricing data quality')
    validate_swaps_parser.add_argument('--sample-size', type=int, default=1000, help='Sample size for validation')

    # Backfill swaps command
    backfill_swaps_parser = subparsers.add_parser('backfill-swaps', help='Backfill swap pricing')
    backfill_swaps_parser.add_argument('--days', type=int, default=7, help='Days to backfill')
    backfill_swaps_parser.add_argument('--limit', type=int, default=10000, help='Maximum swaps to process')

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
            runner.update_all_pricing()

        elif args.command == 'update-swaps':
            runner.update_swap_pricing(limit=args.limit)

        elif args.command == 'validate-swaps':
            runner.validate_swap_pricing(sample_size=args.sample_size)

        elif args.command == 'backfill-swaps':
            runner.update_swap_pricing(limit=args.limit)

        elif args.command == 'backfill':
            runner.backfill_periods(
                period_type=args.type,
                days=args.days,
                start_date=args.start_date,
                end_date=args.end_date
            )
            
        elif args.command == 'status':
            runner.show_pricing_status()
        
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()