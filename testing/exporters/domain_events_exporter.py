#!/usr/bin/env python3
"""
Domain Events Exporter

Exports domain event data to timestamped CSV files with pagination support.
Uses the indexer's dependency injection container for proper initialization.

Location: testing/exporters/domain_events_exporter.py
"""

import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd
from sqlalchemy import text
import math

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.core.logging import IndexerLogger, log_with_context
import logging


class DomainEventsExporter:
    """Export domain events with timestamped directories and pagination"""
    
    def __init__(self, model_name: str = None):
        """
        Initialize exporter with testing environment
        
        Args:
            model_name: Model name (e.g., 'blub_test') - uses env var if None
        """
        self.model_name = model_name
        self.logger = IndexerLogger.get_logger('testing.exporters.domain_events')
        
        # Create timestamped output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir = Path("testing/exports") / f"domain_events_{timestamp}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Domain event tables definition
        self.domain_event_tables = {
            'trades': {
                'description': 'Trade events - user trading activity',
                'key_columns': ['taker', 'direction', 'base_token', 'base_amount', 'trade_type', 'swap_count'],
                'relationships': ['Links to pool_swaps via content_id references']
            },
            'pool_swaps': {
                'description': 'Individual pool swap events within trades',
                'key_columns': ['pool', 'taker', 'direction', 'base_token', 'quote_token', 'trade_id'],
                'relationships': ['Can be linked to trades via trade_id']
            },
            'transfers': {
                'description': 'Token transfer events',
                'key_columns': ['token', 'from_address', 'to_address', 'amount'],
                'relationships': ['Can link to parent events via parent_id/parent_type']
            },
            'liquidity': {
                'description': 'Liquidity provision/removal events',
                'key_columns': ['pool', 'provider', 'action', 'base_token', 'quote_token'],
                'relationships': ['References pools and tokens']
            },
            'rewards': {
                'description': 'Reward distribution events (fees, farming rewards)',
                'key_columns': ['contract', 'recipient', 'token', 'amount', 'reward_type'],
                'relationships': ['Links to reward contracts and recipients']
            },
            'positions': {
                'description': 'Position changes - deposits, withdrawals, balance updates',
                'key_columns': ['user', 'token', 'amount', 'custodian'],
                'relationships': ['Can link to parent events via parent_id/parent_type']
            }
        }
        
        # Processing tables for context
        self.processing_tables = {
            'transaction_processing': {
                'description': 'Transaction processing status and metadata',
                'key_columns': ['tx_hash', 'block_number', 'status', 'events_generated']
            },
            'block_processing': {
                'description': 'Block processing status',
                'key_columns': ['block_number', 'status', 'tx_count']
            },
            'processing_jobs': {
                'description': 'Batch processing job queue',
                'key_columns': ['job_type', 'status', 'priority', 'job_data']
            }
        }
        
        # Initialize testing environment
        self._initialize_environment()
        
    def _initialize_environment(self):
        """Initialize the testing environment and get required services"""
        try:
            log_with_context(self.logger, logging.INFO, "Initializing testing environment",
                           model_name=self.model_name)
            
            # Use testing environment for proper DI setup
            self.env = get_testing_environment(model_name=self.model_name)
            self.config = self.env.get_config()
            
            # Get database services
            from indexer.database.repository_manager import RepositoryManager
            from indexer.database.connection import ModelDatabaseManager
            self.repository_manager = self.env.get_service(RepositoryManager)
            self.model_db = self.env.get_service(ModelDatabaseManager)
            
            log_with_context(self.logger, logging.INFO, "Testing environment initialized successfully",
                           model_name=self.config.model_name,
                           model_version=self.config.model_version,
                           output_dir=str(self.output_dir))
            
            print(f"✅ Initialized for model: {self.config.model_name} v{self.config.model_version}")
            print(f"📊 Database: {self.config.model_db_name}")
            print(f"📁 Output directory: {self.output_dir}")
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to initialize testing environment",
                           error=str(e), model_name=self.model_name)
            raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get table metadata and statistics"""
        try:
            with self.model_db.get_session() as session:
                # Check if table exists
                result = session.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                """))
                exists = result.scalar() > 0
                
                if not exists:
                    return {'exists': False, 'row_count': 0, 'columns': []}
                
                # Get column information
                result = session.execute(text(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """))
                columns = [dict(row._mapping) for row in result]
                
                # Get row count
                result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                row_count = result.scalar()
                
                # Get date range if timestamp column exists
                date_range = None
                if any(col['column_name'] == 'timestamp' for col in columns):
                    result = session.execute(text(f"""
                        SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts 
                        FROM {table_name}
                        WHERE timestamp IS NOT NULL
                    """))
                    range_row = result.fetchone()
                    if range_row and range_row[0]:
                        date_range = {'min': range_row[0], 'max': range_row[1]}
                
                return {
                    'exists': True,
                    'row_count': row_count,
                    'columns': columns,
                    'date_range': date_range
                }
                
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to get table info",
                           table_name=table_name, error=str(e))
            return {'exists': False, 'row_count': 0, 'columns': []}
    
    def export_table(self, table_name: str, limit: Optional[int] = None) -> bool:
        """Export a table to a single CSV file with optional row limit"""
        try:
            table_info = self.get_table_info(table_name)
            
            if not table_info['exists']:
                print(f"⚠️  Table {table_name} does not exist in {self.config.model_db_name}")
                return False
            
            total_rows = table_info['row_count']
            rows_to_export = min(total_rows, limit) if limit else total_rows
            
            print(f"📊 Exporting {table_name} ({total_rows:,} total rows, {rows_to_export:,} to export)...")
            
            if total_rows == 0:
                print(f"   ℹ️  No data in {table_name}")
                # Create empty CSV with headers
                columns = [col['column_name'] for col in table_info['columns']]
                csv_path = self.output_dir / f"{table_name}.csv"
                pd.DataFrame(columns=columns).to_csv(csv_path, index=False)
                return True
            
            # Build query with smart ordering
            order_clause = "ORDER BY block_number DESC, timestamp DESC"
            if not any(col['column_name'] == 'block_number' for col in table_info['columns']):
                if any(col['column_name'] == 'timestamp' for col in table_info['columns']):
                    order_clause = "ORDER BY timestamp DESC"
                elif any(col['column_name'] == 'created_at' for col in table_info['columns']):
                    order_clause = "ORDER BY created_at DESC"
                elif any(col['column_name'] == 'id' for col in table_info['columns']):
                    order_clause = "ORDER BY id DESC"
                else:
                    order_clause = ""
            
            query = f"SELECT * FROM {table_name} {order_clause}"
            if limit:
                query += f" LIMIT {limit}"
            
            # Export data
            with self.model_db.get_session() as session:
                df = pd.read_sql(query, session.bind)
            
            # Save to single CSV file
            csv_path = self.output_dir / f"{table_name}.csv"
            df.to_csv(csv_path, index=False)
            
            print(f"   ✅ Exported {len(df):,} rows to {csv_path.name}")
            
            # Show summary information
            if len(df) > 0:
                print(f"   📋 Columns ({len(df.columns)}): {', '.join(df.columns[:6])}")
                if len(df.columns) > 6:
                    print(f"       ... and {len(df.columns) - 6} more")
                
                # Show date range
                if table_info['date_range']:
                    dr = table_info['date_range']
                    print(f"   📅 Time range: {dr['min']} to {dr['max']}")
                
                # Show sample key data
                if table_name in self.domain_event_tables:
                    config = self.domain_event_tables[table_name]
                    key_cols = [col for col in config['key_columns'] if col in df.columns]
                    if key_cols and len(df) > 0:
                        print(f"   🔑 Sample data from latest records:")
                        for i, (_, row) in enumerate(df.head(2).iterrows()):
                            sample = {col: str(row[col])[:20] + ('...' if len(str(row[col])) > 20 else '') 
                                    for col in key_cols[:4]}
                            print(f"      Row {i+1}: {sample}")
            
            return True
            
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to export table",
                           table_name=table_name, error=str(e))
            print(f"❌ Failed to export {table_name}: {e}")
            return False
    
    def export_all_domain_events(self, limit_per_table: Optional[int] = None):
        """Export all domain event tables with optional row limit"""
        print(f"🚀 Exporting domain events from model: {self.config.model_name}")
        print(f"   Database: {self.config.model_db_name}")
        print(f"   Output directory: {self.output_dir}")
        print(f"   Limit per table: {limit_per_table or 'No limit'}")
        print()
        
        successful_exports = 0
        total_tables = len(self.domain_event_tables)
        
        for table_name, config in self.domain_event_tables.items():
            print(f"📋 {config['description']}")
            success = self.export_table(table_name, limit_per_table)
            if success:
                successful_exports += 1
            print()
        
        print(f"✅ Domain events export complete: {successful_exports}/{total_tables} tables")
        return successful_exports == total_tables
    
    def export_processing_context(self, limit_per_table: Optional[int] = 5000):
        """Export processing tables for context"""
        print(f"📊 Exporting processing context tables...")
        
        for table_name, config in self.processing_tables.items():
            print(f"🔧 {config['description']}")
            self.export_table(table_name, limit_per_table)
            print()
    
    def run_analysis_queries(self):
        """Run analysis queries and save results"""
        print(f"🔍 Running analysis queries...")
        
        analysis_queries = [
            {
                'name': 'domain_events_summary',
                'description': 'Summary statistics for all domain event tables',
                'query': '''
                    SELECT 
                        'trades' as table_name, 
                        COUNT(*) as total_rows, 
                        COUNT(DISTINCT taker) as unique_users,
                        MIN(timestamp) as earliest, 
                        MAX(timestamp) as latest
                    FROM trades
                    WHERE trades.id IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT 'pool_swaps', COUNT(*), COUNT(DISTINCT taker),
                           MIN(timestamp), MAX(timestamp)
                    FROM pool_swaps
                    WHERE pool_swaps.id IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT 'transfers', COUNT(*), COUNT(DISTINCT from_address),
                           MIN(timestamp), MAX(timestamp)
                    FROM transfers
                    WHERE transfers.id IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT 'liquidity', COUNT(*), COUNT(DISTINCT provider),
                           MIN(timestamp), MAX(timestamp)
                    FROM liquidity
                    WHERE liquidity.id IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT 'rewards', COUNT(*), COUNT(DISTINCT recipient),
                           MIN(timestamp), MAX(timestamp)
                    FROM rewards
                    WHERE rewards.id IS NOT NULL
                    
                    UNION ALL
                    
                    SELECT 'positions', COUNT(*), COUNT(DISTINCT "user"),
                           MIN(timestamp), MAX(timestamp)
                    FROM positions
                    WHERE positions.id IS NOT NULL
                    
                    ORDER BY total_rows DESC
                '''
            },
            {
                'name': 'processing_status',
                'description': 'Processing status overview',
                'query': '''
                    SELECT 
                        status,
                        COUNT(*) as transaction_count,
                        AVG(events_generated) as avg_events_per_tx,
                        MIN(block_number) as min_block,
                        MAX(block_number) as max_block
                    FROM transaction_processing
                    GROUP BY status
                    ORDER BY transaction_count DESC
                '''
            },
            {
                'name': 'trade_swap_consistency',
                'description': 'Trade to pool_swap consistency check',
                'query': '''
                    SELECT 
                        t.content_id as trade_id,
                        t.swap_count as expected_swaps,
                        COUNT(ps.content_id) as actual_swaps,
                        CASE 
                            WHEN t.swap_count = COUNT(ps.content_id) THEN 'CONSISTENT'
                            ELSE 'INCONSISTENT'
                        END as status
                    FROM trades t
                    LEFT JOIN pool_swaps ps ON ps.trade_id = t.content_id
                    GROUP BY t.content_id, t.swap_count
                    ORDER BY status DESC, t.content_id
                '''
            }
        ]
        
        for query_config in analysis_queries:
            try:
                print(f"   📈 {query_config['description']}")
                
                with self.model_db.get_session() as session:
                    df = pd.read_sql(query_config['query'], session.bind)
                
                if not df.empty:
                    csv_path = self.output_dir / f"analysis_{query_config['name']}.csv"
                    df.to_csv(csv_path, index=False)
                    print(f"      ✅ Saved {len(df)} rows to {csv_path.name}")
                    
                    # Show preview of key results
                    if len(df) > 0:
                        if query_config['name'] == 'domain_events_summary':
                            print(f"      📋 Summary:")
                            for _, row in df.head(6).iterrows():
                                print(f"         {row['table_name']}: {row['total_rows']:,} rows, {row['unique_users']} unique users")
                        elif query_config['name'] == 'trade_swap_consistency':
                            inconsistent = df[df['status'] == 'INCONSISTENT']
                            if len(inconsistent) > 0:
                                print(f"      ⚠️  Found {len(inconsistent)} inconsistent trades!")
                            else:
                                print(f"      ✅ All {len(df)} trades are consistent with their swaps")
                else:
                    print(f"      ℹ️  No data returned")
                    
            except Exception as e:
                print(f"      ⚠️  Query failed: {e}")
        
        print()
    
    def generate_export_report(self):
        """Generate comprehensive export report"""
        try:
            report_path = self.output_dir / "export_report.md"
            
            with open(report_path, 'w') as f:
                f.write(f"# Domain Events Export Report\n\n")
                f.write(f"**Model:** {self.config.model_name} v{self.config.model_version}\n")
                f.write(f"**Database:** {self.config.model_db_name}\n")
                f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"**Export Directory:** {self.output_dir}\n")
                f.write(f"**Max Rows Per File:** {self.max_rows_per_file:,}\n\n")
                
                f.write("## Domain Event Tables\n\n")
                
                for table_name, config in self.domain_event_tables.items():
                    f.write(f"### {table_name.title()}\n")
                    f.write(f"**Description:** {config['description']}\n\n")
                    
                    # Find the file for this table
                    csv_path = self.output_dir / f"{table_name}.csv"
                    if csv_path.exists():
                        df = pd.read_csv(csv_path)
                        
                        f.write(f"- **Rows exported:** {len(df):,}\n")
                        f.write(f"- **Columns:** {len(df.columns)}\n")
                        f.write(f"- **File:** `{csv_path.name}`\n")
                        f.write(f"- **Key columns:** {', '.join(config['key_columns'])}\n")
                        f.write(f"- **Relationships:** {', '.join(config['relationships'])}\n")
                        
                        # Check for timestamp info
                        if 'timestamp' in df.columns and len(df) > 0:
                            min_ts = df['timestamp'].min()
                            max_ts = df['timestamp'].max()
                            f.write(f"- **Time range:** {min_ts} to {max_ts}\n")
                    else:
                        f.write("- **Status:** No data exported\n")
                    
                    f.write("\n")
                
                f.write("## Files Generated\n\n")
                csv_files = sorted(self.output_dir.glob("*.csv"))
                for file_path in csv_files:
                    df = pd.read_csv(file_path)
                    f.write(f"- `{file_path.name}` ({len(df):,} rows)\n")
                
                f.write("\n## Data Integrity Notes\n\n")
                f.write("- All timestamps are Unix timestamps (seconds since epoch)\n")
                f.write("- Amounts are stored as strings to preserve precision\n")
                f.write("- Address fields use checksummed Ethereum addresses\n")
                f.write("- Enum fields (direction, action, etc.) use lowercase values\n")
                f.write("- Files are ordered by most recent records first\n")
                
            print(f"📄 Export report saved to {report_path}")
            
        except Exception as e:
            print(f"⚠️  Could not generate export report: {e}")


def main():
    """Main execution function"""
    if len(sys.argv) < 2:
        print("Usage: python domain_events_exporter.py <model_name> [limit_per_table]")
        print("Examples:")
        print("  python domain_events_exporter.py blub_test")
        print("  python domain_events_exporter.py blub_test 1000")
        print("  python domain_events_exporter.py blub_test 5000")
        return 1
    
    model_name = sys.argv[1]
    limit_per_table = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    print("🚀 Domain Events Exporter")
    print("=" * 60)
    print(f"Model: {model_name}")
    print(f"Limit per table: {limit_per_table or 'No limit'}")
    print()
    
    try:
        # Create exporter using testing environment
        exporter = DomainEventsExporter(model_name=model_name)
        
        # Export domain events
        success = exporter.export_all_domain_events(limit_per_table)
        
        # Export processing context
        exporter.export_processing_context(limit_per_table=5000)
        
        # Run analysis queries
        exporter.run_analysis_queries()
        
        # Generate report
        exporter.generate_export_report()
        
        if success:
            print("🎉 Export completed successfully!")
            print(f"📁 Output directory: {exporter.output_dir}")
            print(f"📄 See export_report.md for detailed information")
            return 0
        else:
            print("⚠️  Some tables failed to export")
            return 1
            
    except Exception as e:
        print(f"❌ Export failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())