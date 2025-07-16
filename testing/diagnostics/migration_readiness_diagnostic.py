#!/usr/bin/env python3
# testing/diagnostics/migration_readiness_diagnostic.py
"""
Migration Readiness Diagnostic

Comprehensive check of current database state to understand what needs to be migrated
for the pricing service implementation. This script will help us understand:
1. What tables exist vs what we expect
2. What data is currently in the database
3. What schema changes are needed
4. How to handle existing data during migration
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime
import json

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from sqlalchemy import text, inspect, MetaData
from sqlalchemy.engine import Engine
from indexer.database.connection import ModelDatabaseManager, InfrastructureDatabaseManager


class MigrationReadinessDiagnostic:
    """Check database state and migration readiness."""
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name
        self.env = get_testing_environment(model_name=model_name)
        self.config = self.env.get_config()
        
        # Expected schemas based on our current code
        self.expected_shared_tables = {
            'models', 'contracts', 'tokens', 'sources', 'addresses',
            'model_contracts', 'model_tokens', 'model_sources',
            'periods', 'block_prices', 'price_vwap', 'pool_pricing_configs'
        }
        
        self.expected_model_tables = {
            'transaction_processing', 'block_processing', 'processing_jobs',
            'trades', 'pool_swaps', 'transfers', 'positions', 'liquidity', 'rewards',
            'pool_swap_details', 'trade_details', 'asset_price', 'asset_volume'
        }
        
        # Tables that are new/critical for pricing service
        self.pricing_service_tables = {
            'price_vwap',           # Canonical pricing authority
            'pool_swap_details',    # Direct pricing details
            'trade_details',        # Trade valuations
            'asset_price',          # OHLC candles
            'asset_volume',         # Volume metrics
            'pool_pricing_configs'  # Pool pricing configuration
        }
        
        self.results = {
            'shared_database': {
                'connection': None,
                'existing_tables': set(),
                'missing_tables': set(),
                'table_details': {},
                'data_counts': {}
            },
            'model_database': {
                'connection': None,
                'existing_tables': set(),
                'missing_tables': set(),
                'table_details': {},
                'data_counts': {}
            },
            'migration_requirements': {
                'new_tables_needed': set(),
                'existing_data_impact': {},
                'schema_changes_needed': [],
                'data_migration_needed': []
            }
        }
        
    def run(self) -> bool:
        """Run comprehensive migration readiness check."""
        print("🔍 Migration Readiness Diagnostic")
        print("=" * 80)
        print(f"Model: {self.config.model_name}")
        print(f"Checking readiness for pricing service migration...")
        print()
        
        # Check both databases
        self._check_shared_database()
        self._check_model_database()
        
        # Analyze migration requirements
        self._analyze_migration_requirements()
        
        # Generate migration strategy
        self._generate_migration_strategy()
        
        # Print summary
        self._print_summary()
        
        return True
    
    def _check_shared_database(self):
        """Check shared database state."""
        print("📚 Checking Shared Database (indexer_shared)")
        print("-" * 50)
        
        try:
            db_manager = self.env.get_service(InfrastructureDatabaseManager)
            engine = db_manager.engine
            
            # Test connection
            with db_manager.get_session() as session:
                session.execute(text("SELECT 1"))
            
            self.results['shared_database']['connection'] = True
            print("✅ Connection: OK")
            
            # Get existing tables
            inspector = inspect(engine)
            existing_tables = set(inspector.get_table_names())
            self.results['shared_database']['existing_tables'] = existing_tables
            
            # Find missing tables
            missing_tables = self.expected_shared_tables - existing_tables
            self.results['shared_database']['missing_tables'] = missing_tables
            
            print(f"📋 Tables found: {len(existing_tables)}")
            print(f"📋 Tables expected: {len(self.expected_shared_tables)}")
            print(f"📋 Tables missing: {len(missing_tables)}")
            
            if missing_tables:
                print(f"❌ Missing tables: {', '.join(sorted(missing_tables))}")
            
            # Check table details and data counts
            self._check_table_details(db_manager, existing_tables, 'shared_database')
            
        except Exception as e:
            print(f"❌ Shared database error: {e}")
            self.results['shared_database']['connection'] = False
        
        print()
    
    def _check_model_database(self):
        """Check model database state."""
        print(f"🗃️ Checking Model Database ({self.config.model_db_name})")
        print("-" * 50)
        
        try:
            db_manager = self.env.get_service(ModelDatabaseManager)
            engine = db_manager.engine
            
            # Test connection
            with db_manager.get_session() as session:
                session.execute(text("SELECT 1"))
            
            self.results['model_database']['connection'] = True
            print("✅ Connection: OK")
            
            # Get existing tables
            inspector = inspect(engine)
            existing_tables = set(inspector.get_table_names())
            self.results['model_database']['existing_tables'] = existing_tables
            
            # Find missing tables
            missing_tables = self.expected_model_tables - existing_tables
            self.results['model_database']['missing_tables'] = missing_tables
            
            print(f"📋 Tables found: {len(existing_tables)}")
            print(f"📋 Tables expected: {len(self.expected_model_tables)}")
            print(f"📋 Tables missing: {len(missing_tables)}")
            
            if missing_tables:
                print(f"❌ Missing tables: {', '.join(sorted(missing_tables))}")
            
            # Check table details and data counts
            self._check_table_details(db_manager, existing_tables, 'model_database')
            
        except Exception as e:
            print(f"❌ Model database error: {e}")
            self.results['model_database']['connection'] = False
        
        print()
    
    def _check_table_details(self, db_manager, tables: Set[str], db_key: str):
        """Check detailed table information."""
        for table_name in sorted(tables):
            try:
                # Get table structure
                inspector = inspect(db_manager.engine)
                columns = inspector.get_columns(table_name)
                
                # Store column details
                self.results[db_key]['table_details'][table_name] = {
                    'columns': [
                        {
                            'name': col['name'],
                            'type': str(col['type']),
                            'nullable': col['nullable'],
                            'default': col.get('default')
                        }
                        for col in columns
                    ]
                }
                
                # Get row count
                with db_manager.get_session() as session:
                    result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    count = result.scalar()
                    self.results[db_key]['data_counts'][table_name] = count
                    
                    # Show table status
                    status = "📊" if count > 0 else "📋"
                    print(f"   {status} {table_name}: {count:,} rows")
                    
            except Exception as e:
                print(f"   ❌ {table_name}: Error - {e}")
                self.results[db_key]['data_counts'][table_name] = f"Error: {e}"
    
    def _analyze_migration_requirements(self):
        """Analyze what's needed for migration."""
        print("🔍 Analyzing Migration Requirements")
        print("-" * 50)
        
        # Check which pricing service tables are missing
        shared_missing = self.results['shared_database']['missing_tables']
        model_missing = self.results['model_database']['missing_tables']
        
        # Critical pricing service tables
        critical_shared = shared_missing & {'price_vwap', 'pool_pricing_configs'}
        critical_model = model_missing & {'pool_swap_details', 'trade_details', 'asset_price', 'asset_volume'}
        
        self.results['migration_requirements']['new_tables_needed'] = critical_shared | critical_model
        
        print(f"🆕 New tables needed: {len(critical_shared | critical_model)}")
        if critical_shared:
            print(f"   Shared DB: {', '.join(sorted(critical_shared))}")
        if critical_model:
            print(f"   Model DB: {', '.join(sorted(critical_model))}")
        
        # Check for existing data that might complicate migration
        self._check_existing_data_impact()
        
        print()
    
    def _check_existing_data_impact(self):
        """Check if existing data will impact migration."""
        print("\n📊 Checking Existing Data Impact")
        print("-" * 30)
        
        # Check shared database tables with data
        shared_counts = self.results['shared_database']['data_counts']
        model_counts = self.results['model_database']['data_counts']
        
        # Tables that might need NOT NULL columns added
        tables_with_data = {}
        
        for table, count in shared_counts.items():
            if isinstance(count, int) and count > 0:
                tables_with_data[f"shared.{table}"] = count
        
        for table, count in model_counts.items():
            if isinstance(count, int) and count > 0:
                tables_with_data[f"model.{table}"] = count
        
        if tables_with_data:
            print("⚠️  Tables with existing data:")
            for table, count in tables_with_data.items():
                print(f"   {table}: {count:,} rows")
            
            # Check if these tables need schema changes
            self._check_schema_changes_needed(tables_with_data)
        else:
            print("✅ No existing data - clean migration possible")
    
    def _check_schema_changes_needed(self, tables_with_data: Dict[str, int]):
        """Check if existing tables need schema changes."""
        
        # Tables that are likely to need timestamp columns added
        timestamp_additions = []
        
        for table_key, count in tables_with_data.items():
            db_type, table_name = table_key.split('.', 1)
            
            # Check if table has timestamp columns
            if db_type == 'shared':
                table_details = self.results['shared_database']['table_details'].get(table_name, {})
            else:
                table_details = self.results['model_database']['table_details'].get(table_name, {})
            
            if table_details:
                columns = {col['name'] for col in table_details['columns']}
                
                # Check for missing timestamp columns
                if 'created_at' not in columns or 'updated_at' not in columns:
                    timestamp_additions.append(table_key)
                
                # Check for specific fields that might be missing
                if table_name == 'addresses' and 'address_type' not in columns:
                    self.results['migration_requirements']['schema_changes_needed'].append(
                        f"addresses table missing address_type column (has {count} rows)"
                    )
                
                if table_name == 'contracts' and 'project' not in columns:
                    self.results['migration_requirements']['schema_changes_needed'].append(
                        f"contracts table missing project column (has {count} rows)"
                    )
        
        if timestamp_additions:
            self.results['migration_requirements']['schema_changes_needed'].extend(
                [f"{table} needs timestamp columns added" for table in timestamp_additions]
            )
    
    def _generate_migration_strategy(self):
        """Generate migration strategy recommendations."""
        print("📋 Migration Strategy Recommendations")
        print("-" * 50)
        
        shared_missing = self.results['shared_database']['missing_tables']
        model_missing = self.results['model_database']['missing_tables']
        schema_changes = self.results['migration_requirements']['schema_changes_needed']
        
        strategies = []
        
        # Strategy for new tables
        if shared_missing or model_missing:
            strategies.append("🆕 CREATE NEW TABLES")
            if shared_missing:
                strategies.append(f"   Shared DB: {', '.join(sorted(shared_missing))}")
            if model_missing:
                strategies.append(f"   Model DB: {', '.join(sorted(model_missing))}")
        
        # Strategy for existing table modifications
        if schema_changes:
            strategies.append("🔧 MODIFY EXISTING TABLES")
            for change in schema_changes:
                strategies.append(f"   {change}")
        
        # Strategy recommendations
        if not shared_missing and not model_missing and not schema_changes:
            strategies.append("✅ CLEAN MIGRATION - No schema changes needed")
        elif schema_changes:
            strategies.append("⚠️  CAREFUL MIGRATION - Handle existing data")
            strategies.append("   Recommend: Add columns as nullable first, then populate")
        
        for strategy in strategies:
            print(strategy)
        
        print()
    
    def _print_summary(self):
        """Print diagnostic summary."""
        print("📋 Migration Readiness Summary")
        print("=" * 80)
        
        # Database connectivity
        shared_ok = self.results['shared_database']['connection']
        model_ok = self.results['model_database']['connection']
        
        print(f"Database Connectivity:")
        print(f"  Shared DB: {'✅ OK' if shared_ok else '❌ FAILED'}")
        print(f"  Model DB:  {'✅ OK' if model_ok else '❌ FAILED'}")
        
        # Table status
        shared_missing = len(self.results['shared_database']['missing_tables'])
        model_missing = len(self.results['model_database']['missing_tables'])
        
        print(f"\nTable Status:")
        print(f"  Shared DB missing: {shared_missing}")
        print(f"  Model DB missing:  {model_missing}")
        
        # Migration complexity
        schema_changes = len(self.results['migration_requirements']['schema_changes_needed'])
        
        print(f"\nMigration Complexity:")
        if shared_missing == 0 and model_missing == 0 and schema_changes == 0:
            print("  ✅ LOW - No changes needed")
        elif schema_changes > 0:
            print("  ⚠️  MEDIUM - Schema changes to existing data")
        else:
            print("  🔧 LOW-MEDIUM - Only new tables needed")
        
        # Ready for migration?
        if shared_ok and model_ok:
            print(f"\n🎯 READY FOR MIGRATION: {'✅ YES' if shared_ok and model_ok else '❌ NO'}")
        else:
            print(f"\n🎯 READY FOR MIGRATION: ❌ NO - Fix database connections first")
        
        print()
        
        # Next steps
        print("Next Steps:")
        if not shared_ok or not model_ok:
            print("  1. Fix database connectivity issues")
        else:
            print("  1. Review migration strategy above")
            print("  2. Create migration with proper data handling")
            print("  3. Test migration on sample data")
        
        print("  4. Run: python -m indexer.cli migrate shared create 'Add pricing service tables'")
        print("  5. Review generated migration file")
        print("  6. Run: python -m indexer.cli migrate shared upgrade")


def main():
    """Run migration readiness diagnostic."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migration Readiness Diagnostic')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    parser.add_argument('--json', action='store_true', help='Output detailed results as JSON')
    args = parser.parse_args()
    
    try:
        diagnostic = MigrationReadinessDiagnostic(model_name=args.model)
        success = diagnostic.run()
        
        if args.json:
            print("\n" + "="*80)
            print("DETAILED RESULTS (JSON)")
            print("="*80)
            print(json.dumps(diagnostic.results, indent=2, default=str))
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 Migration readiness check failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()