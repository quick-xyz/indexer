#!/usr/bin/env python3
"""
Migration Diagnostic and Fix Script

Diagnoses migration issues and safely fixes them using proper DI container.
Can optionally reset the shared database while preserving indexer database.

Usage:
    python migration_fix.py --diagnose                    # Just check current state
    python migration_fix.py --fix-data                   # Fix data for migration
    python migration_fix.py --reset-shared               # Reset shared DB and migrate
    python migration_fix.py --full-migrate               # Complete migration process
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.database.connection import InfrastructureDatabaseManager, ModelDatabaseManager
from indexer.database.migration_manager import MigrationManager
from indexer.core.secrets_service import SecretsService
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


class MigrationDiagnosticTool:
    """
    Diagnostic and fix tool for database migrations using proper DI container
    """
    
    def __init__(self, model_name: str = None):
        """Initialize with testing environment and proper DI"""
        print("🔧 Initializing Migration Diagnostic Tool")
        print("=" * 60)
        
        # Initialize testing environment 
        self.testing_env = get_testing_environment(model_name=model_name, log_level="INFO")
        self.config = self.testing_env.get_config()
        
        # Get database managers from DI container
        self.shared_db_manager = self.testing_env.get_service(InfrastructureDatabaseManager)
        self.indexer_db_manager = self.testing_env.get_service(ModelDatabaseManager)
        
        # Get migration manager
        self.migration_manager = self._get_migration_manager()
        
        print(f"✅ Initialized for model: {self.config.model_name}")
        print(f"   Shared DB: {self.shared_db_manager.config.url.split('/')[-1]}")
        print(f"   Indexer DB: {self.indexer_db_manager.config.url.split('/')[-1]}")
        print()
    
    def _get_migration_manager(self) -> MigrationManager:
        """Get migration manager with proper DI setup"""
        try:
            secrets_service = self.testing_env.get_service(SecretsService)
            return MigrationManager(
                infrastructure_db_manager=self.shared_db_manager,
                secrets_service=secrets_service
            )
        except Exception as e:
            print(f"⚠️ Could not get migration manager via DI: {e}")
            print("   Creating directly...")
            secrets_service = self.testing_env.get_service(SecretsService)
            return MigrationManager(
                infrastructure_db_manager=self.shared_db_manager,
                secrets_service=secrets_service
            )
    
    def diagnose(self) -> Dict[str, any]:
        """Comprehensive diagnosis of current migration state"""
        print("🔍 Running Migration Diagnostics")
        print("-" * 40)
        
        results = {
            'shared_db_status': self._check_shared_db_status(),
            'indexer_db_status': self._check_indexer_db_status(),
            'migration_state': self._check_migration_state(),
            'data_issues': self._check_data_issues(),
            'target_migration': self._check_target_migration()
        }
        
        self._print_diagnosis_summary(results)
        return results
    
    def _check_shared_db_status(self) -> Dict[str, any]:
        """Check shared database connectivity and basic structure"""
        print("📊 Checking shared database...")
        
        try:
            with self.shared_db_manager.get_session() as session:
                # Check connectivity
                session.execute(text("SELECT 1"))
                
                # Check if tables exist
                table_check = session.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('contracts', 'tokens', 'models', 'price_vwap')
                """)).fetchall()
                
                existing_tables = [row[0] for row in table_check]
                
                # Check current revision
                try:
                    current_revision = self.migration_manager.get_shared_current_revision()
                except:
                    current_revision = None
                
                result = {
                    'connected': True,
                    'existing_tables': existing_tables,
                    'has_price_vwap': 'price_vwap' in existing_tables,
                    'current_revision': current_revision
                }
                
                print(f"   ✅ Connected, {len(existing_tables)} tables found")
                if 'price_vwap' in existing_tables:
                    print(f"   ✅ price_vwap table exists")
                else:
                    print(f"   ❌ price_vwap table missing")
                
                return result
                
        except Exception as e:
            print(f"   ❌ Shared database error: {e}")
            return {'connected': False, 'error': str(e)}
    
    def _check_indexer_db_status(self) -> Dict[str, any]:
        """Check indexer database and record counts"""
        print("🗄️ Checking indexer database...")
        
        try:
            with self.indexer_db_manager.get_session() as session:
                # Check connectivity
                session.execute(text("SELECT 1"))
                
                # Get record counts for important tables
                important_tables = ['pool_swaps', 'trades', 'transfers', 'pool_swap_details', 'trade_details']
                counts = {}
                
                for table in important_tables:
                    try:
                        count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        counts[table] = count
                    except:
                        counts[table] = None  # Table doesn't exist
                
                result = {
                    'connected': True,
                    'record_counts': counts,
                    'total_records': sum(c for c in counts.values() if c is not None)
                }
                
                print(f"   ✅ Connected, {result['total_records']} total records")
                for table, count in counts.items():
                    if count is not None:
                        print(f"      {table}: {count:,}")
                
                return result
                
        except Exception as e:
            print(f"   ❌ Indexer database error: {e}")
            return {'connected': False, 'error': str(e)}
    
    def _check_migration_state(self) -> Dict[str, any]:
        """Check current migration state"""
        print("🔄 Checking migration state...")
        
        try:
            current = self.migration_manager.get_shared_current_revision()
            target = "d5c49de1c76d"  # The migration we're trying to apply
            
            result = {
                'current_revision': current,
                'target_revision': target,
                'needs_migration': current != target,
                'is_ahead': False  # We'll check this
            }
            
            print(f"   Current: {current}")
            print(f"   Target:  {target}")
            print(f"   Status:  {'Needs migration' if result['needs_migration'] else 'Up to date'}")
            
            return result
            
        except Exception as e:
            print(f"   ❌ Migration state error: {e}")
            return {'error': str(e)}
    
    def _check_data_issues(self) -> Dict[str, any]:
        """Check for data issues that would cause migration failures"""
        print("🔍 Checking for data issues...")
        
        issues = []
        table_status = {}
        
        try:
            with self.shared_db_manager.get_session() as session:
                # Tables and columns that will become NOT NULL
                checks = [
                    ("addresses", "type", "address_type mapping"),
                    ("contracts", "status", "status field"),
                    ("contracts", "created_at", "timestamp field"),
                    ("contracts", "updated_at", "timestamp field"),
                    ("models", "status", "status field"),
                    ("models", "created_at", "timestamp field"),
                    ("models", "updated_at", "timestamp field"),
                    ("tokens", "name", "name field"),
                    ("tokens", "symbol", "symbol field"),
                    ("tokens", "decimals", "decimals field"),
                    ("tokens", "status", "status field"),
                    ("tokens", "created_at", "timestamp field"),
                    ("tokens", "updated_at", "timestamp field"),
                ]
                
                for table, column, description in checks:
                    try:
                        # Check if table exists
                        table_exists = session.execute(text(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = '{table}'
                            )
                        """)).scalar()
                        
                        if not table_exists:
                            table_status[table] = "missing"
                            continue
                        
                        # Check for NULL values
                        null_count = session.execute(text(f"""
                            SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
                        """)).scalar()
                        
                        total_count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        
                        table_status[f"{table}.{column}"] = {
                            'total_rows': total_count,
                            'null_count': null_count,
                            'has_nulls': null_count > 0
                        }
                        
                        if null_count > 0:
                            issues.append(f"{table}.{column}: {null_count}/{total_count} NULL values")
                        
                    except Exception as e:
                        issues.append(f"{table}.{column}: Check failed - {e}")
                
                # Check for manually added columns that might conflict
                conflicting_columns = []
                for table in ['addresses', 'block_prices', 'model_contracts', 'model_sources', 'model_tokens', 'periods', 'pool_pricing_configs', 'sources']:
                    try:
                        columns = session.execute(text(f"""
                            SELECT column_name FROM information_schema.columns 
                            WHERE table_name = '{table}' 
                            AND column_name IN ('address_type', 'created_at', 'updated_at', 'source_type')
                        """)).fetchall()
                        
                        if columns:
                            conflicting_columns.extend([f"{table}.{col[0]}" for col in columns])
                    except:
                        pass
                
                result = {
                    'issues': issues,
                    'table_status': table_status,
                    'conflicting_columns': conflicting_columns,
                    'has_issues': len(issues) > 0,
                    'has_conflicts': len(conflicting_columns) > 0
                }
                
                if issues:
                    print(f"   ❌ Found {len(issues)} data issues:")
                    for issue in issues[:5]:  # Show first 5
                        print(f"      {issue}")
                    if len(issues) > 5:
                        print(f"      ... and {len(issues) - 5} more")
                else:
                    print(f"   ✅ No data issues found")
                
                if conflicting_columns:
                    print(f"   ⚠️ Found {len(conflicting_columns)} potentially conflicting columns:")
                    for col in conflicting_columns[:5]:
                        print(f"      {col}")
                
                return result
                
        except Exception as e:
            print(f"   ❌ Data check error: {e}")
            return {'error': str(e)}
    
    def _check_target_migration(self) -> Dict[str, any]:
        """Analyze the target migration file"""
        print("📄 Checking target migration...")
        
        migration_file = Path("indexer/database/migrations/versions/d5c49de1c76d_update_schemas_for_pricing_service.py")
        
        if not migration_file.exists():
            print("   ❌ Migration file not found")
            return {'exists': False}
        
        # Read and analyze the migration
        with open(migration_file, 'r') as f:
            content = f.read()
        
        # Count operations
        add_columns = content.count('op.add_column')
        alter_columns = content.count('op.alter_column')
        create_tables = content.count('op.create_table')
        drop_columns = content.count('op.drop_column')
        
        result = {
            'exists': True,
            'file_size': len(content),
            'add_columns': add_columns,
            'alter_columns': alter_columns,
            'create_tables': create_tables,
            'drop_columns': drop_columns,
            'creates_price_vwap': 'price_vwap' in content
        }
        
        print(f"   ✅ Migration file found ({len(content)} chars)")
        print(f"      Creates {create_tables} tables, adds {add_columns} columns")
        print(f"      {'✅' if result['creates_price_vwap'] else '❌'} Creates price_vwap table")
        
        return result
    
    def _print_diagnosis_summary(self, results: Dict[str, any]):
        """Print comprehensive diagnosis summary"""
        print("\n📋 DIAGNOSIS SUMMARY")
        print("=" * 60)
        
        # Overall status
        shared_ok = results['shared_db_status'].get('connected', False)
        indexer_ok = results['indexer_db_status'].get('connected', False)
        has_data_issues = results['data_issues'].get('has_issues', False)
        has_conflicts = results['data_issues'].get('has_conflicts', False)
        needs_migration = results['migration_state'].get('needs_migration', False)
        
        print(f"🔗 Database Connectivity:")
        print(f"   Shared DB:  {'✅' if shared_ok else '❌'}")
        print(f"   Indexer DB: {'✅' if indexer_ok else '❌'}")
        
        print(f"\n🔄 Migration Status:")
        print(f"   Needs Migration: {'Yes' if needs_migration else 'No'}")
        print(f"   Current: {results['migration_state'].get('current_revision', 'Unknown')}")
        print(f"   Target:  {results['migration_state'].get('target_revision', 'Unknown')}")
        
        print(f"\n⚠️ Issues:")
        print(f"   Data Issues:     {'Yes' if has_data_issues else 'No'}")
        print(f"   Column Conflicts: {'Yes' if has_conflicts else 'No'}")
        
        if results['indexer_db_status'].get('connected'):
            total_records = results['indexer_db_status']['record_counts']
            total = sum(c for c in total_records.values() if c is not None)
            print(f"\n📊 Indexer Data: {total:,} total records (PROTECTED)")
        
        # Recommendations
        print(f"\n💡 Recommendations:")
        if not shared_ok:
            print("   ❌ Fix shared database connectivity first")
        elif has_conflicts:
            print("   🧹 Clean up conflicting columns before migration")
        elif has_data_issues:
            print("   🔧 Fix data issues or reset shared database")
        elif needs_migration:
            print("   ✅ Ready for migration")
        else:
            print("   ✅ Migration already complete")
    
    def fix_data_issues(self) -> bool:
        """Fix data issues to prepare for migration"""
        print("\n🔧 Fixing Data Issues")
        print("-" * 40)
        
        try:
            with self.shared_db_manager.get_session() as session:
                fixes_applied = []
                
                # Clean up any conflicting columns first
                print("🧹 Cleaning up conflicting columns...")
                cleanup_commands = [
                    "ALTER TABLE addresses DROP COLUMN IF EXISTS address_type",
                    "ALTER TABLE block_prices DROP COLUMN IF EXISTS created_at",
                    "ALTER TABLE block_prices DROP COLUMN IF EXISTS updated_at",
                    "ALTER TABLE model_contracts DROP COLUMN IF EXISTS updated_at",
                    "ALTER TABLE model_sources DROP COLUMN IF EXISTS updated_at", 
                    "ALTER TABLE model_tokens DROP COLUMN IF EXISTS updated_at",
                    "ALTER TABLE periods DROP COLUMN IF EXISTS created_at",
                    "ALTER TABLE periods DROP COLUMN IF EXISTS updated_at",
                    "ALTER TABLE pool_pricing_configs DROP COLUMN IF EXISTS updated_at",
                    "ALTER TABLE sources DROP COLUMN IF EXISTS source_type",
                    "ALTER TABLE sources DROP COLUMN IF EXISTS updated_at",
                ]
                
                for cmd in cleanup_commands:
                    try:
                        session.execute(text(cmd))
                        table = cmd.split()[2]
                        column = cmd.split()[-1]
                        fixes_applied.append(f"Removed conflicting {table}.{column}")
                    except Exception as e:
                        if "does not exist" not in str(e):
                            print(f"   ⚠️ Cleanup warning: {e}")
                
                # Fix NULL values for fields that will become NOT NULL
                print("🔧 Fixing NULL values...")
                
                data_fixes = [
                    ("contracts", "status", "'active'"),
                    ("contracts", "created_at", "NOW()"),
                    ("contracts", "updated_at", "NOW()"),
                    ("models", "status", "'active'"),
                    ("models", "created_at", "NOW()"),
                    ("models", "updated_at", "NOW()"),
                    ("tokens", "name", "'Unknown'"),
                    ("tokens", "symbol", "'UNK'"),
                    ("tokens", "decimals", "18"),
                    ("tokens", "status", "'active'"),
                    ("tokens", "created_at", "NOW()"),
                    ("tokens", "updated_at", "NOW()"),
                    ("sources", "status", "'active'"),
                    ("sources", "created_at", "NOW()"),
                    ("model_contracts", "created_at", "NOW()"),
                    ("model_sources", "created_at", "NOW()"),
                    ("model_tokens", "created_at", "NOW()"),
                    ("pool_pricing_configs", "created_at", "NOW()"),
                    ("addresses", "type", "'unknown'"),  # This will be used for address_type
                ]
                
                for table, column, default_value in data_fixes:
                    try:
                        # Check if table and column exist
                        exists = session.execute(text(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.columns 
                                WHERE table_name = '{table}' AND column_name = '{column}'
                            )
                        """)).scalar()
                        
                        if exists:
                            # Count nulls before fix
                            null_count = session.execute(text(f"""
                                SELECT COUNT(*) FROM {table} WHERE {column} IS NULL
                            """)).scalar()
                            
                            if null_count > 0:
                                session.execute(text(f"""
                                    UPDATE {table} SET {column} = {default_value} WHERE {column} IS NULL
                                """))
                                fixes_applied.append(f"Fixed {null_count} NULL values in {table}.{column}")
                        
                    except Exception as e:
                        print(f"   ⚠️ Could not fix {table}.{column}: {e}")
                
                # Commit all fixes
                session.commit()
                
                print(f"\n✅ Applied {len(fixes_applied)} fixes:")
                for fix in fixes_applied:
                    print(f"   {fix}")
                
                return True
                
        except Exception as e:
            print(f"❌ Failed to fix data issues: {e}")
            return False
    
    def reset_shared_database(self) -> bool:
        """Reset shared database to clean state and apply migrations"""
        print("\n🔄 Resetting Shared Database")
        print("-" * 40)
        print("⚠️ This will DELETE all shared database data!")
        
        response = input("Are you sure? Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("❌ Cancelled")
            return False
        
        try:
            # Drop and recreate shared database
            print("🗑️ Dropping shared database...")
            self.migration_manager.drop_shared_database()
            
            print("🆕 Creating fresh shared database...")
            self.migration_manager.create_shared_database()
            
            print("📦 Running migrations...")
            self.migration_manager.upgrade_shared()
            
            print("✅ Shared database reset and migrated successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Failed to reset shared database: {e}")
            return False
    
    def run_migration(self) -> bool:
        """Run the migration after ensuring data is ready"""
        print("\n🚀 Running Migration")
        print("-" * 40)
        
        try:
            # Check current state
            current = self.migration_manager.get_shared_current_revision()
            print(f"Current revision: {current}")
            
            # Run the migration
            print("🔄 Applying migration...")
            self.migration_manager.upgrade_shared()
            
            # Verify result
            new_revision = self.migration_manager.get_shared_current_revision()
            print(f"New revision: {new_revision}")
            
            # Check if price_vwap table was created
            with self.shared_db_manager.get_session() as session:
                price_vwap_exists = session.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'price_vwap'
                    )
                """)).scalar()
            
            if price_vwap_exists:
                print("✅ Migration successful! price_vwap table created.")
                return True
            else:
                print("⚠️ Migration completed but price_vwap table not found")
                return False
                
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            return False


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="Migration Diagnostic and Fix Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python migration_fix.py --diagnose                    # Check current state
    python migration_fix.py --fix-data                   # Fix data issues
    python migration_fix.py --reset-shared               # Reset shared DB
    python migration_fix.py --full-migrate               # Complete process
        """
    )
    
    parser.add_argument('--model', default=None, help='Model name (default: from env)')
    parser.add_argument('--diagnose', action='store_true', help='Run diagnostics only')
    parser.add_argument('--fix-data', action='store_true', help='Fix data issues for migration')
    parser.add_argument('--reset-shared', action='store_true', help='Reset shared database')
    parser.add_argument('--full-migrate', action='store_true', help='Complete migration process')
    
    args = parser.parse_args()
    
    if not any([args.diagnose, args.fix_data, args.reset_shared, args.full_migrate]):
        print("❌ Please specify an action. Use --help for options.")
        return 1
    
    try:
        tool = MigrationDiagnosticTool(model_name=args.model)
        
        if args.diagnose:
            tool.diagnose()
            
        elif args.fix_data:
            results = tool.diagnose()
            if results['data_issues'].get('has_issues') or results['data_issues'].get('has_conflicts'):
                if tool.fix_data_issues():
                    print("\n🔄 Re-running diagnostics...")
                    tool.diagnose()
            else:
                print("✅ No data issues found to fix")
                
        elif args.reset_shared:
            if tool.reset_shared_database():
                print("\n🔄 Running final diagnostics...")
                tool.diagnose()
                
        elif args.full_migrate:
            print("🚀 Running Full Migration Process")
            print("=" * 60)
            
            # Step 1: Diagnose
            results = tool.diagnose()
            
            # Step 2: Decide on approach
            if results['data_issues'].get('has_issues') or results['data_issues'].get('has_conflicts'):
                print("\n❓ Data issues found. Choose approach:")
                print("   1. Fix data and migrate")
                print("   2. Reset shared database")
                choice = input("Enter choice (1 or 2): ")
                
                if choice == "1":
                    if tool.fix_data_issues() and tool.run_migration():
                        print("\n🎉 Migration completed successfully!")
                    else:
                        print("\n❌ Migration process failed")
                        return 1
                elif choice == "2":
                    if tool.reset_shared_database():
                        print("\n🎉 Database reset completed successfully!")
                    else:
                        print("\n❌ Database reset failed")
                        return 1
                else:
                    print("❌ Invalid choice")
                    return 1
            else:
                # No issues, just migrate
                if tool.run_migration():
                    print("\n🎉 Migration completed successfully!")
                else:
                    print("\n❌ Migration failed")
                    return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Tool failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())