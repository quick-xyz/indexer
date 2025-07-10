#!/usr/bin/env python3
"""
Comprehensive Database Schema Check
Check ALL tables and enums for model vs database disparities
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.database.repository import RepositoryManager
from sqlalchemy import text
import logging


class SchemaChecker:
    def __init__(self):
        self.container = create_indexer()
        self.repository_manager = self.container.get(RepositoryManager)
        self.issues = []
    
    def check_all_schemas(self):
        """Run comprehensive schema checks"""
        print("🔍 COMPREHENSIVE DATABASE SCHEMA CHECK")
        print("=" * 60)
        
        # Check all enum types first
        self.check_enum_types()
        
        # Check each table schema
        tables_to_check = [
            'transaction_processing',
            'processing_jobs', 
            'block_processing',
            'trades',
            'pool_swaps',
            'positions',
            'transfers',
            'liquidity',
            'rewards'
        ]
        
        for table in tables_to_check:
            self.check_table_schema(table)
        
        # Summary
        self.print_summary()
        
        return len(self.issues) == 0
    
    def check_enum_types(self):
        """Check all enum types and their values"""
        print("\n📋 ENUM TYPES CHECK")
        print("-" * 40)
        
        with self.repository_manager.get_session() as session:
            # Get all custom enum types
            result = session.execute(text("""
                SELECT t.typname, e.enumlabel
                FROM pg_type t 
                JOIN pg_enum e ON t.oid = e.enumtypid
                WHERE t.typname IN ('transactionstatus', 'jobstatus', 'jobtype', 'liquidityaction', 'rewardtype')
                ORDER BY t.typname, e.enumsortorder
            """))
            
            enum_values = {}
            for row in result:
                if row.typname not in enum_values:
                    enum_values[row.typname] = []
                enum_values[row.typname].append(row.enumlabel)
        
        # Define expected enum values
        expected_enums = {
            'transactionstatus': ['PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'],
            'jobstatus': ['pending', 'processing', 'complete', 'failed'],
            'jobtype': ['BLOCK', 'BLOCK_RANGE', 'TRANSACTIONS', 'REPROCESS_FAILED'],  # These might be wrong
            'liquidityaction': ['ADD', 'REMOVE', 'UPDATE'],
            'rewardtype': ['FEES', 'REWARDS']
        }
        
        for enum_name, expected_values in expected_enums.items():
            actual_values = enum_values.get(enum_name, [])
            
            print(f"\n🔍 {enum_name}:")
            print(f"   Expected: {expected_values}")
            print(f"   Actual:   {actual_values}")
            
            if set(actual_values) != set(expected_values):
                self.issues.append({
                    'type': 'enum_mismatch',
                    'enum_name': enum_name,
                    'expected': expected_values,
                    'actual': actual_values,
                    'missing': [v for v in expected_values if v not in actual_values],
                    'extra': [v for v in actual_values if v not in expected_values]
                })
                print(f"   ❌ MISMATCH!")
                if expected_values and actual_values:
                    print(f"      Missing: {[v for v in expected_values if v not in actual_values]}")
                    print(f"      Extra:   {[v for v in actual_values if v not in expected_values]}")
            else:
                print(f"   ✅ Match")
    
    def check_table_schema(self, table_name):
        """Check individual table schema"""
        print(f"\n📋 TABLE: {table_name}")
        print("-" * 40)
        
        with self.repository_manager.get_session() as session:
            # Check if table exists
            table_exists = session.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = :table_name
                )
            """), {"table_name": table_name}).scalar()
            
            if not table_exists:
                print(f"   ❌ Table does not exist!")
                self.issues.append({
                    'type': 'missing_table',
                    'table_name': table_name
                })
                return
            
            # Get current columns
            result = session.execute(text("""
                SELECT 
                    column_name, 
                    data_type,
                    udt_name,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_name = :table_name
                ORDER BY ordinal_position
            """), {"table_name": table_name})
            
            current_columns = {}
            for row in result:
                current_columns[row.column_name] = {
                    'data_type': row.data_type,
                    'udt_name': row.udt_name,
                    'nullable': row.is_nullable == 'YES',
                    'default': row.column_default
                }
        
        # Define expected schemas for key tables
        expected_schemas = self.get_expected_schemas()
        
        if table_name in expected_schemas:
            expected = expected_schemas[table_name]
            
            print(f"   Columns found: {len(current_columns)}")
            
            # Check for missing/extra columns
            current_column_names = set(current_columns.keys())
            expected_column_names = expected if isinstance(expected, set) else set(expected.keys())
            
            missing = expected_column_names - current_column_names
            extra = current_column_names - expected_column_names
            
            if missing:
                print(f"   ❌ Missing columns: {list(missing)}")
                self.issues.append({
                    'type': 'missing_columns',
                    'table_name': table_name,
                    'columns': list(missing)
                })
            
            if extra:
                print(f"   ⚠️  Extra columns: {list(extra)}")
                # Extra columns are usually OK, just note them
            
            if not missing and not extra:
                print(f"   ✅ All expected columns present")
        else:
            print(f"   ⚠️  No schema definition to check against")
    
    def get_expected_schemas(self):
        """Define expected schemas for all tables"""
        return {
            'transaction_processing': {
                'id', 'created_at', 'updated_at',
                'tx_hash', 'block_number', 'tx_index', 'timestamp', 
                'status', 'retry_count', 'last_processed_at',
                'error_message', 'gas_used', 'gas_price',
                'logs_processed', 'events_generated', 'tx_success',
                'signals_generated', 'positions_generated'
            },
            'processing_jobs': {
                'id', 'created_at', 'updated_at',
                'job_type', 'status', 'job_data', 'worker_id', 
                'priority', 'retry_count', 'max_retries',
                'error_message', 'started_at', 'completed_at'
            },
            'block_processing': {
                'id', 'created_at', 'updated_at',
                'block_number', 'block_hash', 'timestamp',
                'transaction_count', 'transactions_pending',
                'transactions_processing', 'transactions_complete',
                'transactions_failed'
            },
            'trades': {
                'content_id', 'tx_hash', 'block_number', 'timestamp', 'created_at', 'updated_at',
                'maker', 'taker', 'base_token', 'quote_token', 'base_amount', 'quote_amount',
                'trade_type', 'pool', 'price'
            },
            'pool_swaps': {
                'content_id', 'tx_hash', 'block_number', 'timestamp', 'created_at', 'updated_at',
                'pool', 'sender', 'recipient', 'token_in', 'amount_in', 'token_out', 'amount_out'
            },
            'positions': {
                'content_id', 'tx_hash', 'block_number', 'timestamp', 'created_at', 'updated_at',
                'user', 'token', 'amount', 'position_type'
            },
            'transfers': {
                'content_id', 'tx_hash', 'block_number', 'timestamp', 'created_at', 'updated_at',
                'from_address', 'to_address', 'token', 'amount', 'classification',
                'parent_type', 'parent_id'
            },
            'liquidity': {
                'content_id', 'tx_hash', 'block_number', 'timestamp', 'created_at', 'updated_at',
                'pool', 'provider', 'action', 'base_token', 'base_amount', 
                'quote_token', 'quote_amount'
            },
            'rewards': {
                'content_id', 'tx_hash', 'block_number', 'timestamp', 'created_at', 'updated_at',
                'contract', 'recipient', 'token', 'amount', 'reward_type'
            }
        }
    
    def print_summary(self):
        """Print summary of all issues found"""
        print(f"\n📊 SUMMARY")
        print("=" * 60)
        
        if not self.issues:
            print("🎉 No schema issues found! Everything looks good.")
            return
        
        print(f"❌ Found {len(self.issues)} schema issues:")
        
        enum_issues = [i for i in self.issues if i['type'] == 'enum_mismatch']
        table_issues = [i for i in self.issues if i['type'] in ['missing_table', 'missing_columns']]
        
        if enum_issues:
            print(f"\n🔸 ENUM ISSUES ({len(enum_issues)}):")
            for issue in enum_issues:
                enum_name = issue['enum_name']
                missing = issue['missing']
                extra = issue['extra']
                print(f"   • {enum_name}")
                if missing:
                    print(f"     Missing values: {missing}")
                if extra:
                    print(f"     Extra values: {extra}")
        
        if table_issues:
            print(f"\n🔸 TABLE ISSUES ({len(table_issues)}):")
            for issue in table_issues:
                if issue['type'] == 'missing_table':
                    print(f"   • Missing table: {issue['table_name']}")
                elif issue['type'] == 'missing_columns':
                    print(f"   • {issue['table_name']}: Missing columns {issue['columns']}")
        
        print(f"\n🔧 RECOMMENDED ACTIONS:")
        print("   1. Run database migrations to fix schema mismatches")
        print("   2. Update enum values to match model definitions")
        print("   3. Re-run this check after fixes")


def main():
    """Main entry point"""
    print("🔍 Comprehensive Database Schema Checker")
    print("Checking ALL tables and enums for model disparities")
    print()
    
    # Configure basic logging
    logging.basicConfig(level=logging.WARNING)
    
    checker = SchemaChecker()
    success = checker.check_all_schemas()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()