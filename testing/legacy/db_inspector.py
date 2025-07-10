#!/usr/bin/env python3
"""
Database Inspector Script

Quick script to inspect database schema and sample data
without needing psql installed.
"""

import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer.cli.context import CLIContext
from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine

def inspect_database_schema(db_manager, db_name: str):
    """Inspect database schema using DatabaseManager"""
    print(f"\n{'='*60}")
    print(f"DATABASE: {db_name}")
    print(f"{'='*60}")
    
    engine = db_manager.engine
    inspector = inspect(engine)
    
    # Get all table names
    tables = inspector.get_table_names()
    print(f"\nTables ({len(tables)}):")
    
    for table in sorted(tables):
        print(f"  📋 {table}")
        
        # Get column info
        columns = inspector.get_columns(table)
        print(f"     Columns ({len(columns)}):")
        
        for col in columns:
            nullable = "NULL" if col['nullable'] else "NOT NULL"
            print(f"       • {col['name']}: {col['type']} {nullable}")
        
        # Get row count
        try:
            with db_manager.get_session() as session:
                result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                print(f"     Rows: {count:,}")
        except Exception as e:
            print(f"     Rows: Error - {e}")
        
        print()

def get_sample_data(db_manager, table: str, limit: int = 3):
    """Get sample data from a table"""
    print(f"\n📋 Sample data from {table} (limit {limit}):")
    print("-" * 60)
    
    try:
        with db_manager.get_session() as session:
            result = session.execute(text(f"SELECT * FROM {table} LIMIT {limit}"))
            rows = result.fetchall()
            columns = result.keys()
            
            if rows:
                # Print column headers
                for col in columns:
                    print(f"{col:<20}", end="")
                print()
                print("-" * (20 * len(columns)))
                
                # Print rows
                for row in rows:
                    for value in row:
                        # Truncate long values
                        str_val = str(value)
                        if len(str_val) > 18:
                            str_val = str_val[:15] + "..."
                        print(f"{str_val:<20}", end="")
                    print()
            else:
                print("No data found")
                
    except Exception as e:
        print(f"Error: {e}")

def main():
    """Main inspection function"""
    print("🔍 Database Inspector")
    print("===================")
    
    # Initialize CLI context
    cli_context = CLIContext()
    
    # Inspect shared database
    try:
        shared_db_manager = cli_context.infrastructure_db_manager
        inspect_database_schema(shared_db_manager, "indexer_shared")
        
        # Sample data from key shared tables
        key_shared_tables = ['models', 'contracts', 'tokens', 'sources', 'pool_pricing_configs']
        for table in key_shared_tables:
            try:
                get_sample_data(shared_db_manager, table)
            except Exception as e:
                print(f"Could not sample {table}: {e}")
                
    except Exception as e:
        print(f"❌ Could not connect to shared database: {e}")
    
    # Inspect model database (blub_test)
    try:
        model_db_manager = cli_context.get_model_db_manager("blub_test")
        inspect_database_schema(model_db_manager, "blub_test")
        
        # Sample data from key model tables
        key_model_tables = ['trades', 'pool_swaps', 'positions', 'processing_jobs', 'pool_swap_details']
        for table in key_model_tables:
            try:
                get_sample_data(model_db_manager, table)
            except Exception as e:
                print(f"Could not sample {table}: {e}")
                
    except Exception as e:
        print(f"❌ Could not connect to model database: {e}")

if __name__ == "__main__":
    main()