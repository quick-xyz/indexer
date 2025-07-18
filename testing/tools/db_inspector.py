#!/usr/bin/env python3
"""
Database Inspector

Inspect database schema and sample data for both shared and model databases.
"""

import sys
from pathlib import Path
from typing import Optional, List

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from sqlalchemy import text, inspect
from indexer.database.connection import ModelDatabaseManager, SharedDatabaseManager


class DatabaseInspector:
    """Inspect database schema and data."""
    
    def __init__(self, model_name: str = None):
        self.env = get_testing_environment(model_name=model_name)
        self.config = self.env.get_config()
        
    def inspect_all(self):
        """Inspect both databases."""
        print("🔍 Database Inspector")
        print("=" * 80)
        
        # Inspect shared database
        self._inspect_shared_database()
        
        # Inspect model database
        self._inspect_model_database()
        
    def _inspect_shared_database(self):
        """Inspect shared/infrastructure database."""
        print(f"\n{'='*60}")
        print(f"SHARED DATABASE: indexer_shared")
        print(f"{'='*60}")
        
        try:
            db_manager = self.env.get_service(SharedDatabaseManager)
            self._inspect_database(db_manager, "shared")
            
            # Sample key tables
            key_tables = [
                'models',
                'contracts',
                'tokens',
                'sources',
                'pool_pricing_configs',
                'block_prices'
            ]
            
            for table in key_tables:
                self._sample_table_data(db_manager, table)
                
        except Exception as e:
            print(f"❌ Error inspecting shared database: {e}")
    
    def _inspect_model_database(self):
        """Inspect model-specific database."""
        db_name = self.config.model_db
        
        print(f"\n{'='*60}")
        print(f"MODEL DATABASE: {db_name}")
        print(f"{'='*60}")
        
        try:
            db_manager = self.env.get_service(ModelDatabaseManager)
            self._inspect_database(db_manager, "model")
            
            # Sample key tables
            key_tables = [
                'trades',
                'pool_swaps',
                'positions',
                'processing_jobs',
                'pool_swap_details',
                'trade_details'
            ]
            
            for table in key_tables:
                self._sample_table_data(db_manager, table)
                
        except Exception as e:
            print(f"❌ Error inspecting model database: {e}")
    
    def _inspect_database(self, db_manager, db_type: str):
        """Inspect database schema."""
        try:
            engine = db_manager.engine
            inspector = inspect(engine)
            
            # Get all tables
            tables = inspector.get_table_names()
            print(f"\nTables ({len(tables)}):")
            
            for table in sorted(tables):
                # Get column count
                columns = inspector.get_columns(table)
                
                # Get row count
                try:
                    with db_manager.get_session() as session:
                        count = session.execute(
                            text(f"SELECT COUNT(*) FROM {table}")
                        ).scalar()
                    print(f"  📋 {table:<30} ({len(columns)} columns, {count:,} rows)")
                except:
                    print(f"  📋 {table:<30} ({len(columns)} columns)")
                    
        except Exception as e:
            print(f"❌ Error inspecting {db_type} database: {e}")
    
    def _sample_table_data(self, db_manager, table_name: str, limit: int = 3):
        """Sample data from a table."""
        print(f"\n📊 {table_name} (sample):")
        print("-" * 60)
        
        try:
            with db_manager.get_session() as session:
                # Get column names
                result = session.execute(
                    text(f"SELECT * FROM {table_name} LIMIT 0")
                )
                columns = result.keys()
                
                # Get sample rows
                result = session.execute(
                    text(f"SELECT * FROM {table_name} LIMIT :limit"),
                    {"limit": limit}
                )
                rows = result.fetchall()
                
                if not rows:
                    print("  (no data)")
                    return
                
                # Determine column widths
                col_widths = {}
                for col in columns:
                    col_widths[col] = min(max(len(str(col)), 10), 30)
                
                # Print header
                header = ""
                for col in columns:
                    header += f"{str(col)[:col_widths[col]]:<{col_widths[col]}} "
                print(f"  {header}")
                print(f"  {'-' * len(header)}")
                
                # Print rows
                for row in rows:
                    row_str = ""
                    for i, col in enumerate(columns):
                        value = row[i]
                        str_val = str(value) if value is not None else "NULL"
                        if len(str_val) > col_widths[col] - 3:
                            str_val = str_val[:col_widths[col]-3] + "..."
                        row_str += f"{str_val:<{col_widths[col]}} "
                    print(f"  {row_str}")
                    
        except Exception as e:
            print(f"  Error sampling data: {e}")


def main():
    """Run database inspection."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Database Inspector')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    parser.add_argument('--table', help='Specific table to inspect')
    parser.add_argument('--limit', type=int, default=5, help='Number of rows to sample')
    args = parser.parse_args()
    
    try:
        inspector = DatabaseInspector(model_name=args.model)
        
        if args.table:
            # Inspect specific table
            print(f"🔍 Inspecting table: {args.table}")
            
            # Try both databases
            try:
                shared_db = inspector.env.get_service(SharedDatabaseManager)
                inspector._sample_table_data(shared_db, args.table, args.limit)
            except:
                try:
                    model_db = inspector.env.get_service(ModelDatabaseManager)
                    inspector._sample_table_data(model_db, args.table, args.limit)
                except Exception as e:
                    print(f"❌ Table '{args.table}' not found in either database")
        else:
            # Full inspection
            inspector.inspect_all()
            
    except Exception as e:
        print(f"\n💥 Inspection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()