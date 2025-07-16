#!/usr/bin/env python3
# testing/diagnostics/raw_database_inspector.py
"""
Raw Database Inspector

Direct database inspection using DI container pattern but bypassing ORM to understand current schema.
This gets database connections from the container but inspects raw schema.
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional
import json

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import text, inspect
from sqlalchemy.engine import Engine
from indexer.core.config import IndexerConfig
from indexer import create_indexer


class RawDatabaseInspector:
    """Direct database inspection using DI container pattern."""
    
    def __init__(self, model_name: str = None):
        print(f"🔍 Raw Database Inspector")
        print("Getting database connections from DI container...")
        
        # Try to get database connections from DI container
        # even if ORM initialization fails
        try:
            self.container = create_indexer(model_name=model_name)
            self.config = self.container._config
            print(f"✅ Container initialized for model: {self.config.model_name}")
            
            # Get database managers directly
            from indexer.database.connection import InfrastructureDatabaseManager, ModelDatabaseManager
            self.shared_db_manager = self.container.get(InfrastructureDatabaseManager)
            self.model_db_manager = self.container.get(ModelDatabaseManager)
            
            print(f"✅ Database managers obtained")
            
        except Exception as e:
            print(f"⚠️  Container initialization failed: {e}")
            print("This might be due to schema mismatch - trying to get engines directly...")
            
            # Try to get engines directly even if ORM models fail
            try:
                self.container = create_indexer(model_name=model_name)
                from indexer.database.connection import InfrastructureDatabaseManager, ModelDatabaseManager
                self.shared_db_manager = self.container.get(InfrastructureDatabaseManager)
                self.model_db_manager = self.container.get(ModelDatabaseManager)
                print(f"✅ Got database managers despite ORM issues")
            except Exception as e2:
                print(f"❌ Could not get database managers: {e2}")
                raise
        
        print()
        
    def inspect_all(self):
        """Inspect both databases."""
        print("=" * 80)
        print("RAW DATABASE INSPECTION")
        print("=" * 80)
        
        # Inspect shared database
        self._inspect_database(self.shared_db_manager, "SHARED DATABASE")
        
        # Inspect model database  
        self._inspect_database(self.model_db_manager, "MODEL DATABASE")
        
        # Summary
        self._print_summary()
        
    def _inspect_database(self, db_manager, db_name: str):
        """Inspect a specific database."""
        print(f"\n📚 {db_name}")
        print("-" * 60)
        
        try:
            engine = db_manager.engine
            
            # Test connection using raw SQL
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("✅ Connection: OK")
            
            # Get all tables
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            print(f"📋 Tables found: {len(tables)}")
            
            if not tables:
                print("❌ No tables found")
                return
            
            # Check each table
            for table_name in sorted(tables):
                self._inspect_table(engine, table_name)
                
        except Exception as e:
            print(f"❌ Database error: {e}")
            
    def _inspect_table(self, engine: Engine, table_name: str):
        """Inspect a specific table."""
        print(f"\n   📋 {table_name}")
        
        try:
            inspector = inspect(engine)
            
            # Get columns
            columns = inspector.get_columns(table_name)
            print(f"      Columns: {len(columns)}")
            
            # Show column details
            for col in columns:
                nullable = "NULL" if col['nullable'] else "NOT NULL"
                default = f" DEFAULT {col['default']}" if col.get('default') else ""
                print(f"        • {col['name']}: {col['type']} {nullable}{default}")
            
            # Get row count
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = result.scalar()
                print(f"      Rows: {count:,}")
                
                # Show sample data for small tables
                if count > 0 and count <= 10:
                    print(f"      Sample data:")
                    result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 3"))
                    rows = result.fetchall()
                    columns_names = result.keys()
                    
                    for row in rows:
                        row_data = dict(zip(columns_names, row))
                        print(f"        {row_data}")
                        
        except Exception as e:
            print(f"      ❌ Error inspecting table: {e}")
    
    def _print_summary(self):
        """Print inspection summary."""
        print("\n" + "=" * 80)
        print("INSPECTION SUMMARY")
        print("=" * 80)
        
        print("\n🎯 Key Findings:")
        print("1. Check if 'models' table has 'target_asset' column")
        print("2. Check if 'contracts' table has 'project' column")
        print("3. Look for pricing service tables:")
        print("   - price_vwap (shared)")
        print("   - pool_pricing_configs (shared)")
        print("   - pool_swap_details (model)")
        print("   - trade_details (model)")
        print("   - asset_price (model)")
        print("   - asset_volume (model)")
        
        print("\n🔧 Next Steps:")
        print("1. Compare actual schema with expected schema")
        print("2. Identify missing columns in existing tables")
        print("3. Identify completely missing tables")
        print("4. Plan migration strategy for existing data")


def main():
    """Run raw database inspection."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Raw Database Inspector')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        inspector = RawDatabaseInspector(model_name=args.model)
        inspector.inspect_all()
        
    except Exception as e:
        print(f"\n💥 Raw inspection failed: {e}")
        print("\nPossible issues:")
        print("- Database server not running")
        print("- Connection credentials incorrect")
        print("- DI container initialization failed")
        sys.exit(1)


if __name__ == "__main__":
    main()