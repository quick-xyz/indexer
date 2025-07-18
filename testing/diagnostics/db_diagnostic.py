#!/usr/bin/env python3
# testing/diagnostics/db_diagnostic.py

"""
Database Connection Diagnostic

Verifies database connections and schema for both shared and model databases.
"""

import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from sqlalchemy import text, inspect
from indexer.database.connection import ModelDatabaseManager, SharedDatabaseManager


class DatabaseDiagnostic:
    """Check database connections and schema."""
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name
        self.results: Dict[str, List[Tuple[str, bool, str]]] = {
            "shared": [],
            "model": []
        }
        
    def run(self) -> bool:
        """Run all database checks."""
        print("🗄️ Database Connection Diagnostic")
        print("=" * 60)
        
        # Initialize environment
        try:
            self.env = get_testing_environment(model_name=self.model_name)
        except Exception as e:
            print(f"❌ Failed to initialize environment: {e}")
            return False
        
        # Test shared database
        self._test_shared_database()
        
        # Test model database
        self._test_model_database()
        
        # Print results
        self._print_results()
        
        # Return overall success
        all_results = self.results["shared"] + self.results["model"]
        return all(result[1] for result in all_results)
    
    def _test_shared_database(self):
        """Test shared/infrastructure database."""
        print("\n📚 Testing shared database (indexer_shared)...")
        
        try:
            # Get infrastructure DB manager
            db_manager = self.env.get_service(SharedDatabaseManager)
            
            # Test connection
            with db_manager.get_session() as session:
                result = session.execute(text("SELECT 1"))
                result.scalar()
            self.results["shared"].append(("Connection", True, "Connected"))
            
            # Check key tables
            self._check_tables(db_manager, "shared", [
                "models",
                "contracts", 
                "tokens",
                "sources",
                "addresses",
                "pool_pricing_configs",
                "block_prices",
                "periods"
            ])
            
        except Exception as e:
            self.results["shared"].append(("Connection", False, str(e)))
    
    def _test_model_database(self):
        """Test model-specific database."""
        config = self.env.get_config()
        db_name = config.model_db_name
        
        print(f"\n🎯 Testing model database ({db_name})...")
        
        try:
            # Get model DB manager
            db_manager = self.env.get_service(ModelDatabaseManager)
            
            # Test connection
            with db_manager.get_session() as session:
                result = session.execute(text("SELECT 1"))
                result.scalar()
            self.results["model"].append(("Connection", True, "Connected"))
            
            # Check key tables
            self._check_tables(db_manager, "model", [
                "transaction_processing",
                "block_processing",
                "processing_jobs",
                "trades",
                "pool_swaps",
                "positions",
                "transfers",
                "liquidity",
                "rewards",
                "pool_swap_details",
                "trade_details",
                "event_details"
            ])
            
        except Exception as e:
            self.results["model"].append(("Connection", False, str(e)))
    
    def _check_tables(self, db_manager, db_type: str, expected_tables: List[str]):
        """Check if expected tables exist."""
        try:
            engine = db_manager.engine
            inspector = inspect(engine)
            actual_tables = set(inspector.get_table_names())
            
            for table in expected_tables:
                if table in actual_tables:
                    # Get row count
                    try:
                        with db_manager.get_session() as session:
                            count = session.execute(
                                text(f"SELECT COUNT(*) FROM {table}")
                            ).scalar()
                        self.results[db_type].append((
                            f"Table: {table}", 
                            True, 
                            f"{count:,} rows"
                        ))
                    except Exception as e:
                        self.results[db_type].append((
                            f"Table: {table}", 
                            True, 
                            "exists (count failed)"
                        ))
                else:
                    self.results[db_type].append((
                        f"Table: {table}", 
                        False, 
                        "missing"
                    ))
                    
        except Exception as e:
            self.results[db_type].append(("Table check", False, str(e)))
    
    def _print_results(self):
        """Print diagnostic results."""
        print("\n📊 Results:")
        print("-" * 60)
        
        # Shared database results
        print("\n🌐 Shared Database:")
        for check, success, info in self.results["shared"]:
            status = "✅" if success else "❌"
            print(f"  {status} {check:<25} {info}")
        
        # Model database results  
        print(f"\n🎯 Model Database ({self.env.get_config().model_db_name}):")
        for check, success, info in self.results["model"]:
            status = "✅" if success else "❌"
            print(f"  {status} {check:<25} {info}")
        
        # Summary
        all_results = self.results["shared"] + self.results["model"]
        total = len(all_results)
        passed = sum(1 for _, success, _ in all_results if success)
        
        print("-" * 60)
        print(f"Total: {passed}/{total} database checks passed")
        
        if passed < total:
            print("\n💡 Common fixes:")
            print("  - Check database connection credentials")
            print("  - Run migrations: python -m indexer.cli migrate dev setup <model>")
            print("  - Import configuration: python -m indexer.cli config import-model <file>")


def main():
    """Run database diagnostic."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Database Connection Diagnostic')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        diagnostic = DatabaseDiagnostic(model_name=args.model)
        success = diagnostic.run()
        
        if success:
            print("\n✅ All database checks passed!")
        else:
            print("\n❌ Some database checks failed")
            
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 Diagnostic failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()