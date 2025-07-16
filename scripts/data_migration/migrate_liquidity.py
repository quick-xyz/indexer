#!/usr/bin/env python3
"""
Liquidity Table Migration Script

Migrates liquidity table data from v1 (blub_test) to v2 (blub_test_v2) database.
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
from sqlalchemy import text

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data_migration.base_migrator import BaseMigrator


class LiquidityMigrator(BaseMigrator):
    """Migrate liquidity table from v1 to v2 database."""
    
    def __init__(self, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2", model_name: str = None):
        super().__init__("liquidity", v1_db_name, v2_db_name, model_name)
    
    def get_v1_data_stats(self) -> Dict:
        """Get detailed v1 liquidity data statistics."""
        print(f"\n📊 Analyzing v1 liquidity data...")
        
        stats_queries = {
            "total_count": "SELECT COUNT(*) as count FROM liquidity",
            "unique_content_ids": "SELECT COUNT(DISTINCT content_id) as count FROM liquidity", 
            "action_distribution": "SELECT action, COUNT(*) as count FROM liquidity GROUP BY action ORDER BY action",
            "block_range": "SELECT MIN(block_number) as min_block, MAX(block_number) as max_block FROM liquidity",
            "sample_data": "SELECT * FROM liquidity ORDER BY block_number, timestamp LIMIT 3"
        }
        
        stats = {}
        with self.v1_engine.connect() as conn:
            for stat_name, query in stats_queries.items():
                result = conn.execute(text(query))
                if stat_name in ["action_distribution", "sample_data"]:
                    stats[stat_name] = [dict(row._mapping) for row in result]
                else:
                    stats[stat_name] = dict(result.fetchone()._mapping)
        
        # Print stats
        print(f"   Total rows: {stats['total_count']['count']}")
        print(f"   Unique content_ids: {stats['unique_content_ids']['count']}")
        print(f"   Block range: {stats['block_range']['min_block']} - {stats['block_range']['max_block']}")
        print(f"   Action distribution:")
        for action_stat in stats['action_distribution']:
            print(f"     - {action_stat['action']}: {action_stat['count']}")
            
        return stats
    
    def migrate_data(self) -> Dict:
        """Migrate liquidity data with proper field mapping."""
        # Field mapping: v2_field -> v1_field (all fields map directly)
        field_mapping = {
            "content_id": "content_id",
            "tx_hash": "tx_hash", 
            "block_number": "block_number",
            "timestamp": "timestamp",
            "pool": "pool",
            "provider": "provider",
            "action": "action",
            "base_token": "base_token",
            "base_amount": "base_amount",
            "quote_token": "quote_token",
            "quote_amount": "quote_amount"
        }
        
        return self.execute_migration(field_mapping)
    
    def validate_migration(self) -> Dict:
        """Validate liquidity migration with detailed checks."""
        print(f"\n✅ Validating liquidity migration...")
        
        validation_queries = {
            "v1_count": f"SELECT COUNT(*) as count FROM liquidity",
            "v2_count": f"SELECT COUNT(*) as count FROM liquidity", 
            "v1_unique_ids": f"SELECT COUNT(DISTINCT content_id) as count FROM liquidity",
            "v2_unique_ids": f"SELECT COUNT(DISTINCT content_id) as count FROM liquidity",
            "v1_actions": f"SELECT action, COUNT(*) as count FROM liquidity GROUP BY action ORDER BY action",
            "v2_actions": f"SELECT action, COUNT(*) as count FROM liquidity GROUP BY action ORDER BY action",
            "v1_block_range": f"SELECT MIN(block_number) as min_block, MAX(block_number) as max_block FROM liquidity",
            "v2_block_range": f"SELECT MIN(block_number) as min_block, MAX(block_number) as max_block FROM liquidity"
        }
        
        results = {}
        
        # Get v1 results
        with self.v1_engine.connect() as conn:
            for key in ["v1_count", "v1_unique_ids", "v1_actions", "v1_block_range"]:
                result = conn.execute(text(validation_queries[key]))
                if key == "v1_actions":
                    results[key] = [dict(row._mapping) for row in result]
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Get v2 results  
        with self.v2_engine.connect() as conn:
            for key in ["v2_count", "v2_unique_ids", "v2_actions", "v2_block_range"]:
                result = conn.execute(text(validation_queries[key]))
                if key == "v2_actions":
                    results[key] = [dict(row._mapping) for row in result]
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Compare results
        validation_passed = True
        
        # Check row counts
        v1_count = results["v1_count"]["count"]
        v2_count = results["v2_count"]["count"]
        if v1_count == v2_count:
            print(f"   ✅ Row counts match: {v1_count}")
        else:
            print(f"   ❌ Row counts mismatch: v1={v1_count}, v2={v2_count}")
            validation_passed = False
        
        # Check unique content_ids
        v1_unique = results["v1_unique_ids"]["count"]
        v2_unique = results["v2_unique_ids"]["count"]
        if v1_unique == v2_unique:
            print(f"   ✅ Unique content_ids match: {v1_unique}")
        else:
            print(f"   ❌ Unique content_ids mismatch: v1={v1_unique}, v2={v2_unique}")
            validation_passed = False
            
        # Check action distributions
        v1_actions = {item["action"]: item["count"] for item in results["v1_actions"]}
        v2_actions = {item["action"]: item["count"] for item in results["v2_actions"]}
        if v1_actions == v2_actions:
            print(f"   ✅ Action distributions match: {v1_actions}")
        else:
            print(f"   ❌ Action distributions mismatch:")
            print(f"     V1: {v1_actions}")
            print(f"     V2: {v2_actions}")
            validation_passed = False
        
        # Check block ranges
        v1_range = results["v1_block_range"]
        v2_range = results["v2_block_range"]
        if v1_range == v2_range:
            print(f"   ✅ Block ranges match: {v1_range['min_block']} - {v1_range['max_block']}")
        else:
            print(f"   ❌ Block ranges mismatch:")
            print(f"     V1: {v1_range}")
            print(f"     V2: {v2_range}")
            validation_passed = False
        
        print(f"\n{'✅ DETAILED VALIDATION PASSED' if validation_passed else '❌ DETAILED VALIDATION FAILED'}")
        
        return {
            "validation_passed": validation_passed,
            "v1_count": v1_count,
            "v2_count": v2_count,
            "details": results
        }
    
    def run_full_migration(self) -> Dict:
        """Run complete liquidity migration with validation."""
        print(f"\n🚀 Starting liquidity table migration: {self.v1_db_name} → {self.v2_db_name}")
        print("=" * 80)
        
        try:
            # Analyze source data
            schema_info = self.get_v1_schema_info()
            data_stats = self.get_v1_data_stats()
            
            # Perform migration
            migration_result = self.migrate_data()
            
            # Validate migration
            validation_result = self.validate_migration()
            
            # Print summary
            overall_success = self.print_migration_summary(migration_result, validation_result)
            
            return {
                "success": overall_success,
                "schema_info": schema_info,
                "data_stats": data_stats,
                "migration_result": migration_result,
                "validation_result": validation_result
            }
            
        except Exception as e:
            print(f"\n❌ Migration failed with error: {e}")
            return {"success": False, "error": str(e)}


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate liquidity table from v1 to v2")
    parser.add_argument("--v1-db", default="blub_test", help="V1 database name")
    parser.add_argument("--v2-db", default="blub_test_v2", help="V2 database name")
    parser.add_argument("--model", default=None, help="Model name (defaults to INDEXER_MODEL_NAME env var)")
    args = parser.parse_args()
    
    migrator = LiquidityMigrator(v1_db_name=args.v1_db, v2_db_name=args.v2_db, model_name=args.model)
    result = migrator.run_full_migration()
    
    if not result["success"]:
        sys.exit(1)


if __name__ == "__main__":
    main()