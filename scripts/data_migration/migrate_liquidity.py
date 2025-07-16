#!/usr/bin/env python3
"""
Liquidity Table Migration Script - Proper DI Version

Uses the established create_indexer() pattern to get database connections with proper credentials.
"""

import sys
import os
from pathlib import Path
from typing import Dict
from sqlalchemy import text, create_engine

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.database.connection import InfrastructureDatabaseManager, ModelDatabaseManager


class ProperLiquidityMigrator:
    """Migrate liquidity table using proper DI container setup."""
    
    def __init__(self, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2"):
        self.v1_db_name = v1_db_name
        self.v2_db_name = v2_db_name
        
        print(f"🔧 Initializing liquidity migration")
        print(f"   V1 DB: {v1_db_name}")
        print(f"   V2 DB: {v2_db_name}")
        
        # Use create_indexer to get proper database credentials
        self._setup_database_connections()
        
    def _setup_database_connections(self):
        """Setup database connections using the established DI pattern."""
        print("🔗 Setting up database connections using infrastructure DB pattern...")
        
        try:
            # Use the same pattern as _create_infrastructure_db_manager() to get credentials
            env = os.environ
            project_id = env.get("INDEXER_GCP_PROJECT_ID")
            
            if project_id:
                # Use SecretsService directly (like in _create_infrastructure_db_manager)
                from indexer.core.secrets_service import SecretsService
                temp_secrets_service = SecretsService(project_id)
                db_credentials = temp_secrets_service.get_database_credentials()
                
                db_user = db_credentials.get('user') or env.get("INDEXER_DB_USER") 
                db_password = db_credentials.get('password') or env.get("INDEXER_DB_PASSWORD")
                db_host = env.get("INDEXER_DB_HOST") or db_credentials.get('host') or "127.0.0.1"
                db_port = env.get("INDEXER_DB_PORT") or db_credentials.get('port') or "5432"
            else:
                db_user = env.get("INDEXER_DB_USER")
                db_password = env.get("INDEXER_DB_PASSWORD")  
                db_host = env.get("INDEXER_DB_HOST", "127.0.0.1")
                db_port = env.get("INDEXER_DB_PORT", "5432")

            if not db_user or not db_password:
                raise ValueError("Database credentials not found")
            
            # Build URLs for both v1 and v2 databases using the same credentials
            base_url = f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}"
            
            v1_url = f"{base_url}/{self.v1_db_name}"
            v2_url = f"{base_url}/{self.v2_db_name}"
            
            # Create engines for both databases
            self.v1_engine = create_engine(v1_url)
            self.v2_engine = create_engine(v2_url)
            
            print(f"✅ Database credentials obtained via SecretsService")
            print(f"   DB Host: {db_host}:{db_port}")
            print(f"   DB User: {db_user}")
            
            # Test connections
            self._test_connections()
            
        except Exception as e:
            print(f"❌ Failed to setup database connections: {e}")
            raise
    
    def _test_connections(self):
        """Test both database connections."""
        try:
            with self.v1_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"✅ V1 database connection ({self.v1_db_name}): OK")
        except Exception as e:
            raise Exception(f"Failed to connect to v1 database {self.v1_db_name}: {e}")
            
        try:
            with self.v2_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"✅ V2 database connection ({self.v2_db_name}): OK")
        except Exception as e:
            raise Exception(f"Failed to connect to v2 database {self.v2_db_name}: {e}")
    
    def get_v1_schema_info(self) -> Dict:
        """Get v1 table schema information."""
        print(f"\n📋 Analyzing v1 liquidity table schema...")
        
        schema_query = text("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = 'liquidity' 
              AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        with self.v1_engine.connect() as conn:
            result = conn.execute(schema_query)
            columns = [dict(row._mapping) for row in result]
            
        print(f"   Found {len(columns)} columns:")
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']:<15} {col['data_type']:<20} {nullable}")
            
        return {"columns": columns}
    
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
        print(f"\n🚚 Migrating liquidity data from {self.v1_db_name} to {self.v2_db_name}...")
        
        # Get all data from v1
        select_query = text("""
            SELECT 
                content_id,
                tx_hash,
                block_number,
                timestamp,
                pool,
                provider,
                action,
                base_token,
                base_amount,
                quote_token,
                quote_amount
            FROM liquidity
            ORDER BY block_number, timestamp
        """)
        
        # Prepare insert query for v2
        insert_query = text("""
            INSERT INTO liquidity (
                content_id,
                tx_hash,
                block_number,
                timestamp,
                pool,
                provider,
                action,
                base_token,
                base_amount,
                quote_token,
                quote_amount
            ) VALUES (
                :content_id,
                :tx_hash,
                :block_number,
                :timestamp,
                :pool,
                :provider,
                :action,
                :base_token,
                :base_amount,
                :quote_token,
                :quote_amount
            )
        """)
        
        # Execute migration
        with self.v1_engine.connect() as v1_conn:
            v1_data = v1_conn.execute(select_query)
            rows_to_migrate = [dict(row._mapping) for row in v1_data]
        
        print(f"   Fetched {len(rows_to_migrate)} rows from v1")
        
        with self.v2_engine.connect() as v2_conn:
            trans = v2_conn.begin()
            try:
                # Clear existing data first (in case of re-migration)
                v2_conn.execute(text("DELETE FROM liquidity"))
                print(f"   Cleared existing v2 data")
                
                # Insert new data
                if rows_to_migrate:
                    v2_conn.execute(insert_query, rows_to_migrate)
                    print(f"   Inserted {len(rows_to_migrate)} rows into v2")
                else:
                    print(f"   No data to migrate")
                
                trans.commit()
                print(f"   ✅ Migration committed successfully")
                
                return {"migrated_rows": len(rows_to_migrate), "success": True}
                
            except Exception as e:
                trans.rollback()
                print(f"   ❌ Migration failed, rolled back: {e}")
                raise
    
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
            print(f"\n📋 MIGRATION SUMMARY: liquidity")
            print("=" * 50)
            print(f"Source: {self.v1_db_name}")
            print(f"Target: {self.v2_db_name}")
            print(f"Rows migrated: {migration_result.get('migrated_rows', 0)}")
            print(f"Validation: {'PASSED' if validation_result.get('validation_passed', False) else 'FAILED'}")
            
            overall_success = migration_result.get("success", False) and validation_result.get("validation_passed", False)
            print(f"Overall status: {'✅ SUCCESS' if overall_success else '❌ FAILED'}")
            
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
    args = parser.parse_args()
    
    migrator = ProperLiquidityMigrator(v1_db_name=args.v1_db, v2_db_name=args.v2_db)
    result = migrator.run_full_migration()
    
    if not result["success"]:
        sys.exit(1)


if __name__ == "__main__":
    main()