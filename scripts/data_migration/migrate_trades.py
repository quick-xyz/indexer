#!/usr/bin/env python3
"""
Trades Table Migration Script

Migrates trades table data from v1 to v2 database using the established pattern.
All fields map directly - no transformations needed.
"""

import sys
from pathlib import Path
from typing import Dict
from sqlalchemy import text, create_engine

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# No imports needed - will use os.environ and dynamic imports like pool_swaps pattern


class TradesMigrator:
    """Migrate trades table from v1 to v2 database."""
    
    def __init__(self, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2"):
        self.v1_db_name = v1_db_name
        self.v2_db_name = v2_db_name
        
        print(f"🔧 Initializing trades migration")
        print(f"   V1 DB: {v1_db_name}")
        print(f"   V2 DB: {v2_db_name}")
        
        # Use the same credential pattern as successful migrations
        self._setup_database_connections()
        
    def _setup_database_connections(self):
        """Setup database connections using infrastructure DB pattern."""
        print("🔗 Setting up database connections using infrastructure DB pattern...")
        
        try:
            # Use the same pattern as successful pool_swaps migration
            import os
            env = os.environ
            project_id = env.get("INDEXER_GCP_PROJECT_ID")
            
            if project_id:
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
            
            # Build URLs for both v1 and v2 databases
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
    
    def analyze_v1_data(self) -> Dict:
        """Analyze v1 trades table for migration planning."""
        print(f"\n📊 Analyzing v1 trades data for migration...")
        
        queries = {
            "row_count": "SELECT COUNT(*) as count FROM trades",
            "direction_distribution": """
                SELECT direction, COUNT(*) as count 
                FROM trades 
                GROUP BY direction 
                ORDER BY count DESC
            """,
            "trade_type_distribution": """
                SELECT trade_type, COUNT(*) as count 
                FROM trades 
                GROUP BY trade_type 
                ORDER BY count DESC
            """,
            "router_stats": """
                SELECT 
                    COUNT(DISTINCT taker) as unique_takers,
                    COUNT(DISTINCT base_token) as unique_base_tokens,
                    COUNT(DISTINCT router) as unique_routers,
                    COUNT(*) FILTER (WHERE router IS NOT NULL) as with_router,
                    COUNT(*) FILTER (WHERE router IS NULL) as without_router
                FROM trades
            """,
            "swap_count_stats": """
                SELECT 
                    MIN(swap_count) as min_swaps,
                    MAX(swap_count) as max_swaps,
                    AVG(swap_count) as avg_swaps,
                    COUNT(*) FILTER (WHERE swap_count IS NULL) as null_swap_counts
                FROM trades
            """,
            "block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM trades
            """,
            "sample_data": """
                SELECT taker, direction, base_token, base_amount, trade_type, router, swap_count, content_id, block_number 
                FROM trades 
                ORDER BY content_id 
                LIMIT 3
            """
        }
        
        results = {}
        with self.v1_engine.connect() as conn:
            for key, query in queries.items():
                result = conn.execute(text(query))
                if key == "sample_data":
                    results[key] = [dict(row._mapping) for row in result]
                elif key == "direction_distribution":
                    results[key] = {row._mapping["direction"]: row._mapping["count"] for row in result}
                elif key == "trade_type_distribution":
                    results[key] = {row._mapping["trade_type"]: row._mapping["count"] for row in result}
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Print analysis
        print(f"   📈 Total rows: {results['row_count']['count']}")
        print(f"   📊 Directions: {results['direction_distribution']}")
        print(f"   🎯 Trade types: {results['trade_type_distribution']}")
        print(f"   👥 Unique takers: {results['router_stats']['unique_takers']}")
        print(f"   💰 Unique base tokens: {results['router_stats']['unique_base_tokens']}")
        print(f"   🔀 Router usage: {results['router_stats']['with_router']} with router, {results['router_stats']['without_router']} without")
        print(f"   🔢 Swap counts: {results['swap_count_stats']['min_swaps']}-{results['swap_count_stats']['max_swaps']} (avg: {results['swap_count_stats']['avg_swaps']:.1f})")
        print(f"   📊 Block range: {results['block_range']['min_block']} - {results['block_range']['max_block']}")
        
        return results
    
    def migrate_data(self) -> Dict:
        """Migrate trades data from v1 to v2 - all fields map directly."""
        print(f"\n🚚 Migrating trades data from {self.v1_db_name} to {self.v2_db_name}...")
        
        # Get all data from v1 - all fields map directly (perfect 1:1 mapping)
        select_query = text("""
            SELECT 
                taker,
                direction,
                base_token,
                base_amount,
                trade_type,
                router,
                swap_count,
                content_id,
                tx_hash,
                block_number,
                timestamp
            FROM trades
            ORDER BY content_id
        """)
        
        # Prepare insert query for v2 - same field names
        insert_query = text("""
            INSERT INTO trades (
                taker,
                direction,
                base_token,
                base_amount,
                trade_type,
                router,
                swap_count,
                content_id,
                tx_hash,
                block_number,
                timestamp
            ) VALUES (
                :taker,
                :direction,
                :base_token,
                :base_amount,
                :trade_type,
                :router,
                :swap_count,
                :content_id,
                :tx_hash,
                :block_number,
                :timestamp
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
                v2_conn.execute(text("DELETE FROM trades"))
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
        """Validate trades migration with detailed checks."""
        print(f"\n🔍 Validating trades migration...")
        
        validation_queries = {
            "v1_count": "SELECT COUNT(*) as count FROM trades",
            "v2_count": "SELECT COUNT(*) as count FROM trades",
            "v1_directions": """
                SELECT direction, COUNT(*) as count 
                FROM trades 
                GROUP BY direction 
                ORDER BY direction
            """,
            "v2_directions": """
                SELECT direction, COUNT(*) as count 
                FROM trades 
                GROUP BY direction 
                ORDER BY direction
            """,
            "v1_trade_types": """
                SELECT trade_type, COUNT(*) as count 
                FROM trades 
                GROUP BY trade_type 
                ORDER BY trade_type
            """,
            "v2_trade_types": """
                SELECT trade_type, COUNT(*) as count 
                FROM trades 
                GROUP BY trade_type 
                ORDER BY trade_type
            """,
            "v1_router_stats": """
                SELECT 
                    COUNT(DISTINCT taker) as unique_takers,
                    COUNT(DISTINCT base_token) as unique_base_tokens,
                    COUNT(*) FILTER (WHERE router IS NOT NULL) as with_router,
                    COUNT(*) FILTER (WHERE router IS NULL) as without_router
                FROM trades
            """,
            "v2_router_stats": """
                SELECT 
                    COUNT(DISTINCT taker) as unique_takers,
                    COUNT(DISTINCT base_token) as unique_base_tokens,
                    COUNT(*) FILTER (WHERE router IS NOT NULL) as with_router,
                    COUNT(*) FILTER (WHERE router IS NULL) as without_router
                FROM trades
            """,
            "v1_block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM trades
            """,
            "v2_block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM trades
            """,
            "v1_sample": """
                SELECT taker, direction, base_token, base_amount, trade_type, router, swap_count, content_id
                FROM trades 
                ORDER BY content_id 
                LIMIT 5
            """,
            "v2_sample": """
                SELECT taker, direction, base_token, base_amount, trade_type, router, swap_count, content_id
                FROM trades 
                ORDER BY content_id 
                LIMIT 5
            """
        }
        
        results = {}
        
        # Run validation queries
        with self.v1_engine.connect() as v1_conn:
            for key, query in validation_queries.items():
                if not key.startswith("v1_"):
                    continue
                result = v1_conn.execute(text(query))
                if key == "v1_sample":
                    results[key] = [dict(row._mapping) for row in result]
                elif key == "v1_directions":
                    results[key] = {row._mapping["direction"]: row._mapping["count"] for row in result}
                elif key == "v1_trade_types":
                    results[key] = {row._mapping["trade_type"]: row._mapping["count"] for row in result}
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        with self.v2_engine.connect() as v2_conn:
            for key, query in validation_queries.items():
                if not key.startswith("v2_"):
                    continue
                result = v2_conn.execute(text(query))
                if key == "v2_sample":
                    results[key] = [dict(row._mapping) for row in result]
                elif key == "v2_directions":
                    results[key] = {row._mapping["direction"]: row._mapping["count"] for row in result}
                elif key == "v2_trade_types":
                    results[key] = {row._mapping["trade_type"]: row._mapping["count"] for row in result}
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Validate results
        validation_passed = True
        
        # Check row counts
        v1_count = results["v1_count"]["count"]
        v2_count = results["v2_count"]["count"]
        print(f"   📊 Row counts: V1={v1_count}, V2={v2_count}")
        if v1_count != v2_count:
            print(f"   ❌ Row count mismatch!")
            validation_passed = False
        else:
            print(f"   ✅ Row counts match")
        
        # Check direction distributions
        v1_directions = results["v1_directions"]
        v2_directions = results["v2_directions"]
        if v1_directions == v2_directions:
            print(f"   ✅ Direction distributions match: {v1_directions}")
        else:
            print(f"   ❌ Direction distributions mismatch:")
            print(f"     V1: {v1_directions}")
            print(f"     V2: {v2_directions}")
            validation_passed = False
        
        # Check trade type distributions
        v1_types = results["v1_trade_types"]
        v2_types = results["v2_trade_types"]
        if v1_types == v2_types:
            print(f"   ✅ Trade type distributions match: {v1_types}")
        else:
            print(f"   ❌ Trade type distributions mismatch:")
            print(f"     V1: {v1_types}")
            print(f"     V2: {v2_types}")
            validation_passed = False
        
        # Check router stats
        v1_router = results["v1_router_stats"]
        v2_router = results["v2_router_stats"]
        if v1_router == v2_router:
            print(f"   ✅ Router stats match: {v1_router['unique_takers']} takers, {v1_router['with_router']} with router, {v1_router['without_router']} without")
        else:
            print(f"   ❌ Router stats mismatch:")
            print(f"     V1: {v1_router}")
            print(f"     V2: {v2_router}")
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
        
        # Check sample data
        v1_sample = results["v1_sample"]
        v2_sample = results["v2_sample"]
        if v1_sample == v2_sample:
            print(f"   ✅ Sample data matches (first 5 records)")
        else:
            print(f"   ❌ Sample data mismatch:")
            print(f"     V1 first record: {v1_sample[0] if v1_sample else 'None'}")
            print(f"     V2 first record: {v2_sample[0] if v2_sample else 'None'}")
            validation_passed = False
        
        print(f"\n{'✅ DETAILED VALIDATION PASSED' if validation_passed else '❌ DETAILED VALIDATION FAILED'}")
        
        return {
            "validation_passed": validation_passed,
            "v1_count": v1_count,
            "v2_count": v2_count,
            "details": results
        }
    
    def run_full_migration(self) -> Dict:
        """Run complete trades migration with validation."""
        print(f"🚀 Starting trades table migration: {self.v1_db_name} → {self.v2_db_name}")
        print("=" * 80)
        
        try:
            # Step 1: Analyze source data
            analysis = self.analyze_v1_data()
            
            # Step 2: Migrate data
            migration_result = self.migrate_data()
            
            # Step 3: Validate migration
            validation_result = self.validate_migration()
            
            # Final summary
            success = migration_result["success"] and validation_result["validation_passed"]
            
            print(f"\n🎯 TRADES MIGRATION SUMMARY")
            print("=" * 40)
            print(f"✅ Migration: {'SUCCESS' if migration_result['success'] else 'FAILED'}")
            print(f"✅ Validation: {'PASSED' if validation_result['validation_passed'] else 'FAILED'}")
            print(f"📊 Rows migrated: {migration_result['migrated_rows']}")
            print(f"🎯 Overall result: {'✅ SUCCESS' if success else '❌ FAILED'}")
            
            return {
                "success": success,
                "migrated_rows": migration_result["migrated_rows"],
                "analysis": analysis,
                "validation": validation_result
            }
            
        except Exception as e:
            print(f"\n❌ MIGRATION FAILED WITH ERROR: {e}")
            raise


def main():
    """Main entry point for trades migration script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate trades table from v1 to v2 database")
    parser.add_argument("--v1-db", default="blub_test", help="Source database name (default: blub_test)")
    parser.add_argument("--v2-db", default="blub_test_v2", help="Target database name (default: blub_test_v2)")
    
    args = parser.parse_args()
    
    migrator = TradesMigrator(v1_db_name=args.v1_db, v2_db_name=args.v2_db)
    result = migrator.run_full_migration()
    
    if result["success"]:
        print(f"\n🎉 Trades migration completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Trades migration failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()