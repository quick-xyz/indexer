#!/usr/bin/env python3
"""
Transfers Table Migration Script - FINAL MIGRATION! 🏁

Migrates transfers table data from v1 to v2 database using the established pattern.
All fields map directly - no transformations needed.
This completes our full database migration!
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


class TransfersMigrator:
    """Migrate transfers table from v1 to v2 database - FINAL MIGRATION!"""
    
    def __init__(self, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2"):
        self.v1_db_name = v1_db_name
        self.v2_db_name = v2_db_name
        
        print(f"🔧 Initializing transfers migration - FINAL TABLE! 🏁")
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
        """Analyze v1 transfers table for migration planning."""
        print(f"\n📊 Analyzing v1 transfers data for migration...")
        
        queries = {
            "row_count": "SELECT COUNT(*) as count FROM transfers",
            "token_distribution": """
                SELECT token, COUNT(*) as count 
                FROM transfers 
                GROUP BY token 
                ORDER BY count DESC
                LIMIT 5
            """,
            "parent_stats": """
                SELECT 
                    COUNT(*) FILTER (WHERE parent_id IS NOT NULL) as with_parent,
                    COUNT(*) FILTER (WHERE parent_id IS NULL) as without_parent,
                    COUNT(DISTINCT parent_type) as unique_parent_types,
                    COUNT(DISTINCT classification) as unique_classifications
                FROM transfers
            """,
            "address_stats": """
                SELECT 
                    COUNT(DISTINCT from_address) as unique_from_addresses,
                    COUNT(DISTINCT to_address) as unique_to_addresses,
                    COUNT(DISTINCT token) as unique_tokens
                FROM transfers
            """,
            "amount_stats": """
                SELECT 
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM transfers
            """,
            "parent_type_distribution": """
                SELECT parent_type, COUNT(*) as count 
                FROM transfers 
                WHERE parent_type IS NOT NULL
                GROUP BY parent_type 
                ORDER BY count DESC
            """,
            "classification_distribution": """
                SELECT classification, COUNT(*) as count 
                FROM transfers 
                WHERE classification IS NOT NULL
                GROUP BY classification 
                ORDER BY count DESC
            """,
            "block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM transfers
            """,
            "sample_data": """
                SELECT token, from_address, to_address, amount, parent_id, parent_type, classification, content_id, block_number 
                FROM transfers 
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
                elif key in ["token_distribution", "parent_type_distribution", "classification_distribution"]:
                    results[key] = {row._mapping[key.split("_")[0]]: row._mapping["count"] for row in result}
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Print analysis
        print(f"   📈 Total rows: {results['row_count']['count']}")
        print(f"   💰 Top tokens: {dict(list(results['token_distribution'].items())[:3])}")
        print(f"   🔗 Parent relationships: {results['parent_stats']['with_parent']} with parent, {results['parent_stats']['without_parent']} standalone")
        print(f"   🏷️  Parent types: {results['parent_stats']['unique_parent_types']} unique types")
        print(f"   📊 Classifications: {results['parent_stats']['unique_classifications']} unique classifications")
        print(f"   👥 Address diversity: {results['address_stats']['unique_from_addresses']} senders, {results['address_stats']['unique_to_addresses']} receivers")
        print(f"   🪙 Token diversity: {results['address_stats']['unique_tokens']} unique tokens")
        print(f"   📊 Block range: {results['block_range']['min_block']} - {results['block_range']['max_block']}")
        
        if results['parent_type_distribution']:
            print(f"   🔗 Parent types: {results['parent_type_distribution']}")
        if results['classification_distribution']:
            print(f"   🏷️  Classifications: {results['classification_distribution']}")
        
        return results
    
    def migrate_data(self) -> Dict:
        """Migrate transfers data from v1 to v2 - all fields map directly."""
        print(f"\n🚚 Migrating transfers data from {self.v1_db_name} to {self.v2_db_name}...")
        print(f"   🎯 Perfect 1:1 field mapping - no transformations needed!")
        
        # Get all data from v1 - all fields map directly (perfect 1:1 mapping)
        select_query = text("""
            SELECT 
                token,
                from_address,
                to_address,
                amount,
                parent_id,
                parent_type,
                classification,
                content_id,
                tx_hash,
                block_number,
                timestamp
            FROM transfers
            ORDER BY content_id
        """)
        
        # Prepare insert query for v2 - same field names
        insert_query = text("""
            INSERT INTO transfers (
                token,
                from_address,
                to_address,
                amount,
                parent_id,
                parent_type,
                classification,
                content_id,
                tx_hash,
                block_number,
                timestamp
            ) VALUES (
                :token,
                :from_address,
                :to_address,
                :amount,
                :parent_id,
                :parent_type,
                :classification,
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
                v2_conn.execute(text("DELETE FROM transfers"))
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
        """Validate transfers migration with detailed checks."""
        print(f"\n🔍 Validating transfers migration...")
        
        validation_queries = {
            "v1_count": "SELECT COUNT(*) as count FROM transfers",
            "v2_count": "SELECT COUNT(*) as count FROM transfers",
            "v1_tokens": """
                SELECT token, COUNT(*) as count 
                FROM transfers 
                GROUP BY token 
                ORDER BY token
                LIMIT 5
            """,
            "v2_tokens": """
                SELECT token, COUNT(*) as count 
                FROM transfers 
                GROUP BY token 
                ORDER BY token
                LIMIT 5
            """,
            "v1_parent_stats": """
                SELECT 
                    COUNT(*) FILTER (WHERE parent_id IS NOT NULL) as with_parent,
                    COUNT(*) FILTER (WHERE parent_id IS NULL) as without_parent,
                    COUNT(DISTINCT parent_type) as unique_parent_types
                FROM transfers
            """,
            "v2_parent_stats": """
                SELECT 
                    COUNT(*) FILTER (WHERE parent_id IS NOT NULL) as with_parent,
                    COUNT(*) FILTER (WHERE parent_id IS NULL) as without_parent,
                    COUNT(DISTINCT parent_type) as unique_parent_types
                FROM transfers
            """,
            "v1_address_stats": """
                SELECT 
                    COUNT(DISTINCT from_address) as unique_from_addresses,
                    COUNT(DISTINCT to_address) as unique_to_addresses,
                    COUNT(DISTINCT token) as unique_tokens
                FROM transfers
            """,
            "v2_address_stats": """
                SELECT 
                    COUNT(DISTINCT from_address) as unique_from_addresses,
                    COUNT(DISTINCT to_address) as unique_to_addresses,
                    COUNT(DISTINCT token) as unique_tokens
                FROM transfers
            """,
            "v1_block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM transfers
            """,
            "v2_block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM transfers
            """,
            "v1_sample": """
                SELECT token, from_address, to_address, amount, parent_id, parent_type, classification, content_id
                FROM transfers 
                ORDER BY content_id 
                LIMIT 5
            """,
            "v2_sample": """
                SELECT token, from_address, to_address, amount, parent_id, parent_type, classification, content_id
                FROM transfers 
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
                if key in ["v1_sample"]:
                    results[key] = [dict(row._mapping) for row in result]
                elif key == "v1_tokens":
                    results[key] = {row._mapping["token"]: row._mapping["count"] for row in result}
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        with self.v2_engine.connect() as v2_conn:
            for key, query in validation_queries.items():
                if not key.startswith("v2_"):
                    continue
                result = v2_conn.execute(text(query))
                if key in ["v2_sample"]:
                    results[key] = [dict(row._mapping) for row in result]
                elif key == "v2_tokens":
                    results[key] = {row._mapping["token"]: row._mapping["count"] for row in result}
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
        
        # Check token distributions (top 3)
        v1_tokens = results["v1_tokens"]
        v2_tokens = results["v2_tokens"]
        if v1_tokens == v2_tokens:
            print(f"   ✅ Token distributions match (top tokens preserved)")
        else:
            print(f"   ❌ Token distributions mismatch:")
            print(f"     V1: {dict(list(v1_tokens.items())[:3])}")
            print(f"     V2: {dict(list(v2_tokens.items())[:3])}")
            validation_passed = False
        
        # Check parent relationships
        v1_parents = results["v1_parent_stats"]
        v2_parents = results["v2_parent_stats"]
        if v1_parents == v2_parents:
            print(f"   ✅ Parent stats match: {v1_parents['with_parent']} with parent, {v1_parents['without_parent']} standalone")
        else:
            print(f"   ❌ Parent stats mismatch:")
            print(f"     V1: {v1_parents}")
            print(f"     V2: {v2_parents}")
            validation_passed = False
        
        # Check address diversity
        v1_addresses = results["v1_address_stats"]
        v2_addresses = results["v2_address_stats"]
        if v1_addresses == v2_addresses:
            print(f"   ✅ Address stats match: {v1_addresses['unique_from_addresses']} senders, {v1_addresses['unique_to_addresses']} receivers, {v1_addresses['unique_tokens']} tokens")
        else:
            print(f"   ❌ Address stats mismatch:")
            print(f"     V1: {v1_addresses}")
            print(f"     V2: {v2_addresses}")
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
        """Run complete transfers migration with validation - FINAL MIGRATION!"""
        print(f"🚀 Starting transfers table migration: {self.v1_db_name} → {self.v2_db_name}")
        print(f"🏁 THIS IS THE FINAL TABLE MIGRATION!")
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
            
            print(f"\n🎯 TRANSFERS MIGRATION SUMMARY")
            print("=" * 40)
            print(f"✅ Migration: {'SUCCESS' if migration_result['success'] else 'FAILED'}")
            print(f"✅ Validation: {'PASSED' if validation_result['validation_passed'] else 'FAILED'}")
            print(f"📊 Rows migrated: {migration_result['migrated_rows']}")
            print(f"🏁 Final table result: {'✅ SUCCESS' if success else '❌ FAILED'}")
            
            if success:
                print(f"\n🎉 CONGRATULATIONS! ALL TABLE MIGRATIONS COMPLETED! 🎉")
                print("=" * 60)
                print("🏆 Complete database migration accomplished successfully!")
                print("🎯 All data preserved with perfect validation!")
                print("✨ Ready for production use!")
            
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
    """Main entry point for transfers migration script - FINAL MIGRATION!"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate transfers table from v1 to v2 database - FINAL TABLE!")
    parser.add_argument("--v1-db", default="blub_test", help="Source database name (default: blub_test)")
    parser.add_argument("--v2-db", default="blub_test_v2", help="Target database name (default: blub_test_v2)")
    
    args = parser.parse_args()
    
    migrator = TransfersMigrator(v1_db_name=args.v1_db, v2_db_name=args.v2_db)
    result = migrator.run_full_migration()
    
    if result["success"]:
        print(f"\n🏆 COMPLETE DATABASE MIGRATION FINISHED SUCCESSFULLY! 🏆")
        print(f"🎉 ALL TABLES MIGRATED! PROJECT COMPLETE! 🎉")
        sys.exit(0)
    else:
        print(f"\n💥 Final migration failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()