#!/usr/bin/env python3
"""
Transaction Processing Table Migration Script

Migrates transaction_processing table data from v1 to v2 database.
Note: V2 schema excludes signals_generated, positions_generated, tx_success fields.
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


class TransactionProcessingMigrator:
    """Migrate transaction_processing table from v1 to v2 database."""
    
    def __init__(self, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2"):
        self.v1_db_name = v1_db_name
        self.v2_db_name = v2_db_name
        
        print(f"🔧 Initializing transaction_processing migration")
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
        """Analyze v1 transaction_processing table for migration planning."""
        print(f"\n📊 Analyzing v1 transaction_processing data for migration...")
        
        queries = {
            "row_count": "SELECT COUNT(*) as count FROM transaction_processing",
            "status_distribution": """
                SELECT status, COUNT(*) as count 
                FROM transaction_processing 
                GROUP BY status 
                ORDER BY count DESC
            """,
            "retry_stats": """
                SELECT 
                    MIN(retry_count) as min_retries,
                    MAX(retry_count) as max_retries,
                    AVG(retry_count) as avg_retries,
                    COUNT(*) FILTER (WHERE retry_count > 0) as with_retries
                FROM transaction_processing
            """,
            "processing_stats": """
                SELECT 
                    MIN(logs_processed) as min_logs,
                    MAX(logs_processed) as max_logs,
                    AVG(logs_processed) as avg_logs,
                    MIN(events_generated) as min_events,
                    MAX(events_generated) as max_events,
                    AVG(events_generated) as avg_events
                FROM transaction_processing
            """,
            "gas_stats": """
                SELECT 
                    COUNT(*) FILTER (WHERE gas_used IS NOT NULL) as with_gas_used,
                    COUNT(*) FILTER (WHERE gas_price IS NOT NULL) as with_gas_price,
                    COUNT(*) FILTER (WHERE error_message IS NOT NULL) as with_errors
                FROM transaction_processing
            """,
            "v1_only_fields": """
                SELECT 
                    COUNT(*) FILTER (WHERE signals_generated IS NOT NULL) as with_signals,
                    COUNT(*) FILTER (WHERE positions_generated IS NOT NULL) as with_positions,
                    COUNT(*) FILTER (WHERE tx_success IS NOT NULL) as with_tx_success
                FROM transaction_processing
            """,
            "block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM transaction_processing
            """,
            "sample_data": """
                SELECT block_number, tx_hash, status, retry_count, logs_processed, events_generated, id
                FROM transaction_processing 
                ORDER BY block_number DESC 
                LIMIT 3
            """
        }
        
        results = {}
        with self.v1_engine.connect() as conn:
            for key, query in queries.items():
                result = conn.execute(text(query))
                if key == "sample_data":
                    results[key] = [dict(row._mapping) for row in result]
                elif key == "status_distribution":
                    results[key] = {row._mapping["status"]: row._mapping["count"] for row in result}
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Print analysis
        print(f"   📈 Total rows: {results['row_count']['count']}")
        print(f"   📊 Status distribution: {results['status_distribution']}")
        print(f"   🔄 Retry stats: {results['retry_stats']['min_retries']}-{results['retry_stats']['max_retries']} retries (avg: {results['retry_stats']['avg_retries']:.1f}), {results['retry_stats']['with_retries']} with retries")
        print(f"   📝 Processing stats: {results['processing_stats']['min_logs']}-{results['processing_stats']['max_logs']} logs (avg: {results['processing_stats']['avg_logs']:.1f})")
        print(f"   🎯 Events stats: {results['processing_stats']['min_events']}-{results['processing_stats']['max_events']} events (avg: {results['processing_stats']['avg_events']:.1f})")
        print(f"   ⛽ Gas info: {results['gas_stats']['with_gas_used']} with gas_used, {results['gas_stats']['with_gas_price']} with gas_price")
        print(f"   ❌ Errors: {results['gas_stats']['with_errors']} transactions with error messages")
        print(f"   📊 Block range: {results['block_range']['min_block']} - {results['block_range']['max_block']}")
        
        # Warn about fields that will be dropped
        v1_only = results['v1_only_fields']
        print(f"   ⚠️  V1-only fields to be dropped:")
        print(f"      - signals_generated: {v1_only['with_signals']} non-null values")
        print(f"      - positions_generated: {v1_only['with_positions']} non-null values") 
        print(f"      - tx_success: {v1_only['with_tx_success']} non-null values")
        
        return results
    
    def migrate_data(self) -> Dict:
        """Migrate transaction_processing data from v1 to v2 - drop V1-only fields."""
        print(f"\n🚚 Migrating transaction_processing data from {self.v1_db_name} to {self.v2_db_name}...")
        print(f"   ⚠️  Note: Dropping signals_generated, positions_generated, tx_success fields (not in V2 schema)")
        
        # Get data from v1 - select only fields that exist in V2
        select_query = text("""
            SELECT 
                id,
                block_number,
                tx_hash,
                tx_index,
                timestamp,
                status,
                retry_count,
                last_processed_at,
                gas_used,
                gas_price,
                error_message,
                logs_processed,
                events_generated,
                created_at,
                updated_at
            FROM transaction_processing
            ORDER BY block_number, tx_index
        """)
        
        # Prepare insert query for v2 - only V2 schema fields
        insert_query = text("""
            INSERT INTO transaction_processing (
                id,
                block_number,
                tx_hash,
                tx_index,
                timestamp,
                status,
                retry_count,
                last_processed_at,
                gas_used,
                gas_price,
                error_message,
                logs_processed,
                events_generated,
                created_at,
                updated_at
            ) VALUES (
                :id,
                :block_number,
                :tx_hash,
                :tx_index,
                :timestamp,
                :status,
                :retry_count,
                :last_processed_at,
                :gas_used,
                :gas_price,
                :error_message,
                :logs_processed,
                :events_generated,
                :created_at,
                :updated_at
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
                v2_conn.execute(text("DELETE FROM transaction_processing"))
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
        """Validate transaction_processing migration with detailed checks."""
        print(f"\n🔍 Validating transaction_processing migration...")
        
        validation_queries = {
            "v1_count": "SELECT COUNT(*) as count FROM transaction_processing",
            "v2_count": "SELECT COUNT(*) as count FROM transaction_processing",
            "v1_status": """
                SELECT status, COUNT(*) as count 
                FROM transaction_processing 
                GROUP BY status 
                ORDER BY status
            """,
            "v2_status": """
                SELECT status, COUNT(*) as count 
                FROM transaction_processing 
                GROUP BY status 
                ORDER BY status
            """,
            "v1_processing_stats": """
                SELECT 
                    MIN(logs_processed) as min_logs,
                    MAX(logs_processed) as max_logs,
                    MIN(events_generated) as min_events,
                    MAX(events_generated) as max_events
                FROM transaction_processing
            """,
            "v2_processing_stats": """
                SELECT 
                    MIN(logs_processed) as min_logs,
                    MAX(logs_processed) as max_logs,
                    MIN(events_generated) as min_events,
                    MAX(events_generated) as max_events
                FROM transaction_processing
            """,
            "v1_block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM transaction_processing
            """,
            "v2_block_range": """
                SELECT 
                    MIN(block_number) as min_block,
                    MAX(block_number) as max_block
                FROM transaction_processing
            """,
            "v1_sample": """
                SELECT id, block_number, tx_hash, status, logs_processed, events_generated
                FROM transaction_processing 
                ORDER BY block_number DESC 
                LIMIT 5
            """,
            "v2_sample": """
                SELECT id, block_number, tx_hash, status, logs_processed, events_generated
                FROM transaction_processing 
                ORDER BY block_number DESC 
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
                elif key == "v1_status":
                    results[key] = {row._mapping["status"]: row._mapping["count"] for row in result}
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        with self.v2_engine.connect() as v2_conn:
            for key, query in validation_queries.items():
                if not key.startswith("v2_"):
                    continue
                result = v2_conn.execute(text(query))
                if key == "v2_sample":
                    results[key] = [dict(row._mapping) for row in result]
                elif key == "v2_status":
                    results[key] = {row._mapping["status"]: row._mapping["count"] for row in result}
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
        
        # Check status distributions
        v1_status = results["v1_status"]
        v2_status = results["v2_status"]
        if v1_status == v2_status:
            print(f"   ✅ Status distributions match: {v1_status}")
        else:
            print(f"   ❌ Status distributions mismatch:")
            print(f"     V1: {v1_status}")
            print(f"     V2: {v2_status}")
            validation_passed = False
        
        # Check processing stats
        v1_processing = results["v1_processing_stats"]
        v2_processing = results["v2_processing_stats"]
        if v1_processing == v2_processing:
            print(f"   ✅ Processing stats match: logs {v1_processing['min_logs']}-{v1_processing['max_logs']}, events {v1_processing['min_events']}-{v1_processing['max_events']}")
        else:
            print(f"   ❌ Processing stats mismatch:")
            print(f"     V1: {v1_processing}")
            print(f"     V2: {v2_processing}")
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
        
        # Check sample data (compare only common fields)
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
        """Run complete transaction_processing migration with validation."""
        print(f"🚀 Starting transaction_processing table migration: {self.v1_db_name} → {self.v2_db_name}")
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
            
            print(f"\n🎯 TRANSACTION_PROCESSING MIGRATION SUMMARY")
            print("=" * 50)
            print(f"✅ Migration: {'SUCCESS' if migration_result['success'] else 'FAILED'}")
            print(f"✅ Validation: {'PASSED' if validation_result['validation_passed'] else 'FAILED'}")
            print(f"📊 Rows migrated: {migration_result['migrated_rows']}")
            print(f"⚠️  Fields dropped: signals_generated, positions_generated, tx_success")
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
    """Main entry point for transaction_processing migration script."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate transaction_processing table from v1 to v2 database")
    parser.add_argument("--v1-db", default="blub_test", help="Source database name (default: blub_test)")
    parser.add_argument("--v2-db", default="blub_test_v2", help="Target database name (default: blub_test_v2)")
    
    args = parser.parse_args()
    
    migrator = TransactionProcessingMigrator(v1_db_name=args.v1_db, v2_db_name=args.v2_db)
    result = migrator.run_full_migration()
    
    if result["success"]:
        print(f"\n🎉 Transaction processing migration completed successfully!")
        sys.exit(0)
    else:
        print(f"\n💥 Transaction processing migration failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()