#!/usr/bin/env python3
"""
Positions Table Migration Script

Migrates positions table data from v1 to v2 database using the established pattern.

V1 Schema (256,624 rows):
- user, custodian, token, amount, token_id, parent_id, parent_type
- content_id, tx_hash, block_number, timestamp
- created_at, updated_at (skip - auto-generated in v2)

V2 Schema: Direct 1:1 mapping, no transformations needed.
"""

import sys
from pathlib import Path
from typing import Dict
from sqlalchemy import text, create_engine

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class PositionsMigrator:
    """Migrate positions table from v1 to v2 database."""
    
    def __init__(self, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2"):
        self.v1_db_name = v1_db_name
        self.v2_db_name = v2_db_name
        
        print(f"🔧 Initializing positions migration")
        print(f"   V1 DB: {v1_db_name}")
        print(f"   V2 DB: {v2_db_name}")
        print(f"   Expected rows: ~256,624")
        
        # Use the exact same credential pattern as pool_swaps migration
        self._setup_database_connections()
        
    def _setup_database_connections(self):
        """Setup database connections using infrastructure DB pattern."""
        print("🔗 Setting up database connections using infrastructure DB pattern...")
        
        try:
            # Use the same pattern as pool_swaps migration
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
        print("🔍 Testing database connections...")
        
        try:
            # Test v1 connection and check table exists
            with self.v1_engine.connect() as v1_conn:
                v1_check = v1_conn.execute(text("SELECT COUNT(*) FROM positions"))
                v1_count = v1_check.scalar()
                print(f"   V1 ({self.v1_db_name}): ✅ Connected - {v1_count:,} rows in positions")
                
            # Test v2 connection and check table exists  
            with self.v2_engine.connect() as v2_conn:
                v2_check = v2_conn.execute(text("SELECT COUNT(*) FROM positions"))
                v2_count = v2_check.scalar()
                print(f"   V2 ({self.v2_db_name}): ✅ Connected - {v2_count:,} rows in positions")
                
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            raise
    
    def get_data_stats(self) -> Dict:
        """Get statistics about source data before migration."""
        print(f"\n📊 Analyzing positions data in {self.v1_db_name}...")
        
        with self.v1_engine.connect() as v1_conn:
            # Basic stats
            count_query = text("SELECT COUNT(*) FROM positions")
            total_rows = v1_conn.execute(count_query).scalar()
            
            # Distribution stats
            stats_query = text("""
                SELECT 
                    COUNT(DISTINCT user) as unique_users,
                    COUNT(DISTINCT token) as unique_tokens,
                    COUNT(CASE WHEN amount > 0 THEN 1 END) as positive_positions,
                    COUNT(CASE WHEN amount < 0 THEN 1 END) as negative_positions,
                    COUNT(CASE WHEN amount = 0 THEN 1 END) as zero_positions,
                    MIN(timestamp) as earliest_timestamp,
                    MAX(timestamp) as latest_timestamp,
                    COUNT(CASE WHEN parent_id IS NOT NULL THEN 1 END) as has_parent_id
                FROM positions
            """)
            stats = v1_conn.execute(stats_query).fetchone()
            
            results = {
                "total_rows": total_rows,
                "unique_users": stats.unique_users if stats else 0,
                "unique_tokens": stats.unique_tokens if stats else 0,
                "positive_positions": stats.positive_positions if stats else 0,
                "negative_positions": stats.negative_positions if stats else 0,
                "zero_positions": stats.zero_positions if stats else 0,
                "earliest_timestamp": stats.earliest_timestamp if stats else None,
                "latest_timestamp": stats.latest_timestamp if stats else None,
                "has_parent_id": stats.has_parent_id if stats else 0
            }
            
            print(f"   Total rows: {results['total_rows']:,}")
            print(f"   Unique users: {results['unique_users']:,}")
            print(f"   Unique tokens: {results['unique_tokens']:,}")
            print(f"   Amount distribution: +{results['positive_positions']:,} / -{results['negative_positions']:,} / 0:{results['zero_positions']:,}")
            print(f"   Positions with parent_id: {results['has_parent_id']:,}")
            if results['earliest_timestamp'] and results['latest_timestamp']:
                print(f"   Time range: {results['earliest_timestamp']} to {results['latest_timestamp']}")
            
            return results
    
    def migrate_data(self) -> Dict:
        """Migrate positions data from v1 to v2."""
        print(f"\n🚚 Migrating positions data from {self.v1_db_name} to {self.v2_db_name}...")
        
        # Get all data from v1 - direct field mapping based on schema analysis
        select_query = text("""
            SELECT 
                content_id,
                tx_hash,
                block_number,
                timestamp,
                user,
                custodian,
                token,
                amount,
                token_id,
                parent_id,
                parent_type
            FROM positions
            ORDER BY block_number, timestamp
        """)
        
        # Prepare insert query for v2 - direct 1:1 mapping
        # Note: "user" is a PostgreSQL reserved keyword, so it must be quoted
        insert_query = text("""
            INSERT INTO positions (
                content_id,
                tx_hash,
                block_number,
                timestamp,
                "user",
                custodian,
                token,
                amount,
                token_id,
                parent_id,
                parent_type
            ) VALUES (
                :content_id,
                :tx_hash,
                :block_number,
                :timestamp,
                :user,
                :custodian,
                :token,
                :amount,
                :token_id,
                :parent_id,
                :parent_type
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
                v2_conn.execute(text("DELETE FROM positions"))
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
        """Validate positions migration with comprehensive checks."""
        print(f"\n🔍 Validating positions migration...")
        
        validation_results = {}
        
        with self.v1_engine.connect() as v1_conn:
            with self.v2_engine.connect() as v2_conn:
                
                # 1. Row count validation
                v1_count = v1_conn.execute(text("SELECT COUNT(*) FROM positions")).scalar()
                v2_count = v2_conn.execute(text("SELECT COUNT(*) FROM positions")).scalar()
                
                validation_results["row_count"] = {
                    "v1_count": v1_count,
                    "v2_count": v2_count,
                    "match": v1_count == v2_count
                }
                
                print(f"   Row counts - V1: {v1_count:,}, V2: {v2_count:,} {'✅' if v1_count == v2_count else '❌'}")
                
                if v1_count == 0:
                    print(f"   ⚠️  No data to validate")
                    return validation_results
                
                # 2. User distribution validation
                v1_users = v1_conn.execute(text("SELECT COUNT(DISTINCT user) FROM positions")).scalar()
                v2_users = v2_conn.execute(text("SELECT COUNT(DISTINCT user) FROM positions")).scalar()
                
                validation_results["user_distribution"] = {
                    "v1_users": v1_users,
                    "v2_users": v2_users,
                    "match": v1_users == v2_users
                }
                
                print(f"   User counts - V1: {v1_users:,}, V2: {v2_users:,} {'✅' if v1_users == v2_users else '❌'}")
                
                # 3. Token distribution validation
                v1_tokens = v1_conn.execute(text("SELECT COUNT(DISTINCT token) FROM positions")).scalar()
                v2_tokens = v2_conn.execute(text("SELECT COUNT(DISTINCT token) FROM positions")).scalar()
                
                validation_results["token_distribution"] = {
                    "v1_tokens": v1_tokens,
                    "v2_tokens": v2_tokens,
                    "match": v1_tokens == v2_tokens
                }
                
                print(f"   Token counts - V1: {v1_tokens:,}, V2: {v2_tokens:,} {'✅' if v1_tokens == v2_tokens else '❌'}")
                
                # 4. Amount distribution validation
                v1_amounts = v1_conn.execute(text("""
                    SELECT 
                        COUNT(CASE WHEN amount > 0 THEN 1 END) as positive,
                        COUNT(CASE WHEN amount < 0 THEN 1 END) as negative,
                        COUNT(CASE WHEN amount = 0 THEN 1 END) as zero
                    FROM positions
                """)).fetchone()
                
                v2_amounts = v2_conn.execute(text("""
                    SELECT 
                        COUNT(CASE WHEN amount > 0 THEN 1 END) as positive,
                        COUNT(CASE WHEN amount < 0 THEN 1 END) as negative,
                        COUNT(CASE WHEN amount = 0 THEN 1 END) as zero
                    FROM positions
                """)).fetchone()
                
                amounts_match = (v1_amounts.positive == v2_amounts.positive and
                               v1_amounts.negative == v2_amounts.negative and
                               v1_amounts.zero == v2_amounts.zero)
                
                validation_results["amount_distribution"] = {
                    "v1_amounts": {"positive": v1_amounts.positive, "negative": v1_amounts.negative, "zero": v1_amounts.zero},
                    "v2_amounts": {"positive": v2_amounts.positive, "negative": v2_amounts.negative, "zero": v2_amounts.zero},
                    "match": amounts_match
                }
                
                print(f"   Amount distribution - V1: +{v1_amounts.positive:,}/-{v1_amounts.negative:,}/0:{v1_amounts.zero:,}, "
                      f"V2: +{v2_amounts.positive:,}/-{v2_amounts.negative:,}/0:{v2_amounts.zero:,} {'✅' if amounts_match else '❌'}")
                
                # 5. Content ID sample validation (spot check first 10)
                v1_sample = v1_conn.execute(text("""
                    SELECT content_id, user, token, amount 
                    FROM positions 
                    ORDER BY block_number, timestamp 
                    LIMIT 10
                """)).fetchall()
                
                v2_sample = v2_conn.execute(text("""
                    SELECT content_id, user, token, amount 
                    FROM positions 
                    ORDER BY block_number, timestamp 
                    LIMIT 10
                """)).fetchall()
                
                sample_match = len(v1_sample) == len(v2_sample)
                if sample_match:
                    for v1_row, v2_row in zip(v1_sample, v2_sample):
                        if (v1_row.content_id != v2_row.content_id or 
                            v1_row.user != v2_row.user or 
                            v1_row.token != v2_row.token or 
                            str(v1_row.amount) != str(v2_row.amount)):
                            sample_match = False
                            break
                
                validation_results["sample_data"] = {
                    "v1_sample_count": len(v1_sample),
                    "v2_sample_count": len(v2_sample),
                    "match": sample_match
                }
                
                print(f"   Sample data (first 10 rows) - {'✅' if sample_match else '❌'}")
                
                # 6. Timestamp range validation
                v1_time_range = v1_conn.execute(text("SELECT MIN(timestamp), MAX(timestamp) FROM positions")).fetchone()
                v2_time_range = v2_conn.execute(text("SELECT MIN(timestamp), MAX(timestamp) FROM positions")).fetchone()
                
                time_match = (v1_time_range[0] == v2_time_range[0] and v1_time_range[1] == v2_time_range[1])
                
                validation_results["timestamp_range"] = {
                    "v1_range": {"min": v1_time_range[0], "max": v1_time_range[1]},
                    "v2_range": {"min": v2_time_range[0], "max": v2_time_range[1]},
                    "match": time_match
                }
                
                print(f"   Timestamp range - {'✅' if time_match else '❌'}")
                if v1_time_range[0] and v2_time_range[0]:
                    print(f"     V1: {v1_time_range[0]} to {v1_time_range[1]}")
                    print(f"     V2: {v2_time_range[0]} to {v2_time_range[1]}")
                
                # Overall validation result
                all_validations = [
                    validation_results["row_count"]["match"],
                    validation_results["user_distribution"]["match"], 
                    validation_results["token_distribution"]["match"],
                    validation_results["amount_distribution"]["match"],
                    validation_results["sample_data"]["match"],
                    validation_results["timestamp_range"]["match"]
                ]
                
                validation_results["overall_success"] = all(all_validations)
                
                if validation_results["overall_success"]:
                    print(f"\n🎉 VALIDATION PASSED - All checks successful!")
                else:
                    print(f"\n❌ VALIDATION FAILED - Check details above")
                
                return validation_results
    
    def run_full_migration(self):
        """Run complete migration process."""
        print(f"🚀 Starting full positions migration from {self.v1_db_name} to {self.v2_db_name}")
        print("=" * 80)
        
        try:
            # Step 1: Get data statistics
            stats = self.get_data_stats()
            
            if stats["total_rows"] == 0:
                print("\n⚠️  No data to migrate - positions table is empty")
                return
            
            # Step 2: Migrate data
            migration_result = self.migrate_data()
            
            # Step 3: Validate migration
            validation_result = self.validate_migration()
            
            # Step 4: Summary
            print("\n" + "=" * 80)
            print("MIGRATION SUMMARY")
            print("=" * 80)
            print(f"Database: {self.v1_db_name} → {self.v2_db_name}")
            print(f"Table: positions")
            print(f"Rows migrated: {migration_result.get('migrated_rows', 0):,}")
            print(f"Migration successful: {'✅ YES' if migration_result.get('success', False) else '❌ NO'}")
            print(f"Validation passed: {'✅ YES' if validation_result.get('overall_success', False) else '❌ NO'}")
            
            if validation_result.get('overall_success', False):
                print(f"\n🎉 POSITIONS MIGRATION COMPLETED SUCCESSFULLY!")
                print(f"   - All {migration_result.get('migrated_rows', 0):,} positions migrated")
                print(f"   - All validation checks passed")
                print(f"   - Data integrity preserved")
                print(f"   - Ready for next table migration")
            else:
                print(f"\n❌ Migration completed but validation failed")
                print(f"   Please review validation details above")
                
        except Exception as e:
            print(f"\n💥 Migration failed with error: {e}")
            raise


def main():
    """Main entry point."""
    migrator = PositionsMigrator()
    migrator.run_full_migration()


if __name__ == "__main__":
    main()