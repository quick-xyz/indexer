#!/usr/bin/env python3
"""
Processing Jobs Table Migration Script

Migrates processing_jobs table data from v1 to v2 database using the established pattern.

V1 Schema (356 rows):
- job_type, status, job_data, worker_id, priority, retry_count, max_retries
- error_message, started_at, completed_at, id
- created_at, updated_at (skip - auto-generated in v2)

V2 Schema: Direct mapping, potential enum compatibility to verify.
"""

import sys
from pathlib import Path
from typing import Dict
from sqlalchemy import text, create_engine

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class ProcessingJobsMigrator:
    """Migrate processing_jobs table from v1 to v2 database."""
    
    def __init__(self, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2"):
        self.v1_db_name = v1_db_name
        self.v2_db_name = v2_db_name
        
        print(f"🔧 Initializing processing_jobs migration")
        print(f"   V1 DB: {v1_db_name}")
        print(f"   V2 DB: {v2_db_name}")
        print(f"   Expected rows: ~356")
        
        # Use the exact same credential pattern as successful migrations
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
                v1_check = v1_conn.execute(text("SELECT COUNT(*) FROM processing_jobs"))
                v1_count = v1_check.scalar()
                print(f"   V1 ({self.v1_db_name}): ✅ Connected - {v1_count:,} rows in processing_jobs")
                
            # Test v2 connection and check table exists  
            with self.v2_engine.connect() as v2_conn:
                v2_check = v2_conn.execute(text("SELECT COUNT(*) FROM processing_jobs"))
                v2_count = v2_check.scalar()
                print(f"   V2 ({self.v2_db_name}): ✅ Connected - {v2_count:,} rows in processing_jobs")
                
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            raise
    
    def get_data_stats(self) -> Dict:
        """Get statistics about source data before migration."""
        print(f"\n📊 Analyzing processing_jobs data in {self.v1_db_name}...")
        
        with self.v1_engine.connect() as v1_conn:
            # Basic stats
            count_query = text("SELECT COUNT(*) FROM processing_jobs")
            total_rows = v1_conn.execute(count_query).scalar()
            
            # Distribution stats
            stats_query = text("""
                SELECT 
                    COUNT(DISTINCT job_type) as unique_job_types,
                    COUNT(DISTINCT status) as unique_statuses,
                    COUNT(DISTINCT worker_id) as unique_workers,
                    AVG(priority) as avg_priority,
                    AVG(retry_count) as avg_retry_count,
                    COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) as jobs_with_errors
                FROM processing_jobs
            """)
            stats = v1_conn.execute(stats_query).fetchone()
            
            # Job type distribution
            job_type_query = text("""
                SELECT job_type, COUNT(*) as count
                FROM processing_jobs
                GROUP BY job_type
                ORDER BY count DESC
            """)
            job_types = v1_conn.execute(job_type_query).fetchall()
            
            # Status distribution
            status_query = text("""
                SELECT status, COUNT(*) as count
                FROM processing_jobs
                GROUP BY status
                ORDER BY count DESC
            """)
            statuses = v1_conn.execute(status_query).fetchall()
            
            results = {
                "total_rows": total_rows,
                "unique_job_types": stats.unique_job_types if stats else 0,
                "unique_statuses": stats.unique_statuses if stats else 0,
                "unique_workers": stats.unique_workers if stats else 0,
                "avg_priority": stats.avg_priority if stats else 0,
                "avg_retry_count": stats.avg_retry_count if stats else 0,
                "jobs_with_errors": stats.jobs_with_errors if stats else 0,
                "job_type_distribution": {row.job_type: row.count for row in job_types},
                "status_distribution": {row.status: row.count for row in statuses}
            }
            
            print(f"   Total rows: {results['total_rows']:,}")
            print(f"   Unique job types: {results['unique_job_types']} - {list(results['job_type_distribution'].keys())}")
            print(f"   Unique statuses: {results['unique_statuses']} - {list(results['status_distribution'].keys())}")
            print(f"   Unique workers: {results['unique_workers']:,}")
            print(f"   Jobs with errors: {results['jobs_with_errors']:,}")
            print(f"   Avg priority: {results['avg_priority']:.2f}")
            print(f"   Avg retry count: {results['avg_retry_count']:.2f}")
            
            return results
    
    def migrate_data(self) -> Dict:
        """Migrate processing_jobs data from v1 to v2."""
        print(f"\n🚚 Migrating processing_jobs data from {self.v1_db_name} to {self.v2_db_name}...")
        
        # Get all data from v1 - direct field mapping, skip auto-generated timestamps
        select_query = text("""
            SELECT 
                id,
                job_type,
                status,
                job_data,
                worker_id,
                priority,
                retry_count,
                max_retries,
                error_message,
                started_at,
                completed_at
            FROM processing_jobs
            ORDER BY created_at
        """)
        
        # Prepare insert query for v2 - direct 1:1 mapping 
        insert_query = text("""
            INSERT INTO processing_jobs (
                id,
                job_type,
                status,
                job_data,
                worker_id,
                priority,
                retry_count,
                max_retries,
                error_message,
                started_at,
                completed_at
            ) VALUES (
                :id,
                :job_type,
                :status,
                :job_data,
                :worker_id,
                :priority,
                :retry_count,
                :max_retries,
                :error_message,
                :started_at,
                :completed_at
            )
        """)
        
        # Execute migration
        with self.v1_engine.connect() as v1_conn:
            v1_data = v1_conn.execute(select_query)
            rows_to_migrate = []
            
            for row in v1_data:
                row_dict = dict(row._mapping)
                # Convert job_data dict to JSON string for JSONB compatibility
                if 'job_data' in row_dict and row_dict['job_data'] is not None:
                    import json
                    if isinstance(row_dict['job_data'], dict):
                        row_dict['job_data'] = json.dumps(row_dict['job_data'])
                rows_to_migrate.append(row_dict)
        
        print(f"   Fetched {len(rows_to_migrate)} rows from v1")
        
        with self.v2_engine.connect() as v2_conn:
            trans = v2_conn.begin()
            try:
                # Clear existing data first (in case of re-migration)
                v2_conn.execute(text("DELETE FROM processing_jobs"))
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
        """Validate processing_jobs migration with comprehensive checks."""
        print(f"\n🔍 Validating processing_jobs migration...")
        
        validation_results = {}
        
        with self.v1_engine.connect() as v1_conn:
            with self.v2_engine.connect() as v2_conn:
                
                # 1. Row count validation
                v1_count = v1_conn.execute(text("SELECT COUNT(*) FROM processing_jobs")).scalar()
                v2_count = v2_conn.execute(text("SELECT COUNT(*) FROM processing_jobs")).scalar()
                
                validation_results["row_count"] = {
                    "v1_count": v1_count,
                    "v2_count": v2_count,
                    "match": v1_count == v2_count
                }
                
                print(f"   Row counts - V1: {v1_count:,}, V2: {v2_count:,} {'✅' if v1_count == v2_count else '❌'}")
                
                if v1_count == 0:
                    print(f"   ⚠️  No data to validate")
                    return validation_results
                
                # 2. Job type distribution validation
                v1_job_types = v1_conn.execute(text("""
                    SELECT job_type, COUNT(*) as count 
                    FROM processing_jobs 
                    GROUP BY job_type ORDER BY job_type
                """)).fetchall()
                
                v2_job_types = v2_conn.execute(text("""
                    SELECT job_type, COUNT(*) as count 
                    FROM processing_jobs 
                    GROUP BY job_type ORDER BY job_type
                """)).fetchall()
                
                v1_job_type_dict = {row.job_type: row.count for row in v1_job_types}
                v2_job_type_dict = {row.job_type: row.count for row in v2_job_types}
                job_types_match = v1_job_type_dict == v2_job_type_dict
                
                validation_results["job_type_distribution"] = {
                    "v1_distribution": v1_job_type_dict,
                    "v2_distribution": v2_job_type_dict,
                    "match": job_types_match
                }
                
                print(f"   Job type distribution - {'✅' if job_types_match else '❌'}")
                for job_type in set(list(v1_job_type_dict.keys()) + list(v2_job_type_dict.keys())):
                    v1_count = v1_job_type_dict.get(job_type, 0)
                    v2_count = v2_job_type_dict.get(job_type, 0)
                    status = "✅" if v1_count == v2_count else "❌"
                    print(f"     {job_type}: V1={v1_count}, V2={v2_count} {status}")
                
                # 3. Status distribution validation
                v1_statuses = v1_conn.execute(text("""
                    SELECT status, COUNT(*) as count 
                    FROM processing_jobs 
                    GROUP BY status ORDER BY status
                """)).fetchall()
                
                v2_statuses = v2_conn.execute(text("""
                    SELECT status, COUNT(*) as count 
                    FROM processing_jobs 
                    GROUP BY status ORDER BY status
                """)).fetchall()
                
                v1_status_dict = {row.status: row.count for row in v1_statuses}
                v2_status_dict = {row.status: row.count for row in v2_statuses}
                statuses_match = v1_status_dict == v2_status_dict
                
                validation_results["status_distribution"] = {
                    "v1_distribution": v1_status_dict,
                    "v2_distribution": v2_status_dict,
                    "match": statuses_match
                }
                
                print(f"   Status distribution - {'✅' if statuses_match else '❌'}")
                for status in set(list(v1_status_dict.keys()) + list(v2_status_dict.keys())):
                    v1_count = v1_status_dict.get(status, 0)
                    v2_count = v2_status_dict.get(status, 0)
                    status_icon = "✅" if v1_count == v2_count else "❌"
                    print(f"     {status}: V1={v1_count}, V2={v2_count} {status_icon}")
                
                # 4. Workers validation
                v1_workers = v1_conn.execute(text("SELECT COUNT(DISTINCT worker_id) FROM processing_jobs")).scalar()
                v2_workers = v2_conn.execute(text("SELECT COUNT(DISTINCT worker_id) FROM processing_jobs")).scalar()
                
                validation_results["worker_count"] = {
                    "v1_workers": v1_workers,
                    "v2_workers": v2_workers,
                    "match": v1_workers == v2_workers
                }
                
                print(f"   Worker counts - V1: {v1_workers:,}, V2: {v2_workers:,} {'✅' if v1_workers == v2_workers else '❌'}")
                
                # 5. Sample ID validation (spot check first 5) - use consistent ordering
                v1_sample = v1_conn.execute(text("""
                    SELECT id, job_type, status, worker_id
                    FROM processing_jobs 
                    ORDER BY id 
                    LIMIT 5
                """)).fetchall()
                
                v2_sample = v2_conn.execute(text("""
                    SELECT id, job_type, status, worker_id
                    FROM processing_jobs 
                    ORDER BY id 
                    LIMIT 5
                """)).fetchall()
                
                sample_match = len(v1_sample) == len(v2_sample)
                if sample_match:
                    for v1_row, v2_row in zip(v1_sample, v2_sample):
                        if (str(v1_row.id) != str(v2_row.id) or 
                            v1_row.job_type != v2_row.job_type or 
                            v1_row.status != v2_row.status or 
                            v1_row.worker_id != v2_row.worker_id):
                            sample_match = False
                            break
                
                validation_results["sample_data"] = {
                    "v1_sample_count": len(v1_sample),
                    "v2_sample_count": len(v2_sample),
                    "match": sample_match
                }
                
                print(f"   Sample data (first 5 rows) - {'✅' if sample_match else '❌'}")
                
                # Overall validation result
                all_validations = [
                    validation_results["row_count"]["match"],
                    validation_results["job_type_distribution"]["match"], 
                    validation_results["status_distribution"]["match"],
                    validation_results["worker_count"]["match"],
                    validation_results["sample_data"]["match"]
                ]
                
                validation_results["overall_success"] = all(all_validations)
                
                if validation_results["overall_success"]:
                    print(f"\n🎉 VALIDATION PASSED - All checks successful!")
                else:
                    print(f"\n❌ VALIDATION FAILED - Check details above")
                
                return validation_results
    
    def run_full_migration(self):
        """Run complete migration process."""
        print(f"🚀 Starting full processing_jobs migration from {self.v1_db_name} to {self.v2_db_name}")
        print("=" * 80)
        
        try:
            # Step 1: Get data statistics
            stats = self.get_data_stats()
            
            if stats["total_rows"] == 0:
                print("\n⚠️  No data to migrate - processing_jobs table is empty")
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
            print(f"Table: processing_jobs")
            print(f"Rows migrated: {migration_result.get('migrated_rows', 0):,}")
            print(f"Migration successful: {'✅ YES' if migration_result.get('success', False) else '❌ NO'}")
            print(f"Validation passed: {'✅ YES' if validation_result.get('overall_success', False) else '❌ NO'}")
            
            if validation_result.get('overall_success', False):
                print(f"\n🎉 PROCESSING_JOBS MIGRATION COMPLETED SUCCESSFULLY!")
                print(f"   - All {migration_result.get('migrated_rows', 0):,} processing jobs migrated")
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
    migrator = ProcessingJobsMigrator()
    migrator.run_full_migration()


if __name__ == "__main__":
    main()