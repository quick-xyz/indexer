#!/usr/bin/env python3
"""
Simple Base Migration Class

Simplified version that uses direct database connections without the full DI container.
This avoids model configuration issues during migration.
"""

import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class SimpleBaseMigrator:
    """Base class for table migration scripts using direct database connections."""
    
    def __init__(self, table_name: str, v1_db_name: str = "blub_test", v2_db_name: str = "blub_test_v2"):
        self.table_name = table_name
        self.v1_db_name = v1_db_name
        self.v2_db_name = v2_db_name
        
        print(f"🔧 Initializing {table_name} migration")
        print(f"   V1 DB: {v1_db_name}")
        print(f"   V2 DB: {v2_db_name}")
        
        # Get database connection info from environment variables
        self._get_db_config()
        self._setup_database_connections()
        
    def _get_db_config(self):
        """Get database configuration from environment variables."""
        self.db_host = os.getenv('INDEXER_DB_HOST', '127.0.0.1')
        self.db_port = os.getenv('INDEXER_DB_PORT', '5432')
        self.db_user = os.getenv('INDEXER_DB_USER', 'postgres')
        self.db_password = os.getenv('INDEXER_DB_PASSWORD', '')
        
        if not self.db_password:
            # Try alternative environment variable names
            self.db_password = os.getenv('DB_PASSWORD', '')
            
        if not self.db_password:
            raise ValueError("Database password not found. Set INDEXER_DB_PASSWORD or DB_PASSWORD environment variable")
            
        print(f"   DB Host: {self.db_host}:{self.db_port}")
        print(f"   DB User: {self.db_user}")
        
    def _setup_database_connections(self):
        """Setup separate connections to v1 and v2 databases."""
        print(f"🔗 Setting up database connections...")
        
        # Build connection URLs for both databases
        base_url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}"
        
        v1_url = f"{base_url}/{self.v1_db_name}"
        v2_url = f"{base_url}/{self.v2_db_name}"
        
        # Create engines
        self.v1_engine = create_engine(v1_url)
        self.v2_engine = create_engine(v2_url)
        
        # Test connections
        self._test_connections()
        
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
        print(f"\n📋 Analyzing v1 {self.table_name} table schema...")
        
        schema_query = text(f"""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default
            FROM information_schema.columns 
            WHERE table_name = '{self.table_name}' 
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
    
    def get_v1_basic_stats(self) -> Dict:
        """Get basic v1 data statistics (count and sample)."""
        print(f"\n📊 Analyzing v1 {self.table_name} data...")
        
        basic_queries = {
            "total_count": f"SELECT COUNT(*) as count FROM {self.table_name}",
            "sample_data": f"SELECT * FROM {self.table_name} ORDER BY block_number, timestamp LIMIT 3"
        }
        
        stats = {}
        with self.v1_engine.connect() as conn:
            for stat_name, query in basic_queries.items():
                result = conn.execute(text(query))
                if stat_name == "sample_data":
                    stats[stat_name] = [dict(row._mapping) for row in result]
                else:
                    stats[stat_name] = dict(result.fetchone()._mapping)
        
        print(f"   Total rows: {stats['total_count']['count']}")
        return stats
    
    def execute_migration(self, field_mapping: Dict[str, str], additional_transforms: Dict[str, str] = None) -> Dict:
        """
        Execute migration with field mapping.
        
        Args:
            field_mapping: Dict mapping v2_field -> v1_field
            additional_transforms: Dict of v2_field -> SQL expression for transformations
        """
        print(f"\n🚚 Migrating {self.table_name} data from {self.v1_db_name} to {self.v2_db_name}...")
        
        # Build SELECT query for v1
        v1_fields = []
        for v2_field, v1_field in field_mapping.items():
            if additional_transforms and v2_field in additional_transforms:
                v1_fields.append(f"{additional_transforms[v2_field]} as {v2_field}")
            else:
                v1_fields.append(f"{v1_field}")
        
        select_query = text(f"""
            SELECT {', '.join(v1_fields)}
            FROM {self.table_name}
            ORDER BY block_number, timestamp
        """)
        
        # Build INSERT query for v2
        v2_field_names = list(field_mapping.keys())
        insert_query = text(f"""
            INSERT INTO {self.table_name} ({', '.join(v2_field_names)})
            VALUES ({', '.join([f':{field}' for field in v2_field_names])})
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
                v2_conn.execute(text(f"DELETE FROM {self.table_name}"))
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
    
    def validate_basic_migration(self) -> Dict:
        """Basic validation comparing row counts and sample data."""
        print(f"\n✅ Validating {self.table_name} migration...")
        
        # Basic validation queries
        count_queries = {
            "v1_count": f"SELECT COUNT(*) as count FROM {self.table_name}",
            "v2_count": f"SELECT COUNT(*) as count FROM {self.table_name}",
            "v1_sample": f"SELECT * FROM {self.table_name} ORDER BY block_number LIMIT 2",
            "v2_sample": f"SELECT * FROM {self.table_name} ORDER BY block_number LIMIT 2"
        }
        
        results = {}
        
        # Get v1 results
        with self.v1_engine.connect() as conn:
            for key in ["v1_count", "v1_sample"]:
                result = conn.execute(text(count_queries[key]))
                if key == "v1_sample":
                    results[key] = [dict(row._mapping) for row in result]
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Get v2 results
        with self.v2_engine.connect() as conn:
            for key in ["v2_count", "v2_sample"]:
                result = conn.execute(text(count_queries[key]))
                if key == "v2_sample":
                    results[key] = [dict(row._mapping) for row in result]
                else:
                    results[key] = dict(result.fetchone()._mapping)
        
        # Compare results
        validation_passed = True
        
        v1_count = results["v1_count"]["count"]
        v2_count = results["v2_count"]["count"]
        if v1_count == v2_count:
            print(f"   ✅ Row counts match: {v1_count}")
        else:
            print(f"   ❌ Row counts mismatch: v1={v1_count}, v2={v2_count}")
            validation_passed = False
        
        print(f"\n{'✅ BASIC VALIDATION PASSED' if validation_passed else '❌ BASIC VALIDATION FAILED'}")
        
        return {
            "validation_passed": validation_passed,
            "v1_count": v1_count,
            "v2_count": v2_count,
            "details": results
        }
    
    def print_migration_summary(self, migration_result: Dict, validation_result: Dict):
        """Print migration summary."""
        print(f"\n📋 MIGRATION SUMMARY: {self.table_name}")
        print("=" * 50)
        print(f"Source: {self.v1_db_name}")
        print(f"Target: {self.v2_db_name}")
        print(f"Rows migrated: {migration_result.get('migrated_rows', 0)}")
        print(f"Validation: {'PASSED' if validation_result.get('validation_passed', False) else 'FAILED'}")
        
        overall_success = migration_result.get("success", False) and validation_result.get("validation_passed", False)
        print(f"Overall status: {'✅ SUCCESS' if overall_success else '❌ FAILED'}")
        
        return overall_success