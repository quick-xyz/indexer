#!/usr/bin/env python3
"""
Check processing_jobs table schema
See what columns actually exist vs what the model expects
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.database.repository import RepositoryManager
from sqlalchemy import text
import logging


def check_processing_jobs_schema():
    """Check what columns exist in processing_jobs table"""
    print("🔍 Checking processing_jobs table schema")
    print("=" * 50)
    
    try:
        # Initialize indexer to get database connection
        print("🚀 Initializing indexer...")
        container = create_indexer()
        
        # Get repository manager (which connects to model database)
        repository_manager = container.get(RepositoryManager)
        
        print("✅ Database connection established")
        
        # Check current schema
        print("\n📋 Current processing_jobs columns:")
        with repository_manager.get_session() as session:
            # Get current columns
            result = session.execute(text("""
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable, 
                    column_default,
                    character_maximum_length
                FROM information_schema.columns 
                WHERE table_name = 'processing_jobs'
                ORDER BY ordinal_position
            """))
            
            current_columns = []
            for row in result:
                nullable = "NULL" if row.is_nullable == "YES" else "NOT NULL"
                col_type = row.data_type
                if row.character_maximum_length:
                    col_type += f"({row.character_maximum_length})"
                
                default = f" DEFAULT {row.column_default}" if row.column_default else ""
                
                print(f"   {row.column_name:<25} {col_type:<20} {nullable:<10}{default}")
                current_columns.append(row.column_name)
        
        print(f"\n📊 Found {len(current_columns)} columns")
        
        # Compare with what the NEW model expects
        new_model_columns = [
            'id', 'created_at', 'updated_at',  # BaseModel columns
            'job_type', 'status', 'job_data', 'worker_id', 'priority', 
            'retry_count', 'max_retries', 'error_message', 'started_at', 'completed_at'
        ]
        
        # Compare with what the OLD model expects  
        old_model_columns = [
            'id', 'created_at', 'updated_at',  # BaseModel columns
            'job_type', 'status', 'start_block', 'end_block', 'current_block',
            'error_message', 'metadata'
        ]
        
        print(f"\n🆕 NEW model expects these columns:")
        new_missing = []
        for col in new_model_columns:
            status = "✅" if col in current_columns else "❌"
            print(f"   {status} {col}")
            if col not in current_columns:
                new_missing.append(col)
        
        print(f"\n🗂️  OLD model expects these columns:")
        old_missing = []
        for col in old_model_columns:
            status = "✅" if col in current_columns else "❌"
            print(f"   {status} {col}")
            if col not in current_columns:
                old_missing.append(col)
        
        print(f"\n📊 Schema Analysis:")
        if len(new_missing) == 0:
            print("   ✅ Database has NEW schema - matches current code")
        elif len(old_missing) == 0:
            print("   ⚠️  Database has OLD schema - needs migration")
        else:
            print("   ❌ Database has UNKNOWN schema - manual fix needed")
        
        print(f"\n🔧 Current columns in database: {sorted(current_columns)}")
        
        return current_columns
        
    except Exception as e:
        print(f"\n❌ Schema check failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def main():
    """Main entry point"""
    print("🔍 Processing Jobs Schema Checker")
    print("Checking what columns actually exist in the database")
    print()
    
    # Configure basic logging
    logging.basicConfig(level=logging.WARNING)
    
    columns = check_processing_jobs_schema()
    
    if columns:
        print("\n✅ Schema check completed!")
    else:
        print("\n❌ Schema check failed")
    
    sys.exit(0 if columns else 1)


if __name__ == "__main__":
    main()