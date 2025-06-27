#!/usr/bin/env python3
"""
Check transaction_processing table schema
See what columns actually exist vs what the model expects
"""

import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.database.repository import RepositoryManager
from sqlalchemy import text
import logging


def check_transaction_processing_schema():
    """Check what columns exist in transaction_processing table"""
    print("🔍 Checking transaction_processing table schema")
    print("=" * 50)
    
    try:
        # Initialize indexer to get database connection
        print("🚀 Initializing indexer...")
        container = create_indexer()
        
        # Get repository manager (which connects to model database)
        repository_manager = container.get(RepositoryManager)
        
        print("✅ Database connection established")
        
        # Check current schema
        print("\n📋 Current transaction_processing columns:")
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
                WHERE table_name = 'transaction_processing'
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
        
        # Compare with what the model expects
        expected_columns = [
            'id', 'created_at', 'updated_at',  # BaseModel columns
            'tx_hash', 'block_number', 'timestamp', 'status', 
            'retry_count', 'error_message', 'gas_used', 'gas_price',
            'logs_processed', 'events_generated'
        ]
        
        print(f"\n🎯 Model expects these columns:")
        for col in expected_columns:
            status = "✅" if col in current_columns else "❌"
            print(f"   {status} {col}")
        
        missing = [col for col in expected_columns if col not in current_columns]
        extra = [col for col in current_columns if col not in expected_columns]
        
        if missing:
            print(f"\n❌ Missing columns: {missing}")
        
        if extra:
            print(f"\n➕ Extra columns: {extra}")
        
        if not missing and not extra:
            print(f"\n✅ Schema matches perfectly!")
        
        return current_columns
        
    except Exception as e:
        print(f"\n❌ Schema check failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def main():
    """Main entry point"""
    print("🔍 Transaction Processing Schema Checker")
    print("Checking what columns actually exist in the database")
    print()
    
    # Configure basic logging
    logging.basicConfig(level=logging.WARNING)
    
    columns = check_transaction_processing_schema()
    
    if columns:
        print("\n✅ Schema check completed!")
        print("🎯 Use this information to update the TransactionProcessing model")
    else:
        print("\n❌ Schema check failed")
    
    sys.exit(0 if columns else 1)


if __name__ == "__main__":
    main()