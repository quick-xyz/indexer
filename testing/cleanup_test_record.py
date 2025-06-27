#!/usr/bin/env python3
"""
Clean up test record from DomainEventWriter test
Run this from the project root directory
"""

import sys
import os
from pathlib import Path

# Simple path setup - find the indexer module
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent if current_dir.name == "testing" else current_dir

# Add to Python path
sys.path.insert(0, str(project_root))

# Now import
try:
    from indexer import create_indexer
    from indexer.database.repository import RepositoryManager
    from indexer.types.new import EvmHash
    from sqlalchemy import text
    import logging
except ImportError as e:
    print(f"❌ Import error: {e}")
    print(f"Current directory: {Path.cwd()}")
    print(f"Script location: {Path(__file__).parent}")
    print(f"Project root detected: {project_root}")
    print(f"Make sure you're running from the project root directory")
    sys.exit(1)


def cleanup_test_record():
    """Remove the test record created by DomainEventWriter test"""
    print("🧹 Cleaning up DomainEventWriter test record")
    print("=" * 50)
    
    # Test transaction details from the test
    test_tx_hash = EvmHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
    test_block_number = 12345678
    
    try:
        # Initialize indexer
        print("🚀 Initializing indexer...")
        container = create_indexer()
        
        # Get repository manager
        repository_manager = container.get(RepositoryManager)
        
        print("✅ Database connection established")
        
        # Check if test record exists
        print(f"\n🔍 Looking for test record:")
        print(f"   tx_hash: {test_tx_hash}")
        print(f"   block_number: {test_block_number}")
        
        with repository_manager.get_session() as session:
            # Check for existing record
            result = session.execute(text("""
                SELECT tx_hash, block_number, status, events_generated, logs_processed
                FROM transaction_processing 
                WHERE tx_hash = :tx_hash
            """), {"tx_hash": str(test_tx_hash)})
            
            record = result.fetchone()
            
            if record is None:
                print("   ✅ No test record found - already clean!")
                return True
            
            print(f"   📋 Found record:")
            print(f"      status: {record.status}")
            print(f"      events_generated: {record.events_generated}")
            print(f"      logs_processed: {record.logs_processed}")
            
            # Delete the test record
            print(f"\n🗑️  Deleting test record...")
            
            delete_result = session.execute(text("""
                DELETE FROM transaction_processing 
                WHERE tx_hash = :tx_hash AND block_number = :block_number
            """), {
                "tx_hash": str(test_tx_hash),
                "block_number": test_block_number
            })
            
            # Commit the deletion
            session.commit()
            
            rows_deleted = delete_result.rowcount
            print(f"   ✅ Deleted {rows_deleted} record(s)")
            
            if rows_deleted > 0:
                print("🎉 Test record cleanup completed successfully!")
            else:
                print("⚠️  No records were deleted (may already be clean)")
            
            return True
            
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        
        import traceback
        print(f"\n🔍 Full traceback:")
        traceback.print_exc()
        
        return False


def main():
    """Main entry point"""
    print("🧹 DomainEventWriter Test Record Cleanup")
    print("Removes test data from transaction_processing table")
    print()
    
    # Configure basic logging
    logging.basicConfig(level=logging.WARNING)
    
    success = cleanup_test_record()
    
    print(f"\n{'🎉' if success else '💥'} Cleanup {'COMPLETED' if success else 'FAILED'}")
    
    if success:
        print("✅ Database is clean and ready for pipeline testing")
        print("🎯 Next steps:")
        print("   1. Test full pipeline: python -m indexer.pipeline.batch_runner test 63269916")
        print("   2. Run diagnostics: python testing/diagnostics/quick_diagnostic.py")
    else:
        print("❌ Cleanup had issues - check the error details above")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()