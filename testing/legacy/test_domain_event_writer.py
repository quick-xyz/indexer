#!/usr/bin/env python3
"""
Test DomainEventWriter in isolation
Verifies database writes work without gRPC conflicts
"""

import sys
import os
from pathlib import Path
from typing import Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.database.writers.domain_event_writer import DomainEventWriter
from indexer.database.repository import RepositoryManager
from indexer.types.new import EvmHash, DomainEventId
from indexer.types.model.positions import Position

import logging


def test_domain_event_writer():
    """Test DomainEventWriter directly"""
    print("🧪 Testing DomainEventWriter in isolation")
    print("=" * 50)
    
    try:
        # Initialize with fixed SecretsService singleton
        print("🚀 Initializing indexer...")
        container = create_indexer()
        
        print("✅ Container created successfully")
        
        # Get services
        domain_event_writer = container.get(DomainEventWriter)
        repository_manager = container.get(RepositoryManager)
        
        print("✅ Services retrieved successfully")
        
        # Test database connection
        print("\n🔗 Testing database connection...")
        with repository_manager.get_session() as session:
            # Simple query to verify connection
            from sqlalchemy import text
            result = session.execute(text("SELECT 1 as test")).fetchone()
            print(f"✅ Database connection test: {result.test}")
        
        # Create test data
        print("\n📝 Creating test data...")
        tx_hash = EvmHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
        block_number = 12345678
        timestamp = 1700000000
        
        # Mock events and positions (empty for this test)
        events: Dict[DomainEventId, any] = {}
        positions: Dict[DomainEventId, Position] = {}
        
        print(f"   tx_hash: {tx_hash}")
        print(f"   block_number: {block_number}")
        print(f"   timestamp: {timestamp}")
        print(f"   events: {len(events)} (empty for this test)")
        print(f"   positions: {len(positions)} (empty for this test)")
        
        # Test writing transaction results
        print("\n💾 Testing domain event writer...")
        try:
            events_written, positions_written, events_skipped = domain_event_writer.write_transaction_results(
                tx_hash=tx_hash,
                block_number=block_number,
                timestamp=timestamp,
                events=events,
                positions=positions,
                tx_success=True
            )
            
            print(f"✅ Write completed successfully!")
            print(f"   Events written: {events_written}")
            print(f"   Positions written: {positions_written}")
            print(f"   Events skipped: {events_skipped}")
            
            return True
            
        except Exception as db_error:
            print(f"❌ Database write failed: {db_error}")
            print(f"   Error type: {type(db_error).__name__}")
            
            # Check if it's a gRPC error
            if "grpc" in str(db_error).lower() or "rpc" in str(db_error).lower():
                print("⚠️  This appears to be a gRPC-related error")
                print("   The SecretsService singleton fix may not be complete")
            
            return False
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        print(f"   Error type: {type(e).__name__}")
        
        # Check for common issues
        if "secret" in str(e).lower():
            print("💡 This appears to be a secrets/authentication issue")
            print("   Check INDEXER_GCP_PROJECT_ID and GCP credentials")
        elif "database" in str(e).lower() or "connection" in str(e).lower():
            print("💡 This appears to be a database connection issue")
            print("   Check database credentials and connectivity")
        elif "grpc" in str(e).lower() or "rpc" in str(e).lower():
            print("💡 This appears to be a gRPC conflict issue")
            print("   The SecretsService singleton fix needs additional work")
        
        import traceback
        print(f"\n🔍 Full traceback:")
        traceback.print_exc()
        
        return False


def main():
    """Main entry point"""
    print("🚀 DomainEventWriter Isolation Test")
    print("Testing database writes without pipeline complexity")
    print()
    
    # Configure basic logging
    logging.basicConfig(level=logging.WARNING)
    
    success = test_domain_event_writer()
    
    print(f"\n{'🎉' if success else '💥'} Test {'PASSED' if success else 'FAILED'}")
    
    if success:
        print("✅ DomainEventWriter is working correctly!")
        print("🎯 Next steps:")
        print("   1. Test full pipeline: python -m indexer.pipeline.batch_runner test 63269916")
        print("   2. Run diagnostics: python testing/diagnostics/quick_diagnostic.py")
    else:
        print("❌ DomainEventWriter has issues that need to be resolved")
        print("🔧 Troubleshooting steps:")
        print("   1. Check GCP credentials: python testing/test_gcp_secrets.py") 
        print("   2. Check database connection: python testing/debug_db_connection.py")
        print("   3. Review the error details above")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()