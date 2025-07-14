#!/usr/bin/env python3
"""
Debug Event Data Extraction

Test the _extract_event_data method with actual events from GCS to see what's failing.
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Use the testing environment
sys.path.insert(0, str(PROJECT_ROOT / "testing"))
from testing import get_testing_environment


def debug_extract_event_data():
    """Debug the event data extraction process"""
    print("🔍 Debugging Event Data Extraction")
    print("=" * 60)
    
    # Initialize environment
    env = get_testing_environment()
    
    # Get services
    from indexer.storage.gcs_handler import GCSHandler
    from indexer.database.writers.domain_event_writer import DomainEventWriter
    from indexer.database.repository import RepositoryManager
    
    gcs = env.get_service(GCSHandler)
    repository_manager = env.get_service(RepositoryManager)
    domain_event_writer = env.get_service(DomainEventWriter)
    
    # Get the test block
    block_number = 65297576
    print(f"\n📥 Loading block {block_number} from GCS...")
    block_data = gcs.get_complete_block(block_number)
    
    if not block_data:
        print(f"❌ Block {block_number} not found")
        return
    
    # Get the first transaction with events
    tx_hash = "0x9aadf86e90c7aea93786fb4f1b0376bbc191330df68bebb5238431b905402496"
    if tx_hash not in block_data.transactions:
        print(f"❌ Transaction {tx_hash} not found")
        return
        
    tx = block_data.transactions[tx_hash]
    print(f"✅ Transaction loaded with {len(tx.events)} events")
    
    # Test each event
    for i, (event_id, event) in enumerate(tx.events.items(), 1):
        print(f"\n🎯 Testing Event {i}: {event_id}")
        print(f"   Type: {type(event).__name__}")
        print(f"   String: {str(event)[:200]}...")
        
        try:
            # Test the to_dict method
            print(f"\n   🔧 Testing to_dict()...")
            event_dict = event.to_dict()
            print(f"   ✅ to_dict() successful: {len(event_dict)} fields")
            print(f"   Fields: {list(event_dict.keys())}")
            
            # Test getting the repository
            print(f"\n   🔧 Testing repository selection...")
            repository = domain_event_writer._get_event_repository(event)
            print(f"   ✅ Repository: {type(repository).__name__}")
            
            # Test the extraction method step by step
            print(f"\n   🔧 Testing _extract_event_data()...")
            
            # Step 1: Raw data extraction
            if hasattr(event, 'to_dict'):
                raw_data = event.to_dict()
                print(f"   ✅ Step 1 - Raw data: {len(raw_data)} fields")
            else:
                print(f"   ❌ Step 1 - No to_dict method!")
                continue
            
            # Step 2: Field filtering
            excluded_fields = ['content_id', 'tx_hash', 'timestamp', 'block_number', 'signals', 'positions', 'swaps', 'transfers']
            filtered_data = {k: v for k, v in raw_data.items() if k not in excluded_fields}
            print(f"   ✅ Step 2 - Filtered data: {len(filtered_data)} fields")
            print(f"   Filtered fields: {list(filtered_data.keys())}")
            
            # Step 3: Model inspection
            if hasattr(repository, 'model_class') and hasattr(repository.model_class, '__table__'):
                valid_columns = {col.name for col in repository.model_class.__table__.columns}
                print(f"   ✅ Step 3 - Valid columns: {valid_columns}")
                
                # Step 4: Database filtering
                db_filtered_data = {}
                for key, value in filtered_data.items():
                    if key in valid_columns:
                        db_filtered_data[key] = value
                print(f"   ✅ Step 4 - DB filtered: {len(db_filtered_data)} fields")
                print(f"   DB fields: {list(db_filtered_data.keys())}")
                print(f"   Excluded: {[k for k in filtered_data.keys() if k not in valid_columns]}")
                
                # Step 5: Data conversion
                print(f"\n   🔧 Testing _convert_event_data()...")
                try:
                    converted_data = domain_event_writer._convert_event_data(event, db_filtered_data)
                    print(f"   ✅ Step 5 - Converted data: {len(converted_data)} fields")
                    print(f"   Final data: {converted_data}")
                    
                    print(f"\n   🎉 Event {i} extraction SUCCESSFUL!")
                    
                except Exception as e:
                    print(f"   ❌ Step 5 - Conversion failed: {e}")
                    import traceback
                    print(f"   Traceback: {traceback.format_exc()}")
                
            else:
                print(f"   ❌ Step 3 - No model class or table!")
                
        except Exception as e:
            print(f"   ❌ Event {i} failed: {e}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    debug_extract_event_data()