#!/usr/bin/env python3
"""
Debug Path Building for GCS Block Retrieval
Check if the generated paths match your bucket structure
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler

def debug_path_building(block_number: int, model_name: str = None):
    """Debug path building for a specific block"""
    
    print(f"🔍 Debugging path building for block {block_number}")
    print("=" * 60)
    
    try:
        # Initialize testing environment
        testing_env = get_testing_environment(model_name=model_name, log_level="ERROR")
        storage_handler = testing_env.get_service(GCSHandler)
        config = testing_env.get_config()
        
        print(f"Model: {config.model_name}")
        print(f"Bucket: {storage_handler.bucket_name}")
        print(f"GCS Project: {storage_handler.gcs_project}")
        
        # Get sources information
        print(f"\n📋 Sources Configuration:")
        sources = config.get_all_sources()
        print(f"Total sources: {len(sources)}")
        
        for i, source in enumerate(sources, 1):
            print(f"\n   Source {i}:")
            print(f"     ID: {source.id}")
            print(f"     Name: {source.name}")
            print(f"     Path: {source.path}")
            print(f"     Format: {source.format}")
        
        # Get primary source
        primary_source = config.get_primary_source()
        if not primary_source:
            print("\n❌ No primary source found!")
            return
        
        print(f"\n🎯 Primary Source: {primary_source.name}")
        print(f"   Path: {primary_source.path}")
        print(f"   Format: {primary_source.format}")
        
        # Generate the path using the source
        try:
            generated_path = storage_handler.get_blob_string("rpc", block_number, source=primary_source)
            print(f"\n🔧 Generated Path:")
            print(f"   Full path: {generated_path}")
            
            # Break down the path construction
            print(f"\n🔍 Path Construction Breakdown:")
            print(f"   Source path: '{primary_source.path}'")
            print(f"   Source format: '{primary_source.format}'")
            print(f"   Block number: {block_number}")
            
            # Show format substitution
            try:
                formatted_part = primary_source.format.format(block_number, block_number)
                print(f"   Formatted part: '{formatted_part}'")
                print(f"   Final: '{primary_source.path}' + '{formatted_part}' = '{generated_path}'")
            except Exception as e:
                print(f"   ❌ Format error: {e}")
        
        except Exception as e:
            print(f"\n❌ Path generation failed: {e}")
            return
        
        # Check if blob exists
        print(f"\n🔍 Checking if blob exists...")
        try:
            exists = storage_handler.blob_exists(generated_path)
            print(f"   Blob exists: {exists}")
            
            if not exists:
                print(f"\n💡 Blob not found. Let's check bucket structure...")
                
                # List some blobs with the source path prefix
                print(f"   Listing blobs with prefix: '{primary_source.path}'")
                try:
                    blobs = storage_handler.list_blobs(prefix=primary_source.path, max_results=10)
                    if blobs:
                        print(f"   Found {len(blobs)} blobs with this prefix:")
                        for blob in blobs[:5]:  # Show first 5
                            print(f"     - {blob.name}")
                        if len(blobs) > 5:
                            print(f"     ... and {len(blobs) - 5} more")
                    else:
                        print(f"   ❌ No blobs found with prefix '{primary_source.path}'")
                        
                        # Try broader search
                        print(f"\n   Trying broader search...")
                        root_parts = primary_source.path.rstrip('/').split('/')
                        if len(root_parts) > 1:
                            broader_prefix = '/'.join(root_parts[:-1]) + '/'
                            print(f"   Listing with broader prefix: '{broader_prefix}'")
                            broader_blobs = storage_handler.list_blobs(prefix=broader_prefix, max_results=10)
                            if broader_blobs:
                                print(f"   Found {len(broader_blobs)} blobs:")
                                for blob in broader_blobs[:5]:
                                    print(f"     - {blob.name}")
                
                except Exception as e:
                    print(f"   ❌ Failed to list blobs: {e}")
        
        except Exception as e:
            print(f"   ❌ Blob check failed: {e}")
        
        # Show expected patterns
        print(f"\n💡 Expected File Patterns:")
        print(f"   Looking for files matching: {primary_source.format}")
        print(f"   Example expected filename: {primary_source.format.format(block_number, block_number)}")
        
        # Test a few different block numbers
        print(f"\n🧪 Testing path generation for other blocks:")
        test_blocks = [block_number - 1, block_number + 1, 50000000, 60000000]
        for test_block in test_blocks:
            try:
                test_path = storage_handler.get_blob_string("rpc", test_block, source=primary_source)
                test_exists = storage_handler.blob_exists(test_path)
                status = "✅ EXISTS" if test_exists else "❌ NOT FOUND"
                print(f"   Block {test_block}: {test_path} - {status}")
                if test_exists:
                    print(f"     ^^ This block exists! Try: python testing/test_pipeline.py {test_block}")
            except Exception as e:
                print(f"   Block {test_block}: Error - {e}")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: python debug_path_building.py <block_number> [model_name]")
        print("Example: python debug_path_building.py 58277747 blub_test")
        sys.exit(1)
    
    block_number = int(sys.argv[1])
    model_name = sys.argv[2] if len(sys.argv) > 2 else None
    
    debug_path_building(block_number, model_name)

if __name__ == "__main__":
    main()