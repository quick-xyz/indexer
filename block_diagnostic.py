#!/usr/bin/env python3
"""
Block Diagnostic Tool

Quick diagnostic to troubleshoot why specific blocks can't be loaded.
Checks both GCS storage and RPC availability for a block.

Usage:
    python block_diagnostic.py 58584385
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.storage.gcs_handler import GCSHandler


def diagnose_block(block_number: int, model_name: str = None):
    """Diagnose why a block can't be loaded"""
    print(f"🔍 Diagnosing Block {block_number}")
    print("=" * 50)
    
    # Initialize container
    container = create_indexer(model_name=model_name)
    config = container._config
    gcs = container.get(GCSHandler)
    
    print(f"Model: {config.model_name}")
    
    # Try to get bucket info from GCS handler or config
    try:
        if hasattr(config, 'gcs_bucket'):
            print(f"Bucket: {config.gcs_bucket}")
        elif hasattr(gcs, 'bucket_name'):
            print(f"Bucket: {gcs.bucket_name}")
        elif hasattr(gcs, 'bucket'):
            print(f"Bucket: {gcs.bucket.name}")
        else:
            print("Bucket: (unknown)")
    except:
        print("Bucket: (unable to determine)")
    
    print()
    
    # 1. Check GCS storage locations
    print("📥 Checking GCS Storage:")
    
    storage_types = ["complete", "processing"]
    for storage_type in storage_types:
        try:
            blob_path = gcs.get_blob_string(storage_type, block_number)
            exists = gcs.blob_exists(blob_path)
            print(f"   {storage_type:12}: {'✅ EXISTS' if exists else '❌ NOT FOUND'} - {blob_path}")
            
            if exists:
                try:
                    if storage_type == "complete":
                        block_data = gcs.get_complete_block(block_number)
                    else:
                        block_data = gcs.get_processing_block(block_number)
                    
                    if block_data:
                        tx_count = len(block_data.transactions) if block_data.transactions else 0
                        print(f"                 ✅ Loaded successfully - {tx_count} transactions")
                    else:
                        print(f"                 ❌ Failed to load block data")
                        
                except Exception as e:
                    print(f"                 ❌ Load error: {e}")
                    
        except Exception as e:
            print(f"   {storage_type:12}: ❌ ERROR - {e}")
    
    # 2. Check RPC availability
    print("\n🌐 Checking RPC Sources:")
    
    try:
        # Get primary source
        primary_source = config.get_primary_source()
        if primary_source:
            print(f"   Primary source: {primary_source.name}")
            print(f"   Path: {primary_source.path}")
            
            try:
                rpc_blob_path = gcs.get_blob_string("rpc", block_number, source=primary_source)
                rpc_exists = gcs.blob_exists(rpc_blob_path)
                print(f"   RPC data: {'✅ EXISTS' if rpc_exists else '❌ NOT FOUND'} - {rpc_blob_path}")
                
                if rpc_exists:
                    try:
                        rpc_block = gcs.get_rpc_block(block_number, source=primary_source)
                        if rpc_block:
                            tx_count = len(rpc_block.transactions) if rpc_block.transactions else 0
                            print(f"             ✅ RPC block loaded - {tx_count} transactions")
                        else:
                            print(f"             ❌ Failed to load RPC block")
                    except Exception as e:
                        print(f"             ❌ RPC load error: {e}")
                        
            except Exception as e:
                print(f"   ❌ RPC check error: {e}")
        else:
            print("   ❌ No primary source configured")
            
        # Check all sources
        all_sources = config.get_sources()
        if len(all_sources) > 1:
            print(f"\n   Additional sources available: {len(all_sources) - 1}")
            for source in all_sources[1:]:  # Skip primary
                print(f"   - {source.name}: {source.path}")
                
    except Exception as e:
        print(f"   ❌ Source check error: {e}")
    
    # 3. Check if block number is in expected range
    print(f"\n📊 Block Analysis:")
    print(f"   Block number: {block_number:,}")
    
    # Get some context blocks
    try:
        complete_blocks = gcs.list_complete_blocks()
        if complete_blocks:
            min_complete = min(complete_blocks)
            max_complete = max(complete_blocks)
            print(f"   Complete range: {min_complete:,} - {max_complete:,}")
            
            if block_number < min_complete:
                print(f"   ⚠️  Block is BEFORE complete range (need RPC)")
            elif block_number > max_complete:
                print(f"   ⚠️  Block is AFTER complete range (might not be processed yet)")
            else:
                print(f"   ✅ Block is within complete range")
        else:
            print("   ❌ No complete blocks found")
            
    except Exception as e:
        print(f"   ❌ Range check error: {e}")
    
    # 4. Suggestions
    print(f"\n💡 Troubleshooting Suggestions:")
    
    # Check if any storage exists
    has_any_storage = False
    try:
        for storage_type in ["complete", "processing"]:
            blob_path = gcs.get_blob_string(storage_type, block_number)
            if gcs.blob_exists(blob_path):
                has_any_storage = True
                break
    except:
        pass
    
    if not has_any_storage:
        print("   1. Block may not have been processed yet")
        print("   2. Check if block number is correct")
        print("   3. Verify GCS bucket permissions")
        print("   4. Check if RPC source has this block")
    else:
        print("   1. Storage exists but loading failed - check msgspec compatibility")
        print("   2. Check for corrupted data in GCS")
        print("   3. Verify block format matches expected schema")


def main():
    if len(sys.argv) != 2:
        print("Usage: python block_diagnostic.py <block_number>")
        print("Example: python block_diagnostic.py 58584385")
        sys.exit(1)
    
    try:
        block_number = int(sys.argv[1])
        diagnose_block(block_number)
    except ValueError:
        print("❌ Block number must be an integer")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()