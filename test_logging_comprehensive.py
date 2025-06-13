# test_logging_comprehensive.py
"""
Comprehensive test of the complete logging system
"""

try:
    from indexer import create_indexer
    from indexer.core.logging_config import IndexerLogger
    from pathlib import Path
    
    print("🧪 Starting comprehensive logging test...")
    print("=" * 60)
    
    # Step 1: Create indexer (tests config and container logging)
    print("\n1️⃣  TESTING: Indexer creation and service registration")
    
    config_path = Path("config/config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    container = create_indexer(config_path=str(config_path))
    
    # Step 2: Get some services (tests container dependency injection)
    print("\n2️⃣  TESTING: Service retrieval and dependency injection")
    
    # Get services using the container.get() method
    print("Getting RPC client...")
    from indexer.clients.quicknode_rpc import QuickNodeRpcClient
    rpc_client = container.get(QuickNodeRpcClient)
    
    # Get storage handler
    print("Getting storage handler...")
    from indexer.storage.gcs_handler import GCSHandler
    storage = container.get(GCSHandler)
    
    # Get transformation manager
    print("Getting transformation manager...")
    from indexer.transform.manager import TransformManager
    transform_manager = container.get(TransformManager)
    
    # Get block decoder
    print("Getting block decoder...")
    from indexer.decode.block_decoder import BlockDecoder
    block_decoder = container.get(BlockDecoder)
    
    # Step 3: Test transformer registry (tests transformer setup logging)
    print("\n3️⃣  TESTING: Transformer registry and setup")
    from indexer.transform.registry import TransformRegistry
    registry = container.get(TransformRegistry)
    
    # Get service info
    container_info = container.get_service_info()
    print(f"\n📊 Container info: {container_info['registered_services']} services registered, {container_info['cached_instances']} cached")
    
    # Step 4: Test a simple block processing operation (tests transformer logging)
    print("\n4️⃣  TESTING: Simple block processing (if possible)")
    try:
        # Try to get a block (this might fail if GCS isn't accessible, but will show logging)
        block_number = 54391450
        print(f"Attempting to retrieve block {block_number}...")
        raw_block = storage.get_rpc_block(block_number)
        
        if raw_block:
            print(f"✅ Retrieved block with {len(raw_block.transactions)} transactions")
            
            # Try to decode first transaction only (to see transformer logging)
            if raw_block.transactions:
                print("Attempting to decode block...")
                decoded_block = block_decoder.decode_block(raw_block)
                print(f"✅ Decoded block with {len(decoded_block.transactions)} transactions")
                
                # Try to transform first transaction only
                if decoded_block.transactions:
                    first_tx = list(decoded_block.transactions.values())[0]
                    print(f"Attempting to transform transaction {first_tx.tx_hash}...")
                    success, transformed_tx = transform_manager.process_transaction(first_tx)
                    
                    if success:
                        transfer_count = len(transformed_tx.transfers) if transformed_tx.transfers else 0
                        event_count = len(transformed_tx.events) if transformed_tx.events else 0
                        error_count = len(transformed_tx.errors) if transformed_tx.errors else 0
                        
                        print(f"✅ Transformation completed:")
                        print(f"   Transfers: {transfer_count}")
                        print(f"   Events: {event_count}")
                        print(f"   Errors: {error_count}")
                    else:
                        print("⚠️  Transformation skipped (no decoded logs or failed tx)")
                else:
                    print("⚠️  No transactions in decoded block")
            else:
                print("⚠️  No transactions in raw block")
        else:
            print("⚠️  Block not found (expected if no GCS access)")
            
    except Exception as e:
        print(f"⚠️  Block processing failed (expected if no GCS access): {e}")
    
    print("\n" + "=" * 60)
    print("🎉 COMPREHENSIVE LOGGING TEST COMPLETED!")
    print("\n✅ What was tested:")
    print("   - Indexer creation and early logging setup")
    print("   - Configuration loading with hierarchical loggers")
    print("   - Service registration in container")
    print("   - Dependency injection logging")
    print("   - Factory service creation")
    print("   - Transformer registry and setup")
    print("   - Block processing pipeline (if GCS accessible)")
    print("\n📝 Check the logs above for:")
    print("   - Structured JSON output")
    print("   - Hierarchical logger names (indexer.core.*, indexer.transform.*, etc.)")
    print("   - Context information in log messages")
    print("   - Dependency resolution details")
    print("   - Service creation and caching")
    
except Exception as e:
    print(f"❌ Comprehensive test failed: {e}")
    import traceback
    traceback.print_exc()