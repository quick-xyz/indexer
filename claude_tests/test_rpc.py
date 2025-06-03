# scripts/test_rpc.py

"""
Phase 2 Test: RPC Client Functionality
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_rpc_connection():
    """Test RPC client connection and basic functionality"""
    print("🔧 Testing RPC Connection...")
    
    try:
        from indexer import create_indexer
        from indexer.clients.quicknode_rpc import QuickNodeRpcClient
        
        # Create indexer
        config_path = project_root / "config" / "config.json"
        indexer = create_indexer(config_path=str(config_path))
        
        # Get RPC client
        rpc = indexer.get(QuickNodeRpcClient)
        print("✅ RPC client created")
        
        # Test connection
        latest_block = rpc.get_latest_block_number()
        print(f"✅ RPC connection successful - Latest block: {latest_block}")
        
        # Test block retrieval
        if latest_block > 0:
            block = rpc.get_block(latest_block - 1)  # Get previous block (more stable)
            print(f"✅ Block retrieval successful - Block {block['number']} has {len(block['transactions'])} transactions")
            
            # Test transaction receipt if block has transactions
            if block['transactions']:
                tx_hash = block['transactions'][0]['hash']
                if hasattr(tx_hash, 'hex'):
                    tx_hash = tx_hash.hex()
                receipt = rpc.get_transaction_receipt(tx_hash)
                print(f"✅ Receipt retrieval successful - Receipt has {len(receipt['logs'])} logs")
        
        return True
        
    except Exception as e:
        print(f"❌ RPC test failed: {e}")
        print("   Check INDEXER_AVAX_RPC environment variable")
        return False

def test_rpc_batch_operations():
    """Test RPC batch operations and block with receipts"""
    print("\n🔧 Testing RPC Batch Operations...")
    
    try:
        from indexer import create_indexer
        from indexer.clients.quicknode_rpc import QuickNodeRpcClient
        
        config_path = project_root / "config" / "config.json"
        indexer = create_indexer(config_path=str(config_path))
        rpc = indexer.get(QuickNodeRpcClient)
        
        latest_block = rpc.get_latest_block_number()
        test_block = latest_block - 10  # Use older block for stability
        
        # Test block with receipts
        block_with_receipts = rpc.get_block_with_receipts(test_block)
        print(f"✅ Block with receipts retrieved - Block {test_block}")
        
        # Verify receipts are included
        tx_with_receipts = 0
        for tx in block_with_receipts['transactions']:
            if 'receipt' in tx:
                tx_with_receipts += 1
        
        print(f"✅ {tx_with_receipts}/{len(block_with_receipts['transactions'])} transactions have receipts")
        
        return True
        
    except Exception as e:
        print(f"❌ RPC batch operations test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("PHASE 2: RPC Client Tests")
    print("=" * 60)
    
    success = True
    success &= test_rpc_connection()
    success &= test_rpc_batch_operations()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 Phase 2 RPC tests PASSED - RPC client working")
    else:
        print("💥 Phase 2 RPC tests FAILED - Check RPC configuration")
        sys.exit(1)


