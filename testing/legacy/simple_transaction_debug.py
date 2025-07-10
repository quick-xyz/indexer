#!/usr/bin/env python3
"""
Simple Transaction Debug - Step by step analysis
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment

def debug_transaction_step_by_step(tx_hash: str, block_number: int, model_name: str = None):
    """Debug transaction step by step with detailed output"""
    
    print(f"🔍 Step-by-step transaction debug")
    print(f"Transaction: {tx_hash}")
    print(f"Block: {block_number}")
    print(f"Model: {model_name}")
    print("=" * 60)
    
    try:
        # Step 1: Initialize environment
        print("📚 Step 1: Initialize testing environment...")
        testing_env = get_testing_environment(model_name=model_name, log_level="ERROR")
        config = testing_env.get_config()
        print(f"✅ Environment initialized for model: {config.model_name}")
        
        # Step 2: Check source configuration
        print("\n📋 Step 2: Check source configuration...")
        primary_source = config.get_primary_source()
        if not primary_source:
            print("❌ No primary source found")
            return
        print(f"✅ Primary source: {primary_source.name}")
        print(f"   Path: {primary_source.path}")
        print(f"   Format: {primary_source.format}")
        
        # Step 3: Get storage handler
        print("\n☁️  Step 3: Get storage handler...")
        from indexer.storage.gcs_handler import GCSHandler
        storage_handler = testing_env.get_service(GCSHandler)
        print(f"✅ Storage handler initialized")
        print(f"   Bucket: {storage_handler.bucket_name}")
        
        # Step 4: Retrieve raw block
        print(f"\n📦 Step 4: Retrieve raw block {block_number}...")
        raw_block = storage_handler.get_rpc_block(block_number, source=primary_source)
        if not raw_block:
            print("❌ Raw block not found")
            return
        print(f"✅ Raw block retrieved")
        print(f"   Transactions: {len(raw_block.transactions) if raw_block.transactions else 0}")
        print(f"   Receipts: {len(raw_block.receipts) if raw_block.receipts else 0}")
        
        # Step 5: Decode block
        print(f"\n🔓 Step 5: Decode block...")
        from indexer.decode.block_decoder import BlockDecoder
        block_decoder = testing_env.get_service(BlockDecoder)
        
        try:
            decoded_block = block_decoder.decode_block(raw_block)
            print(f"✅ Block decoded successfully")
            print(f"   Block number: {decoded_block.block_number}")
            print(f"   Timestamp: {decoded_block.timestamp}")
            print(f"   Transactions: {len(decoded_block.transactions) if decoded_block.transactions else 0}")
        except Exception as e:
            print(f"❌ Block decoding failed: {e}")
            return
        
        # Step 6: Find target transaction
        print(f"\n🎯 Step 6: Find target transaction...")
        if not decoded_block.transactions:
            print("❌ No transactions in decoded block")
            return
        
        if tx_hash not in decoded_block.transactions:
            print(f"❌ Target transaction not found")
            print(f"   Available transactions: {list(decoded_block.transactions.keys())}")
            return
        
        transaction = decoded_block.transactions[tx_hash]
        print(f"✅ Target transaction found")
        print(f"   From: {transaction.origin_from}")
        print(f"   To: {transaction.origin_to}")
        print(f"   Success: {transaction.tx_success}")
        print(f"   Logs: {len(transaction.logs) if transaction.logs else 0}")
        
        # Step 7: Analyze logs
        print(f"\n📋 Step 7: Analyze transaction logs...")
        if not transaction.logs:
            print("❌ No logs in transaction")
            return
        
        decoded_count = 0
        encoded_count = 0
        
        for log_index, log in transaction.logs.items():
            log_type = getattr(log, 'type', type(log).__name__)
            if hasattr(log, 'name'):
                decoded_count += 1
                print(f"   Log {log_index}: {log_type} - {log.name} (DECODED)")
            else:
                encoded_count += 1
                print(f"   Log {log_index}: {log_type} - {log.signature[:10]}... (ENCODED)")
        
        print(f"   Decoded logs: {decoded_count}")
        print(f"   Encoded logs: {encoded_count}")
        
        if decoded_count == 0:
            print("❌ ISSUE: No logs were decoded - this explains why no signals are generated")
            
            # Step 8: Check contract configuration
            print(f"\n🔧 Step 8: Check contract configuration...")
            contracts = config.contracts
            print(f"   Configured contracts: {len(contracts)}")
            
            # Check if any log contracts are in our configuration
            log_contracts = set()
            for log in transaction.logs.values():
                if hasattr(log, 'contract'):
                    log_contracts.add(log.contract.lower())
            
            print(f"   Log contracts: {log_contracts}")
            
            configured_contracts = set(addr.lower() for addr in contracts.keys())
            print(f"   Configured contracts: {configured_contracts}")
            
            missing_contracts = log_contracts - configured_contracts
            if missing_contracts:
                print(f"❌ Missing contract configurations: {missing_contracts}")
            else:
                print(f"✅ All log contracts are configured")
                print(f"❌ Issue must be in ABI loading or ContractManager")
        
        else:
            print(f"✅ Logs are being decoded correctly")
        
        # Step 9: Check signals (if any)
        print(f"\n🚀 Step 9: Check signal generation...")
        if hasattr(transaction, 'signals') and transaction.signals:
            print(f"✅ Signals generated: {len(transaction.signals)}")
        else:
            print(f"❌ No signals generated")
        
        print(f"\n📋 Summary:")
        print(f"   Raw block: {'✅' if raw_block else '❌'}")
        print(f"   Block decoding: {'✅' if decoded_block else '❌'}")
        print(f"   Transaction found: {'✅' if transaction else '❌'}")
        print(f"   Logs present: {'✅' if transaction.logs else '❌'}")
        print(f"   Logs decoded: {'✅' if decoded_count > 0 else '❌'}")
        print(f"   Signals generated: {'✅' if hasattr(transaction, 'signals') and transaction.signals else '❌'}")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    if len(sys.argv) < 3:
        print("Usage: python simple_transaction_debug.py <tx_hash> <block_number> [model_name]")
        sys.exit(1)
    
    tx_hash = sys.argv[1]
    block_number = int(sys.argv[2])
    model_name = sys.argv[3] if len(sys.argv) > 3 else None
    
    debug_transaction_step_by_step(tx_hash, block_number, model_name)

if __name__ == "__main__":
    main()