# testing/diagnostics/quick_signal_check.py
"""
Quick Signal Generation Check
Rapidly identifies why no signals are being generated
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.transform.registry import TransformRegistry
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager


def quick_check():
    """Quick diagnostic of the most common signal generation issues"""
    
    print("🔍 QUICK SIGNAL GENERATION CHECK")
    print("=" * 40)
    
    try:
        # Initialize environment quietly
        testing_env = get_testing_environment(log_level="ERROR")
        
        # Test 1: Check if any transformers are registered
        transformer_registry = testing_env.get_service(TransformRegistry)
        all_transformers = transformer_registry.get_all_contracts()
        
        print(f"📋 Registered Transformers: {len(all_transformers)}")
        
        if len(all_transformers) == 0:
            print("❌ CRITICAL: No transformers registered!")
            print("   Check config.json transform configurations")
            return False
        
        # Test 2: Check specific transaction
        tx_hash = "0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"
        block_number = 63269916
        
        storage_handler = testing_env.get_service(GCSHandler)
        block_decoder = testing_env.get_service(BlockDecoder)
        
        # Get transaction
        raw_block = storage_handler.get_rpc_block(block_number)
        if not raw_block:
            print(f"❌ Block {block_number} not found in storage")
            return False
        
        decoded_block = block_decoder.decode_block(raw_block)
        if not decoded_block.transactions or tx_hash not in decoded_block.transactions:
            print(f"❌ Transaction {tx_hash[:10]}... not found")
            return False
        
        transaction = decoded_block.transactions[tx_hash]
        print(f"✅ Transaction found with {len(transaction.logs)} logs")
        
        # Test 3: Check decoded logs
        decoded_logs = [log for log in transaction.logs.values() if hasattr(log, 'name')]
        print(f"✅ Decoded logs: {len(decoded_logs)}/{len(transaction.logs)}")
        
        if len(decoded_logs) == 0:
            print("❌ CRITICAL: No logs were decoded!")
            print("   Check ABI files and contract registry")
            return False
        
        # Test 4: Check transformer mapping for each decoded log
        print(f"\n📊 Log Analysis:")
        mapped_logs = 0
        
        for log_idx, log in transaction.logs.items():
            if not hasattr(log, 'name'):
                continue
                
            transformer = transformer_registry.get_transformer(log.contract)
            has_handler = False
            
            if transformer and hasattr(transformer, 'handler_map'):
                has_handler = log.name in transformer.handler_map
            
            status = "✅" if transformer and has_handler else "❌"
            transformer_name = type(transformer).__name__ if transformer else "None"
            
            print(f"   {status} Log {log_idx}: {log.name} @ {log.contract[:8]}... -> {transformer_name}")
            
            if transformer and has_handler:
                mapped_logs += 1
            elif transformer:
                available_handlers = list(transformer.handler_map.keys()) if hasattr(transformer, 'handler_map') else []
                print(f"      ⚠️  Available handlers: {available_handlers}")
            else:
                print(f"      ❌ No transformer for contract {log.contract}")
        
        print(f"\n📈 Summary: {mapped_logs}/{len(decoded_logs)} logs can generate signals")
        
        # Test 5: Quick signal generation test
        if mapped_logs > 0:
            print(f"\n🔬 Testing signal generation...")
            
            transform_manager = testing_env.get_service(TransformManager)
            
            try:
                success, processed_tx = transform_manager.process_transaction(transaction)
                
                signal_count = len(processed_tx.signals) if processed_tx.signals else 0
                error_count = len(processed_tx.errors) if processed_tx.errors else 0
                
                print(f"   Generated: {signal_count} signals, {error_count} errors")
                
                if signal_count == 0 and error_count == 0:
                    print("   ⚠️  No signals or errors - may indicate validation issues")
                    return False
                elif signal_count > 0:
                    print("   ✅ Signals generated successfully!")
                    return True
                else:
                    print("   ❌ Errors during signal generation")
                    print("   Use the full troubleshooter for detailed error analysis")
                    return False
                    
            except Exception as e:
                print(f"   ❌ Signal generation exception: {e}")
                return False
        else:
            print(f"\n❌ No logs can generate signals - check transformer configuration")
            return False
            
    except Exception as e:
        print(f"❌ Check failed: {e}")
        return False


def print_next_steps(success: bool):
    """Print next debugging steps"""
    
    print(f"\n🎯 NEXT STEPS:")
    
    if success:
        print("   ✅ Signal generation is working!")
        print("   Run full pipeline test to see complete flow")
    else:
        print("   ❌ Issues found. Recommended actions:")
        print("   1. Run full troubleshooter:")
        print("      python signal_troubleshooter.py 0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b 63269916")
        print("   2. Check config.json transformer configurations")
        print("   3. Verify ABI files exist in config/abis/")
        print("   4. Enable DEBUG logging and run test_pipeline.py")


if __name__ == "__main__":
    try:
        success = quick_check()
        print_next_steps(success)
    except KeyboardInterrupt:
        print("\n⏹️  Check interrupted")
    except Exception as e:
        print(f"\n💥 Check failed: {e}")