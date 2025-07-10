#!/usr/bin/env python3
# testing/pipeline/test_transaction.py
"""
Test Transaction Processing

Tests processing a specific transaction through the pipeline.
"""

import sys
import json
from pathlib import Path
from typing import Optional, Dict, Any

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager


class TransactionProcessingTest:
    """Test processing a specific transaction."""
    
    def __init__(self, model_name: str = None):
        self.env = get_testing_environment(model_name=model_name)
        self.config = self.env.get_config()
        
        # Get pipeline services
        self.gcs = self.env.get_service(GCSHandler)
        self.decoder = self.env.get_service(BlockDecoder)
        self.transform_manager = self.env.get_service(TransformManager)
        
    def test_transaction(self, tx_hash: str, block_number: int) -> bool:
        """Test processing a specific transaction."""
        print(f"🧪 Testing Transaction Processing")
        print(f"Transaction: {tx_hash[:10]}...")
        print(f"Block: {block_number}")
        print(f"Model: {self.config.model_name} v{self.config.model_version}")
        print("=" * 60)
        
        try:
            # Step 1: Get block containing transaction
            print(f"\n1️⃣ Retrieving block {block_number}...")
            raw_block = self._retrieve_block(block_number)
            if not raw_block:
                return False
            
            # Step 2: Find transaction in block
            print(f"\n2️⃣ Finding transaction in block...")
            if not self._verify_transaction_exists(raw_block, tx_hash):
                return False
            
            # Step 3: Decode block
            print(f"\n3️⃣ Decoding block...")
            decoded_block = self._decode_block(raw_block)
            if not decoded_block:
                return False
            
            # Step 4: Get decoded transaction
            decoded_tx = decoded_block.transactions.get(tx_hash)
            if not decoded_tx:
                print(f"   ❌ Transaction not found in decoded block")
                return False
                
            print(f"   ✅ Found transaction with {len(decoded_tx.logs)} logs")
            
            # Step 5: Process transaction
            print(f"\n4️⃣ Processing transaction...")
            processed_tx = self._process_transaction(decoded_tx)
            if not processed_tx:
                return False
            
            # Print detailed results
            self._print_transaction_details(decoded_tx, processed_tx)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Test failed: {e}")
            return False
    
    def _retrieve_block(self, block_number: int):
        """Retrieve block from GCS."""
        try:
            primary_source = self.config.get_primary_source()
            if not primary_source:
                print("❌ No primary source configured")
                return None
            
            raw_block = self.gcs.get_rpc_block(block_number, source=primary_source)
            
            if raw_block:
                print(f"   ✅ Retrieved block with {len(raw_block.transactions)} transactions")
                return raw_block
            else:
                print(f"   ❌ Block not found")
                return None
                
        except Exception as e:
            print(f"   ❌ Failed to retrieve: {e}")
            return None
    
    def _verify_transaction_exists(self, raw_block, tx_hash: str) -> bool:
        """Verify transaction exists in raw block."""
        tx_hashes = []
        
        for tx in raw_block.transactions:
            if hasattr(tx, 'hash'):
                tx_hash_str = tx.hash.hex() if hasattr(tx.hash, 'hex') else str(tx.hash)
                tx_hashes.append(tx_hash_str)
                
                if tx_hash_str.lower() == tx_hash.lower():
                    print(f"   ✅ Transaction found in block")
                    return True
        
        print(f"   ❌ Transaction not found in block")
        print(f"   Available transactions: {len(tx_hashes)}")
        if len(tx_hashes) < 10:
            for h in tx_hashes:
                print(f"     • {h[:10]}...")
                
        return False
    
    def _decode_block(self, raw_block):
        """Decode the block."""
        try:
            decoded_block = self.decoder.decode_block(raw_block)
            
            if decoded_block:
                print(f"   ✅ Block decoded successfully")
                return decoded_block
            else:
                print(f"   ❌ Failed to decode block")
                return None
                
        except Exception as e:
            print(f"   ❌ Decode failed: {e}")
            return None
    
    def _process_transaction(self, decoded_tx):
        """Process the transaction through transform system."""
        try:
            success, processed_tx = self.transform_manager.process_transaction(decoded_tx)
            
            if success and processed_tx:
                signal_count = len(processed_tx.signals) if processed_tx.signals else 0
                event_count = len(processed_tx.events) if processed_tx.events else 0
                error_count = len(processed_tx.errors) if processed_tx.errors else 0
                
                print(f"   ✅ Generated {signal_count} signals → {event_count} events")
                if error_count > 0:
                    print(f"   ⚠️ {error_count} errors during processing")
                    
                return processed_tx
            else:
                print(f"   ❌ Transaction processing failed")
                return None
                
        except Exception as e:
            print(f"   ❌ Processing failed: {e}")
            return None
    
    def _print_transaction_details(self, decoded_tx, processed_tx):
        """Print detailed transaction results."""
        print(f"\n📋 Transaction Details")
        print("─" * 60)
        
        # Basic info
        print(f"From: {decoded_tx.from_address}")
        print(f"To: {decoded_tx.to_address}")
        print(f"Value: {decoded_tx.value}")
        print(f"Gas Used: {decoded_tx.gas_used}")
        
        # Logs
        print(f"\nLogs: {len(decoded_tx.logs)}")
        for i, log in enumerate(decoded_tx.logs[:3]):  # First 3 logs
            print(f"  Log {i}: {log.address} - {log.topics[0][:10] if log.topics else 'no topics'}...")
            
        # Signals
        if processed_tx.signals:
            print(f"\nSignals: {len(processed_tx.signals)}")
            for i, signal in enumerate(processed_tx.signals[:3]):
                print(f"  Signal {i}: {type(signal).__name__}")
                
        # Events
        if processed_tx.events:
            print(f"\nEvents: {len(processed_tx.events)}")
            for i, event in enumerate(processed_tx.events[:5]):
                print(f"  Event {i}: {type(event).__name__}")
                # Print key event attributes
                if hasattr(event, 'user_address'):
                    print(f"    User: {event.user_address}")
                if hasattr(event, 'token_amount'):
                    print(f"    Amount: {event.token_amount}")
        else:
            print(f"\nNo events generated")
            
        # Errors
        if processed_tx.errors:
            print(f"\nErrors: {len(processed_tx.errors)}")
            for i, error in enumerate(processed_tx.errors[:3]):
                print(f"  Error {i}: {error}")


def main():
    """Run transaction processing test."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Transaction Processing')
    parser.add_argument('tx_hash', help='Transaction hash to test')
    parser.add_argument('block_number', type=int, help='Block number containing transaction')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        test = TransactionProcessingTest(model_name=args.model)
        success = test.test_transaction(args.tx_hash, args.block_number)
        
        if success:
            print("\n✅ Transaction processing test passed!")
        else:
            print("\n❌ Transaction processing test failed")
            
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()