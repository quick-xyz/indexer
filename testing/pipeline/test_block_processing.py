#!/usr/bin/env python3
# testing/diagnostics/test_block_processing.py

"""
Test Block Processing

Tests processing a single block through the entire pipeline.
"""

import sys
from pathlib import Path
from typing import Optional

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager


class BlockProcessingTest:
    """Test processing a single block."""
    
    def __init__(self, model_name: str = None):
        self.env = get_testing_environment(model_name=model_name)
        self.config = self.env.get_config()
        
        # Get pipeline services
        self.gcs = self.env.get_service(GCSHandler)
        self.decoder = self.env.get_service(BlockDecoder)
        self.transform_manager = self.env.get_service(TransformManager)
        
    def test_block(self, block_number: int) -> bool:
        """Test processing a specific block."""
        print(f"🧪 Testing Block Processing")
        print(f"Block: {block_number}")
        print(f"Model: {self.config.model_name} v{self.config.model_version}")
        print("=" * 60)
        
        try:
            # Step 1: Retrieve block from GCS
            print(f"\n1️⃣ Retrieving block from GCS...")
            raw_block = self._retrieve_block(block_number)
            if not raw_block:
                return False
            
            # Step 2: Decode block
            print(f"\n2️⃣ Decoding block...")
            decoded_block = self._decode_block(raw_block)
            if not decoded_block:
                return False
            
            # Step 3: Transform block
            print(f"\n3️⃣ Transforming block...")
            transformed_block = self._transform_block(decoded_block)
            if not transformed_block:
                return False
            
            # Summary
            self._print_summary(transformed_block)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Test failed: {e}")
            return False
    
    def _retrieve_block(self, block_number: int):
        """Retrieve block from GCS."""
        try:
            # Get primary source
            primary_source = self.config.get_primary_source()
            if not primary_source:
                print("❌ No primary source configured")
                return None
            
            print(f"   Source: {primary_source.name}")
            
            # Get block from GCS
            raw_block = self.gcs.get_rpc_block(block_number, source=primary_source)
            
            if raw_block:
                tx_count = len(raw_block.transactions) if raw_block.transactions else 0
                print(f"   ✅ Retrieved block with {tx_count} transactions")
                return raw_block
            else:
                print(f"   ❌ Block not found in GCS")
                return None
                
        except Exception as e:
            print(f"   ❌ Failed to retrieve: {e}")
            return None
    
    def _decode_block(self, raw_block):
        """Decode the block."""
        try:
            decoded_block = self.decoder.decode_block(raw_block)
            
            if decoded_block:
                # Count decoded transactions
                tx_count = len(decoded_block.transactions)
                log_count = sum(
                    len(tx.logs) for tx in decoded_block.transactions.values()
                )
                
                print(f"   ✅ Decoded {tx_count} transactions with {log_count} logs")
                return decoded_block
            else:
                print(f"   ❌ Failed to decode block")
                return None
                
        except Exception as e:
            print(f"   ❌ Decode failed: {e}")
            return None
    
    def _transform_block(self, decoded_block):
        """Transform the block."""
        try:
            success, transformed_block = self.transform_manager.process_block(decoded_block)
            
            if success and transformed_block:
                # Count results
                signal_count = sum(
                    len(tx.signals) if tx.signals else 0
                    for tx in transformed_block.transactions.values()
                )
                event_count = sum(
                    len(tx.events) if tx.events else 0
                    for tx in transformed_block.transactions.values()
                )
                error_count = sum(
                    len(tx.errors) if tx.errors else 0
                    for tx in transformed_block.transactions.values()
                )
                
                print(f"   ✅ Generated {signal_count} signals → {event_count} events")
                if error_count > 0:
                    print(f"   ⚠️ {error_count} errors during transformation")
                    
                return transformed_block
            else:
                print(f"   ❌ Transform failed")
                return None
                
        except Exception as e:
            print(f"   ❌ Transform failed: {e}")
            return None
    
    def _print_summary(self, transformed_block):
        """Print processing summary."""
        print(f"\n📊 Processing Summary")
        print("─" * 60)
        
        # Transaction summary
        total_tx = len(transformed_block.transactions)
        tx_with_events = sum(
            1 for tx in transformed_block.transactions.values()
            if tx.events and len(tx.events) > 0
        )
        
        print(f"Transactions processed: {total_tx}")
        print(f"Transactions with events: {tx_with_events}")
        
        # Event type breakdown
        event_types = {}
        for tx in transformed_block.transactions.values():
            if tx.events:
                for event in tx.events:
                    event_type = type(event).__name__
                    event_types[event_type] = event_types.get(event_type, 0) + 1
        
        if event_types:
            print(f"\nEvent Types:")
            for event_type, count in sorted(event_types.items()):
                print(f"  • {event_type}: {count}")
        else:
            print(f"\nNo events generated")
        
        # Sample transaction details
        if tx_with_events > 0:
            print(f"\nSample Transaction:")
            for tx_hash, tx in transformed_block.transactions.items():
                if tx.events and len(tx.events) > 0:
                    print(f"  Hash: {tx_hash[:10]}...")
                    print(f"  Events: {len(tx.events)}")
                    print(f"  First event: {type(tx.events[0]).__name__}")
                    break


def main():
    """Run block processing test."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Block Processing')
    parser.add_argument('block_number', type=int, help='Block number to test')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        test = BlockProcessingTest(model_name=args.model)
        success = test.test_block(args.block_number)
        
        if success:
            print("\n✅ Block processing test passed!")
        else:
            print("\n❌ Block processing test failed")
            
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 Test failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()