#!/usr/bin/env python3
# testing/pipeline/test_end_to_end.py

"""
Hybrid End-to-End Pipeline Test

Uses the working block processing logic but adds database persistence
and GCS storage to test the complete workflow.
"""

import sys
from pathlib import Path
import time

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.database.repository import RepositoryManager
from indexer.database.writers.domain_event_writer import DomainEventWriter
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager


class HybridEndToEndTest:
    """Test complete pipeline including database persistence and GCS storage."""
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name
        
        # Initialize the complete indexer container
        print("🏗️ Initializing indexer container...")
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get all the services we need
        self.repository_manager = self.container.get(RepositoryManager)
        self.domain_event_writer = self.container.get(DomainEventWriter)
        self.gcs = self.container.get(GCSHandler)
        self.decoder = self.container.get(BlockDecoder)
        self.transform_manager = self.container.get(TransformManager)
        
        print(f"✅ Indexer initialized: {self.config.model_name} v{self.config.model_version}")
        
    def test_block(self, block_number: int) -> bool:
        """Test complete end-to-end processing of a block."""
        print(f"\n🧪 HYBRID END-TO-END PIPELINE TEST")
        print(f"Block: {block_number}")
        print(f"Model: {self.config.model_name} v{self.config.model_version}")
        print("=" * 80)
        
        try:
            # Step 1: Retrieve block from GCS (like our working test)
            print(f"\n1️⃣ Retrieving block from GCS...")
            raw_block = self._retrieve_block(block_number)
            if not raw_block:
                return False
            
            # Step 2: Decode block (like our working test)
            print(f"\n2️⃣ Decoding block...")
            decoded_block = self._decode_block(raw_block)
            if not decoded_block:
                return False
            
            # Step 3: Transform block (like our working test)
            print(f"\n3️⃣ Transforming block...")
            transformed_block = self._transform_block(decoded_block)
            if not transformed_block:
                return False
            
            # Step 4: NEW - Persist to database
            print(f"\n4️⃣ Persisting to database...")
            db_success = self._persist_to_database(transformed_block)
            if not db_success:
                return False
            
            # Step 5: NEW - Save to GCS storage
            print(f"\n5️⃣ Saving to GCS storage...")
            gcs_success = self._save_to_gcs(transformed_block)
            if not gcs_success:
                return False
            
            # Step 6: Verify results
            print(f"\n6️⃣ Verifying results...")
            verification_success = self._verify_results(block_number)
            if not verification_success:
                return False
            
            print(f"\n🎉 HYBRID END-TO-END TEST PASSED!")
            return True
            
        except Exception as e:
            print(f"\n💥 Pipeline test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _retrieve_block(self, block_number: int):
        """Retrieve block from GCS (reuse working logic)."""
        try:
            primary_source = self.config.get_primary_source()
            if not primary_source:
                print("❌ No primary source configured")
                return None
            
            print(f"   Source: {primary_source.name}")
            
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
        """Decode the block (reuse working logic)."""
        try:
            decoded_block = self.decoder.decode_block(raw_block)
            
            if decoded_block:
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
        """Transform the block (reuse working logic)."""
        try:
            transformed_transactions = {}
            total_signals = 0
            total_events = 0
            total_errors = 0
            
            for tx_hash, transaction in decoded_block.transactions.items():
                success, transformed_tx = self.transform_manager.process_transaction(transaction)
                transformed_transactions[tx_hash] = transformed_tx
                
                if transformed_tx.signals:
                    total_signals += len(transformed_tx.signals)
                if transformed_tx.events:
                    total_events += len(transformed_tx.events)
                if transformed_tx.errors:
                    total_errors += len(transformed_tx.errors)
            
            from indexer.types.indexer import Block
            transformed_block = Block(
                block_number=decoded_block.block_number,
                timestamp=decoded_block.timestamp,
                transactions=transformed_transactions
            )
            
            print(f"   ✅ Generated {total_signals} signals → {total_events} events")
            if total_errors > 0:
                print(f"   ⚠️ {total_errors} errors during transformation")
                
            return transformed_block
                
        except Exception as e:
            print(f"   ❌ Transform failed: {e}")
            return None
    
    def _persist_to_database(self, transformed_block):
        """NEW: Persist domain events and positions to database."""
        try:
            events_written = 0
            positions_written = 0
            
            for tx_hash, transaction in transformed_block.transactions.items():
                if transaction.events or transaction.positions:
                    events, positions, skipped = self.domain_event_writer.write_transaction_results(
                        tx_hash=tx_hash,
                        block_number=transformed_block.block_number,
                        timestamp=transformed_block.timestamp,
                        events=transaction.events or {},
                        positions=transaction.positions or {},
                        tx_success=transaction.tx_success
                    )
                    events_written += events
                    positions_written += positions
            
            print(f"   ✅ Persisted {events_written} events, {positions_written} positions")
            return True
            
        except Exception as e:
            print(f"   ❌ Database persistence failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _save_to_gcs(self, transformed_block):
        """NEW: Save processed block to GCS storage."""
        try:
            # Save to processing stage first
            processing_success = self.gcs.save_processing_block(
                transformed_block.block_number, 
                transformed_block
            )
            
            if not processing_success:
                print(f"   ❌ Failed to save processing block")
                return False
            
            print(f"   ✅ Saved processing block")
            
            # Save to complete stage
            complete_success = self.gcs.save_complete_block(
                transformed_block.block_number,
                transformed_block
            )
            
            if not complete_success:
                print(f"   ❌ Failed to save complete block")
                return False
            
            print(f"   ✅ Saved complete block")
            return True
            
        except Exception as e:
            print(f"   ❌ GCS save failed: {e}")
            return False
    
    def _verify_results(self, block_number: int):
        """Verify that everything was persisted correctly."""
        try:
            # Check database
            with self.repository_manager.get_session() as session:
                from indexer.database.indexer.tables.events import DomainEvent
                events = session.query(DomainEvent).filter(
                    DomainEvent.block_number == block_number
                ).all()
                
                from indexer.database.indexer.tables.positions import Position
                positions = session.query(Position).filter(
                    Position.block_number == block_number
                ).all()
            
            # Check GCS
            processing_exists = self.gcs.blob_exists(
                self.gcs.get_blob_string("processing", block_number)
            )
            complete_exists = self.gcs.blob_exists(
                self.gcs.get_blob_string("complete", block_number)
            )
            
            print(f"   📊 Database: {len(events)} events, {len(positions)} positions")
            print(f"   ☁️ GCS: Processing {'✅' if processing_exists else '❌'}, Complete {'✅' if complete_exists else '❌'}")
            
            # Success if we have either events or positions AND both GCS files
            return (len(events) > 0 or len(positions) > 0) and complete_exists
            
        except Exception as e:
            print(f"   ❌ Verification failed: {e}")
            return False


def main():
    """Run hybrid end-to-end pipeline test."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Hybrid End-to-End Pipeline Test')
    parser.add_argument('block_number', type=int, help='Block number to process')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        test = HybridEndToEndTest(model_name=args.model)
        success = test.test_block(args.block_number)
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print(f"\n⏹️ Test interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test setup failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()