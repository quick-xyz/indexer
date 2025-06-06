# testing/test_pipeline.py
"""
Pipeline Testing Script for Blockchain Indexer

Uses the indexer's architecture and logging system for comprehensive testing.
Moved from root to testing/ directory and enhanced to use proper DI.
"""

import sys
import logging
from datetime import datetime
from typing import Optional
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.core.logging_config import log_with_context
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformationManager
from indexer.types import EvmFilteredBlock, Block


class PipelineTester:
    """
    Pipeline tester using the indexer's dependency injection system
    """
    
    def __init__(self, config_path: str = None):
        self.testing_env = get_testing_environment(config_path, log_level="DEBUG")
        self.logger = self.testing_env.get_logger("pipeline.tester")
        
        # Get services from DI container
        self.rpc_client = self.testing_env.get_service(QuickNodeRpcClient)
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformationManager)
        
        log_with_context(
            self.logger,
            logging.INFO,
            "Pipeline tester initialized",
            indexer_name=self.testing_env.config.name,
            indexer_version=self.testing_env.config.version
        )
    
    def test_complete_pipeline(self, block_number: int) -> bool:
        """Test the complete pipeline for a specific block"""
        log_with_context(
            self.logger,
            logging.INFO,
            "Starting complete pipeline test",
            block_number=block_number
        )
        
        try:
            # Phase 1: Block Retrieval
            raw_block = self._test_block_retrieval(block_number)
            if raw_block is None:
                return False
            
            # Phase 2: Block Decoding
            decoded_block = self._test_block_decoding(raw_block)
            if decoded_block is None:
                return False
            
            # Phase 3: Transformation
            transformed_block = self._test_transformation(decoded_block)
            if transformed_block is None:
                return False
            
            # Phase 4: Storage
            success = self._test_storage(transformed_block)
            
            if success:
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Complete pipeline test passed",
                    block_number=block_number
                )
            
            return success
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Pipeline test failed with exception",
                block_number=block_number,
                error=str(e),
                exception_type=type(e).__name__
            )
            import traceback
            traceback.print_exc()
            return False
    
    def _test_block_retrieval(self, block_number: int) -> Optional[EvmFilteredBlock]:
        """Test block retrieval from GCS"""
        print(f"\n=== Test 1: Block Retrieval (Block {block_number}) ===")
        
        log_with_context(
            self.logger,
            logging.INFO,
            "Testing block retrieval",
            block_number=block_number
        )
        
        try:
            raw_block = self.storage_handler.get_rpc_block(block_number)
            
            if raw_block is None:
                log_with_context(
                    self.logger,
                    logging.ERROR,
                    "Block not found in storage",
                    block_number=block_number
                )
                print(f"❌ Block {block_number} not found in GCS")
                return None
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Block retrieved successfully",
                block_number=block_number,
                transaction_count=len(raw_block.transactions),
                receipt_count=len(raw_block.receipts)
            )
            
            print(f"✅ Retrieved block {block_number}")
            print(f"   Transactions: {len(raw_block.transactions)}")
            print(f"   Receipts: {len(raw_block.receipts)}")
            
            # Validation
            if len(raw_block.transactions) != len(raw_block.receipts):
                self.logger.warning(
                    "Transaction/receipt count mismatch",
                    block_number=block_number,
                    tx_count=len(raw_block.transactions),
                    receipt_count=len(raw_block.receipts)
                )
                print(f"⚠️  Warning: TX count != Receipt count")
            
            return raw_block
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Block retrieval failed",
                block_number=block_number,
                error=str(e)
            )
            print(f"❌ Block retrieval failed: {e}")
            return None
    
    def _test_block_decoding(self, raw_block: EvmFilteredBlock) -> Optional[Block]:
        """Test block decoding"""
        print(f"\n=== Test 2: Block Decoding ===")
        
        log_with_context(
            self.logger,
            logging.INFO,
            "Testing block decoding",
            raw_transaction_count=len(raw_block.transactions)
        )
        
        try:
            decoded_block = self.block_decoder.decode_block(raw_block)
            
            decoded_tx_count = len(decoded_block.transactions) if decoded_block.transactions else 0
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Block decoded successfully",
                block_number=decoded_block.block_number,
                decoded_transactions=decoded_tx_count,
                timestamp=decoded_block.timestamp
            )
            
            print(f"✅ Block decoded successfully")
            print(f"   Block number: {decoded_block.block_number}")
            print(f"   Decoded transactions: {decoded_tx_count}")
            
            if decoded_block.transactions:
                # Examine first transaction for details
                first_tx_hash = next(iter(decoded_block.transactions.keys()))
                first_tx = decoded_block.transactions[first_tx_hash]
                
                log_count = len(first_tx.logs)
                decoded_log_count = sum(1 for log in first_tx.logs.values() if hasattr(log, 'name'))
                
                log_with_context(
                    self.logger,
                    logging.DEBUG,
                    "Sample transaction analysis",
                    tx_hash=first_tx.tx_hash,
                    total_logs=log_count,
                    decoded_logs=decoded_log_count,
                    tx_success=first_tx.tx_success,
                    origin_from=first_tx.origin_from,
                    origin_to=first_tx.origin_to
                )
                
                print(f"   Sample transaction:")
                print(f"     Hash: {first_tx.tx_hash[:10]}...")
                print(f"     Success: {first_tx.tx_success}")
                print(f"     Logs: {log_count} (decoded: {decoded_log_count})")
            
            return decoded_block
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Block decoding failed",
                error=str(e)
            )
            print(f"❌ Block decoding failed: {e}")
            return None
    
    def _test_transformation(self, decoded_block: Block) -> Optional[Block]:
        """Test transformation pipeline"""
        print(f"\n=== Test 3: Transformation ===")
        
        if not decoded_block.transactions:
            print("⚠️  No transactions to transform")
            return decoded_block
        
        log_with_context(
            self.logger,
            logging.INFO,
            "Testing transformation pipeline",
            input_transaction_count=len(decoded_block.transactions)
        )
        
        try:
            transformed_transactions = {}
            total_processed = 0
            total_transfers = 0
            total_events = 0
            total_errors = 0
            
            for tx_hash, transaction in decoded_block.transactions.items():
                log_with_context(
                    self.logger,
                    logging.DEBUG,
                    "Processing transaction",
                    tx_hash=tx_hash
                )
                
                processed, transformed_tx = self.transform_manager.process_transaction(transaction)
                
                # Collect statistics
                transfer_count = len(transformed_tx.transfers) if transformed_tx.transfers else 0
                event_count = len(transformed_tx.events) if transformed_tx.events else 0
                error_count = len(transformed_tx.errors) if transformed_tx.errors else 0
                
                log_with_context(
                    self.logger,
                    logging.DEBUG,
                    "Transaction processing completed",
                    tx_hash=tx_hash,
                    processed=processed,
                    transfers=transfer_count,
                    events=event_count,
                    errors=error_count
                )
                
                print(f"   TX {tx_hash[:10]}... - Processed: {processed}")
                print(f"     Transfers: {transfer_count}, Events: {event_count}, Errors: {error_count}")
                
                # Log transfer details for debugging
                if transformed_tx.transfers:
                    for transfer_id, transfer in transformed_tx.transfers.items():
                        log_with_context(
                            self.logger,
                            logging.DEBUG,
                            "Transfer created",
                            tx_hash=tx_hash,
                            transfer_id=transfer_id,
                            transfer_type=type(transfer).__name__,
                            token=transfer.token,
                            amount=transfer.amount,
                            from_address=transfer.from_address,
                            to_address=transfer.to_address
                        )
                
                # Log event details for debugging
                if transformed_tx.events:
                    for event_id, event in transformed_tx.events.items():
                        log_with_context(
                            self.logger,
                            logging.DEBUG,
                            "Event created",
                            tx_hash=tx_hash,
                            event_id=event_id,
                            event_type=type(event).__name__
                        )
                
                # Log errors for debugging
                if transformed_tx.errors:
                    for error_id, error in transformed_tx.errors.items():
                        log_with_context(
                            self.logger,
                            logging.WARNING,
                            "Transformation error",
                            tx_hash=tx_hash,
                            error_id=error_id,
                            error_type=error.error_type,
                            error_message=error.message
                        )
                
                transformed_transactions[tx_hash] = transformed_tx
                
                if processed:
                    total_processed += 1
                total_transfers += transfer_count
                total_events += event_count
                total_errors += error_count
            
            # Update block with transformed transactions
            import msgspec
            transformed_block = msgspec.convert(decoded_block, type=Block)
            transformed_block.transactions = transformed_transactions
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Transformation completed",
                total_transactions=len(decoded_block.transactions),
                processed_transactions=total_processed,
                total_transfers=total_transfers,
                total_events=total_events,
                total_errors=total_errors
            )
            
            print(f"✅ Transformation completed")
            print(f"   Processed: {total_processed}/{len(decoded_block.transactions)} transactions")
            print(f"   Total transfers: {total_transfers}")
            print(f"   Total events: {total_events}")
            print(f"   Total errors: {total_errors}")
            
            return transformed_block
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Transformation failed",
                error=str(e)
            )
            print(f"❌ Transformation failed: {e}")
            return None
    
    def _test_storage(self, transformed_block: Block) -> bool:
        """Test storage of transformed block"""
        print(f"\n=== Test 4: Storage ===")
        
        try:
            # Check if block has errors
            error_count = sum(
                len(tx.errors) for tx in transformed_block.transactions.values()
                if tx.errors
            )
            
            has_errors = error_count > 0
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Testing storage",
                block_number=transformed_block.block_number,
                has_errors=has_errors,
                error_count=error_count
            )
            
            # Set processing metadata
            from datetime import datetime
            from indexer.types import ProcessingMetadata
            
            if not transformed_block.processing_metadata:
                transformed_block.processing_metadata = ProcessingMetadata()
            
            transformed_block.processing_metadata.completed_at = datetime.utcnow().isoformat()
            transformed_block.processing_metadata.error_count = error_count
            
            if has_errors:
                print(f"⚠️  Block has {error_count} errors - storing in processing/")
                transformed_block.processing_metadata.error_stage = "transform"
                
                success = self.storage_handler.save_processing_block(
                    transformed_block.block_number,
                    transformed_block
                )
                
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Block stored in processing",
                    block_number=transformed_block.block_number,
                    success=success,
                    error_count=error_count
                )
                
                print(f"{'✅' if success else '❌'} Block stored in processing/")
                
            else:
                print(f"✅ Block processed successfully - storing in complete/")
                
                success = self.storage_handler.save_complete_block(
                    transformed_block.block_number,
                    transformed_block
                )
                
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Block stored in complete",
                    block_number=transformed_block.block_number,
                    success=success
                )
                
                print(f"{'✅' if success else '❌'} Block stored in complete/")
            
            # Show processing summary
            summary = self.storage_handler.get_processing_summary()
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Storage summary retrieved",
                processing_count=summary['processing_count'],
                complete_count=summary['complete_count']
            )
            
            print(f"\n📊 Processing Summary:")
            print(f"   Processing: {summary['processing_count']} blocks")
            print(f"   Complete: {summary['complete_count']} blocks")
            if summary['latest_complete']:
                print(f"   Latest complete: {summary['latest_complete']}")
            
            return success
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Storage test failed",
                error=str(e)
            )
            print(f"❌ Storage failed: {e}")
            return False


def main():
    """Main testing function"""
    print("🚀 BLOCKCHAIN INDEXER PIPELINE TEST")
    print("=" * 50)
    
    # Get block number from command line
    if len(sys.argv) < 2:
        print("Usage: python testing/test_pipeline.py <block_number>")
        print("\nExample: python testing/test_pipeline.py 12345678")
        sys.exit(1)
    
    try:
        block_number = int(sys.argv[1])
    except ValueError:
        print("❌ Invalid block number. Please provide a valid integer.")
        sys.exit(1)
    
    print(f"📄 Testing block: {block_number}")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        # Initialize pipeline tester
        tester = PipelineTester()
        
        # Run complete pipeline test
        success = tester.test_complete_pipeline(block_number)
        
        print(f"\n{'🎉' if success else '💥'} Pipeline test {'COMPLETED' if success else 'FAILED'}")
        
        if success:
            print("✅ All pipeline stages passed!")
            print("\n🎯 Next steps:")
            print("   - Review logs/indexer.log for detailed analysis")
            print("   - Run log analyzer: python testing/diagnostics/log_analyzer.py logs/indexer.log")
        else:
            print("❌ Pipeline test failed - check logs for details")
            print("\n🔍 Debugging tips:")
            print("   - Check logs/indexer.log for error details")
            print("   - Run: python testing/diagnostics/quick_diagnostic.py")
            print("   - Try a different block number")
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()