# testing/test_pipeline.py
"""
Pipeline Testing Script for Blockchain Indexer

Uses the indexer's architecture and logging system for comprehensive testing.
Updated for signal-based transformation architecture.
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
from legacy_transformers.manager_simple import TransformManager
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
        self.transform_manager = self.testing_env.get_service(TransformManager)
        
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
            
            # Phase 3: Signal Generation
            signal_block = self._test_signal_generation(decoded_block)
            if signal_block is None:
                return False
            
            # Phase 4: Storage
            success = self._test_storage(signal_block)
            
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
    
    def _test_signal_generation(self, decoded_block: Block) -> Optional[Block]:
        """Test signal generation pipeline"""
        print(f"\n=== Test 3: Signal Generation ===")
        
        if not decoded_block.transactions:
            print("⚠️  No transactions to process")
            return decoded_block
        
        log_with_context(
            self.logger,
            logging.INFO,
            "Testing signal generation pipeline",
            input_transaction_count=len(decoded_block.transactions)
        )
        
        try:
            processed_transactions = {}
            total_with_signals = 0
            total_signals = 0
            total_errors = 0
            
            for tx_hash, transaction in decoded_block.transactions.items():
                log_with_context(
                    self.logger,
                    logging.DEBUG,
                    "Processing transaction for signals",
                    tx_hash=tx_hash
                )
                
                signals_generated, processed_tx = self.transform_manager.process_transaction(transaction)
                
                # Collect statistics
                signal_count = len(processed_tx.signals) if processed_tx.signals else 0
                error_count = len(processed_tx.errors) if processed_tx.errors else 0
                
                log_with_context(
                    self.logger,
                    logging.DEBUG,
                    "Transaction signal processing completed",
                    tx_hash=tx_hash,
                    signals_generated=signals_generated,
                    signal_count=signal_count,
                    errors=error_count
                )
                
                print(f"   TX {tx_hash[:10]}... - Signals: {signal_count}, Errors: {error_count}")
                
                # Log signal details for debugging
                if processed_tx.signals:
                    for signal_idx, signal in processed_tx.signals.items():
                        log_with_context(
                            self.logger,
                            logging.DEBUG,
                            "Signal generated",
                            tx_hash=tx_hash,
                            signal_index=signal_idx,
                            signal_type=type(signal).__name__,
                            log_index=signal.log_index
                        )
                
                # Log errors for debugging
                if processed_tx.errors:
                    for error_id, error in processed_tx.errors.items():
                        log_with_context(
                            self.logger,
                            logging.WARNING,
                            "Signal generation error",
                            tx_hash=tx_hash,
                            error_id=error_id,
                            error_type=error.error_type,
                            error_message=error.message
                        )
                
                processed_transactions[tx_hash] = processed_tx
                
                if signals_generated:
                    total_with_signals += 1
                total_signals += signal_count
                total_errors += error_count
            
            # Update block with processed transactions
            import msgspec
            signal_block = msgspec.convert(decoded_block, type=Block)
            signal_block.transactions = processed_transactions
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Signal generation completed",
                total_transactions=len(decoded_block.transactions),
                transactions_with_signals=total_with_signals,
                total_signals=total_signals,
                total_errors=total_errors
            )
            
            print(f"✅ Signal generation completed")
            print(f"   Transactions with signals: {total_with_signals}/{len(decoded_block.transactions)}")
            print(f"   Total signals generated: {total_signals}")
            print(f"   Total errors: {total_errors}")
            
            return signal_block
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Signal generation failed",
                error=str(e)
            )
            print(f"❌ Signal generation failed: {e}")
            return None
    
    def _test_storage(self, signal_block: Block) -> bool:
        """Test storage of processed block"""
        print(f"\n=== Test 4: Storage ===")
        
        try:
            # Check if block has errors
            error_count = sum(
                len(tx.errors) for tx in signal_block.transactions.values()
                if tx.errors
            )
            
            has_errors = error_count > 0
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Testing storage",
                block_number=signal_block.block_number,
                has_errors=has_errors,
                error_count=error_count
            )
            
            # Set processing metadata
            from datetime import datetime
            from indexer.types import ProcessingMetadata
            
            if not signal_block.processing_metadata:
                signal_block.processing_metadata = ProcessingMetadata()
            
            signal_block.processing_metadata.completed_at = datetime.utcnow().isoformat()
            signal_block.processing_metadata.error_count = error_count
            
            if has_errors:
                print(f"⚠️  Block has {error_count} errors - storing in processing/")
                signal_block.processing_metadata.error_stage = "transform"
                signal_block.indexing_status = "error"
                
                success = self.storage_handler.save_processing_block(
                    signal_block.block_number,
                    signal_block
                )
                
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Block stored in processing",
                    block_number=signal_block.block_number,
                    success=success,
                    error_count=error_count
                )
                
                print(f"{'✅' if success else '❌'} Block stored in processing/")
                
            else:
                print(f"✅ Block processed successfully - storing in complete/")
                signal_block.indexing_status = "complete"
                
                success = self.storage_handler.save_complete_block(
                    signal_block.block_number,
                    signal_block
                )
                
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Block stored in complete",
                    block_number=signal_block.block_number,
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
            print("   - Review logs/indexer.log for detailed signal analysis")
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