# testing/test_pipeline.py
"""
Pipeline Testing Script for Blockchain Indexer
Updated to use create_indexer() entry point and proper DI system
Clean, focused output for rapid development iteration.
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
from indexer.transform.manager import TransformManager
from indexer.types import EvmFilteredBlock, Block


class PipelineTester:
    """
    Pipeline tester using proper DI system with clean, focused output
    """
    
    def __init__(self, model_name: str = None):
        # Initialize testing environment with database-driven config
        self.testing_env = get_testing_environment(model_name=model_name, log_level="ERROR")
        
        # Override specific loggers we want to see
        critical_loggers = [
            "indexer.testing.pipeline.tester",
            "indexer.transform.manager.TransformManager"
        ]
        
        for logger_name in critical_loggers:
            logger = self.testing_env.get_logger(logger_name)
            logger.setLevel(logging.INFO)
        
        self.logger = self.testing_env.get_logger("pipeline.tester")
        
        # Get services from DI container using proper entry point
        self.rpc_client = self.testing_env.get_service(QuickNodeRpcClient)
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformManager)
        
        log_with_context(
            self.logger,
            logging.INFO,
            "Pipeline tester initialized using DI container",
            model_name=self.testing_env.config.model_name,
            indexer_version=self.testing_env.config.model_version,
            contract_count=len(self.testing_env.config.contracts) if self.testing_env.config.contracts else 0,
            sources_count=len(self.testing_env.config.sources) if self.testing_env.config.sources else 0
        )
    
    def test_complete_pipeline(self, block_number: int) -> bool:
        """Test the complete pipeline with clean output"""
        
        print(f"🔄 Testing block {block_number} with model: {self.testing_env.config.model_name}")
        
        try:
            # Phase 1: Block Retrieval
            raw_block = self._test_block_retrieval(block_number)
            if raw_block is None:
                return False
            
            # Phase 2: Block Decoding  
            decoded_block = self._test_block_decoding(raw_block)
            if decoded_block is None:
                return False
            
            # Phase 3: Signal Generation and Transform
            transformed_block = self._test_signal_generation(decoded_block)
            if transformed_block is None:
                return False
            
            # Phase 4: Storage
            success = self._test_storage(transformed_block)
            
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
            print(f"❌ Pipeline failed: {e}")
            return False
    
    def _test_block_retrieval(self, block_number: int) -> Optional[EvmFilteredBlock]:
        """Test block retrieval from GCS using DI-configured storage handler"""
        try:
            log_with_context(
                self.logger,
                logging.DEBUG,
                "Testing block retrieval",
                block_number=block_number
            )
            
            # Get primary source for RPC block retrieval
            config = self.testing_env.get_config()
            primary_source = config.get_primary_source()
            
            if not primary_source:
                print(f"❌ No primary source configured for model: {config.model_name}")
                log_with_context(
                    self.logger,
                    logging.ERROR,
                    "No primary source available",
                    model_name=config.model_name,
                    sources_count=len(config.sources) if config.sources else 0
                )
                return None
            
            log_with_context(
                self.logger,
                logging.DEBUG,
                "Using primary source for block retrieval",
                source_name=primary_source.name,
                source_path=primary_source.path
            )
            
            raw_block = self.storage_handler.get_rpc_block(block_number, source=primary_source)
            
            if raw_block is None:
                print(f"❌ Block {block_number} not found in GCS")
                log_with_context(
                    self.logger,
                    logging.WARNING,
                    "Block not found in storage",
                    block_number=block_number,
                    source_name=primary_source.name
                )
                return None
            
            tx_count = len(raw_block.transactions) if raw_block.transactions else 0
            print(f"✅ Block retrieved: {tx_count} transactions")
            print(f"   Source: {primary_source.name}")
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Block retrieval successful",
                block_number=block_number,
                transaction_count=tx_count,
                source_name=primary_source.name
            )
            
            return raw_block
            
        except Exception as e:
            print(f"❌ Block retrieval failed: {e}")
            log_with_context(
                self.logger,
                logging.ERROR,
                "Block retrieval failed",
                block_number=block_number,
                error=str(e)
            )
            return None
    
    def _test_block_decoding(self, raw_block: EvmFilteredBlock) -> Optional[Block]:
        """Test block decoding using DI-configured decoder"""
        try:
            log_with_context(
                self.logger,
                logging.DEBUG,
                "Testing block decoding"
            )
            
            decoded_block = self.block_decoder.decode_block(raw_block)
            
            decoded_tx_count = len(decoded_block.transactions) if decoded_block.transactions else 0
            
            print(f"✅ Block decoded: {decoded_tx_count} transactions")
            
            if decoded_block.transactions:
                first_tx = next(iter(decoded_block.transactions.values()))
                log_count = len(first_tx.logs) if first_tx.logs else 0
                print(f"   First transaction: {log_count} logs")
                
                log_with_context(
                    self.logger,
                    logging.INFO,
                    "Block decoding successful",
                    decoded_transactions=decoded_tx_count,
                    first_tx_logs=log_count
                )
            
            return decoded_block
            
        except Exception as e:
            print(f"❌ Block decoding failed: {e}")
            log_with_context(
                self.logger,
                logging.ERROR,
                "Block decoding failed",
                error=str(e)
            )
            return None
    
    def _test_signal_generation(self, decoded_block: Block) -> Optional[Block]:
        """Test signal generation using DI-configured transform manager"""
        try:
            log_with_context(
                self.logger,
                logging.DEBUG,
                "Testing signal generation and transformation"
            )
            
            # Process each transaction individually (TransformManager works per transaction)
            total_signals = 0
            total_events = 0
            total_errors = 0
            processed_transactions = {}
            
            if decoded_block.transactions:
                for tx_hash, transaction in decoded_block.transactions.items():
                    try:
                        # Process transaction through transform manager
                        success, processed_tx = self.transform_manager.process_transaction(transaction)
                        processed_transactions[tx_hash] = processed_tx
                        
                        # Count results
                        if hasattr(processed_tx, 'signals') and processed_tx.signals:
                            total_signals += len(processed_tx.signals)
                        if hasattr(processed_tx, 'events') and processed_tx.events:
                            total_events += len(processed_tx.events)
                        if hasattr(processed_tx, 'errors') and processed_tx.errors:
                            total_errors += len(processed_tx.errors)
                            
                    except Exception as e:
                        log_with_context(
                            self.logger,
                            logging.ERROR,
                            "Transaction processing failed",
                            tx_hash=tx_hash,
                            error=str(e)
                        )
                        total_errors += 1
                        processed_transactions[tx_hash] = transaction  # Keep original
            
            # Update the block with processed transactions
            decoded_block.transactions = processed_transactions
            
            print(f"✅ Transform complete: {total_signals} signals → {total_events} events")
            if total_errors > 0:
                print(f"   ⚠️  {total_errors} processing errors")
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Signal generation completed",
                total_signals=total_signals,
                total_events=total_events,
                total_errors=total_errors,
                transactions_processed=len(processed_transactions)
            )
            
            return decoded_block
            
        except Exception as e:
            print(f"❌ Signal generation failed: {e}")
            log_with_context(
                self.logger,
                logging.ERROR,
                "Signal generation failed",
                error=str(e)
            )
            return None
    
    def _test_storage(self, transformed_block: Block) -> bool:
        """Test storage of transformed block using DI-configured storage handler"""
        try:
            log_with_context(
                self.logger,
                logging.DEBUG,
                "Testing block storage"
            )
            
            # Check if there were any errors during processing
            has_errors = False
            error_count = 0
            
            if transformed_block.transactions:
                for tx in transformed_block.transactions.values():
                    if hasattr(tx, 'errors') and tx.errors:
                        has_errors = True
                        error_count += len(tx.errors)
            
            # Use appropriate storage method based on processing status
            if has_errors:
                # Store in processing folder if there were errors
                transformed_block.indexing_status = "error"
                success = self.storage_handler.save_processing_block(
                    transformed_block.block_number,
                    transformed_block
                )
                print(f"✅ Block stored in processing/ (with {error_count} errors)")
            else:
                # Store in complete folder if no errors
                transformed_block.indexing_status = "complete"
                success = self.storage_handler.save_complete_block(
                    transformed_block.block_number,
                    transformed_block
                )
                print(f"✅ Block stored in complete/")
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Block storage completed",
                block_number=transformed_block.block_number,
                storage_type="processing" if has_errors else "complete",
                error_count=error_count,
                success=success
            )
            
            return success
            
        except Exception as e:
            print(f"❌ Block storage failed: {e}")
            log_with_context(
                self.logger,
                logging.ERROR,
                "Block storage failed with exception",
                error=str(e)
            )
            return False


def print_debug_commands(block_number: int, first_tx_hash: str = None):
    """Print debugging commands for further investigation"""
    print(f"\n🔍 Debug Commands:")
    print(f"   python testing/diagnostics/quick_diagnostic.py --model $INDEXER_MODEL_NAME")
    
    if first_tx_hash:
        print(f"   python testing/scripts/debug_session.py analyze {first_tx_hash} {block_number}")
    
    print(f"   python testing/scripts/debug_session.py analyze <tx_hash> {block_number}")


def main():
    """Main entry point for pipeline testing"""
    if len(sys.argv) < 2:
        print("Usage: python testing/test_pipeline.py <block_number> [model_name]")
        print("\nExample:")
        print("  python testing/test_pipeline.py 12345678")
        print("  python testing/test_pipeline.py 12345678 blub_test")
        sys.exit(1)
    
    try:
        block_number = int(sys.argv[1])
    except ValueError:
        print("Error: Block number must be a valid integer")
        sys.exit(1)
    
    # Optional model name parameter
    model_name = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        # Initialize pipeline tester with proper DI
        print(f"🚀 Initializing pipeline tester...")
        tester = PipelineTester(model_name=model_name)
        
        # Run complete pipeline test
        success = tester.test_complete_pipeline(block_number)
        
        # Get first transaction hash for debug commands
        first_tx_hash = None
        try:
            config = tester.testing_env.get_config()
            primary_source = config.get_primary_source()
            if primary_source:
                raw_block = tester.storage_handler.get_rpc_block(block_number, source=primary_source)
                if raw_block and raw_block.transactions:
                    first_tx = raw_block.transactions[0]
                    if hasattr(first_tx, 'hash'):
                        first_tx_hash = first_tx.hash
                        if hasattr(first_tx_hash, 'hex'):
                            first_tx_hash = first_tx_hash.hex()
        except:
            pass
        
        # Print results
        print(f"\n{'🎉' if success else '💥'} Pipeline test {'COMPLETED' if success else 'FAILED'}")
        
        if success:
            print("✅ All pipeline stages completed successfully!")
            print(f"   Model: {tester.testing_env.config.model_name}")
            print(f"   Version: {tester.testing_env.config.model_version}")
        else:
            print("❌ Pipeline test encountered issues")
        
        # Print debug commands
        print_debug_commands(block_number, first_tx_hash)
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Test failed with exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()