# testing/test_pipeline.py
"""
Pipeline Testing Script for Blockchain Indexer
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
    Pipeline tester with clean, focused output
    """
    
    def __init__(self, config_path: str = None):
        # Set logging to ERROR level to reduce console noise
        self.testing_env = get_testing_environment(config_path, log_level="ERROR")
        
        # Override specific loggers we want to see
        critical_loggers = [
            "indexer.testing.pipeline.tester",
            "indexer.transform.manager.TransformManager"
        ]
        
        for logger_name in critical_loggers:
            logger = self.testing_env.get_logger(logger_name)
            logger.setLevel(logging.INFO)
        
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
        """Test the complete pipeline with clean output"""
        
        print(f"🔄 Testing block {block_number}...")
        
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
            print(f"❌ Pipeline failed: {e}")
            return False
    
    def _test_block_retrieval(self, block_number: int) -> Optional[EvmFilteredBlock]:
        """Test block retrieval from GCS"""
        try:
            raw_block = self.storage_handler.get_rpc_block(block_number)
            
            if raw_block is None:
                print(f"❌ Block {block_number} not found in GCS")
                return None
            
            print(f"✅ Block retrieved: {len(raw_block.transactions)} transactions")
            return raw_block
            
        except Exception as e:
            print(f"❌ Block retrieval failed: {e}")
            return None
    
    def _test_block_decoding(self, raw_block: EvmFilteredBlock) -> Optional[Block]:
        """Test block decoding"""
        try:
            decoded_block = self.block_decoder.decode_block(raw_block)
            
            decoded_tx_count = len(decoded_block.transactions) if decoded_block.transactions else 0
            
            if decoded_block.transactions:
                first_tx = next(iter(decoded_block.transactions.values()))
                log_count = len(first_tx.logs)
                decoded_log_count = sum(1 for log in first_tx.logs.values() if hasattr(log, 'name'))
                print(f"✅ Block decoded: {decoded_tx_count} transactions, {decoded_log_count}/{log_count} logs decoded")
            else:
                print(f"⚠️  Block decoded but no transactions found")
            
            return decoded_block
            
        except Exception as e:
            print(f"❌ Block decoding failed: {e}")
            return None
    
    def _test_signal_generation(self, decoded_block: Block) -> Optional[Block]:
        """Test signal generation and transformation pipeline"""
        
        if not decoded_block.transactions:
            print("⚠️  No transactions to process")
            return decoded_block
        
        try:
            processed_transactions = {}
            stats = {
                'total_processed': 0,
                'with_signals': 0,
                'with_events': 0,
                'with_errors': 0,
                'total_signals': 0,
                'total_events': 0,
                'total_errors': 0,
                'signal_types': {},
                'error_types': {},
                'failed_transformers': set()
            }
            
            for tx_hash, transaction in decoded_block.transactions.items():
                try:
                    # Process transaction through transform manager
                    signals_generated, processed_tx = self.transform_manager.process_transaction(transaction)
                    
                    # Collect statistics
                    signal_count = len(processed_tx.signals) if processed_tx.signals else 0
                    event_count = len(processed_tx.events) if processed_tx.events else 0
                    error_count = len(processed_tx.errors) if processed_tx.errors else 0
                    
                    # Track signal types
                    if processed_tx.signals:
                        for signal in processed_tx.signals.values():
                            signal_type = type(signal).__name__
                            stats['signal_types'][signal_type] = stats['signal_types'].get(signal_type, 0) + 1
                    
                    # Track error types and transformers
                    if processed_tx.errors:
                        for error in processed_tx.errors.values():
                            error_type = error.error_type
                            stats['error_types'][error_type] = stats['error_types'].get(error_type, 0) + 1
                            
                            # Track which transformer failed
                            if error.context and 'transformer_name' in error.context:
                                stats['failed_transformers'].add(error.context['transformer_name'])
                    
                    processed_transactions[tx_hash] = processed_tx
                    
                    # Update statistics
                    stats['total_processed'] += 1
                    if signal_count > 0:
                        stats['with_signals'] += 1
                        stats['total_signals'] += signal_count
                    if event_count > 0:
                        stats['with_events'] += 1
                        stats['total_events'] += event_count
                    if error_count > 0:
                        stats['with_errors'] += 1
                        stats['total_errors'] += error_count
                
                except Exception as e:
                    print(f"❌ Transaction processing exception: {e}")
                    processed_transactions[tx_hash] = transaction
                    stats['total_processed'] += 1
                    stats['with_errors'] += 1
            
            # Update block with processed transactions
            import msgspec
            transformed_block = msgspec.convert(decoded_block, type=Block)
            transformed_block.transactions = processed_transactions
            
            # Print clean summary
            self._print_processing_summary(stats)
            
            return transformed_block
            
        except Exception as e:
            print(f"❌ Signal generation failed: {e}")
            return None
    
    def _print_processing_summary(self, stats):
        """Print clean processing summary"""
        success_icon = "✅" if stats['total_errors'] == 0 else "⚠️"
        
        print(f"{success_icon} Processing: {stats['total_signals']} signals, {stats['total_events']} events, {stats['total_errors']} errors")
        
        if stats['signal_types']:
            signal_summary = ", ".join(f"{k}({v})" for k, v in stats['signal_types'].items())
            print(f"   📊 Signals: {signal_summary}")
        
        if stats['total_errors'] > 0:
            error_summary = ", ".join(f"{k}({v})" for k, v in stats['error_types'].items())
            print(f"   🚨 Errors: {error_summary}")
            
            if stats['failed_transformers']:
                transformers = ", ".join(stats['failed_transformers'])
                print(f"   🔧 Failed transformers: {transformers}")
    
    def _test_storage(self, transformed_block: Block) -> bool:
        """Test storage of processed block"""
        try:
            # Check if block has errors
            error_count = sum(
                len(tx.errors) for tx in transformed_block.transactions.values()
                if tx.errors
            )
            
            has_errors = error_count > 0
            
            # Set processing metadata
            from datetime import datetime, timezone
            from indexer.types import ProcessingMetadata
            
            if not transformed_block.processing_metadata:
                transformed_block.processing_metadata = ProcessingMetadata()
            
            transformed_block.processing_metadata.completed_at = datetime.now(timezone.utc).isoformat()
            transformed_block.processing_metadata.error_count = error_count
            
            if has_errors:
                transformed_block.processing_metadata.error_stage = "transform"
                transformed_block.indexing_status = "error"
                success = self.storage_handler.save_processing_block(
                    transformed_block.block_number,
                    transformed_block
                )
                print(f"✅ Block stored in processing/ (with {error_count} errors)")
            else:
                transformed_block.indexing_status = "complete"
                success = self.storage_handler.save_complete_block(
                    transformed_block.block_number,
                    transformed_block
                )
                print(f"✅ Block stored in complete/")
            
            return success
            
        except Exception as e:
            print(f"❌ Storage failed: {e}")
            return False


def print_debug_commands(block_number: int, tx_hash: str = None):
    """Print helpful debug commands"""
    print(f"\n🔍 Debug Commands:")
    print(f"   Detailed transaction analysis:")
    
    if tx_hash:
        print(f"   python testing/scripts/debug_session.py analyze {tx_hash} {block_number}")
    else:
        print(f"   python testing/scripts/debug_session.py block {block_number}")
    
    print(f"   Log analysis:")
    print(f"   python testing/diagnostics/log_analyzer.py logs/indexer.log")
    print(f"   Transformer report:")
    print(f"   python testing/scripts/debug_session.py transformers")


def main():
    """Main testing function with clean output"""
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
    
    try:
        # Initialize pipeline tester
        tester = PipelineTester()
        
        # Run complete pipeline test
        success = tester.test_complete_pipeline(block_number)
        
        # Get first transaction hash for debug commands
        try:
            # Try to get the transaction hash from logs or storage
            raw_block = tester.storage_handler.get_rpc_block(block_number)
            first_tx_hash = None
            if raw_block and raw_block.transactions:
                first_tx = raw_block.transactions[0]
                if hasattr(first_tx, 'hash'):
                    first_tx_hash = first_tx.hash
                    if hasattr(first_tx_hash, 'hex'):
                        first_tx_hash = first_tx_hash.hex()
        except:
            first_tx_hash = None
        
        # Print results
        print(f"\n{'🎉' if success else '💥'} Pipeline test {'COMPLETED' if success else 'FAILED'}")
        
        if success:
            print("✅ All pipeline stages completed successfully!")
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