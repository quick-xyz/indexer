# testing/scripts/debug_session.py
"""
Interactive Debug Session for Blockchain Indexer

Provides an interactive environment for debugging transformation issues
using the indexer's architecture and logging system.
"""

import sys
from pathlib import Path
from typing import Optional

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.core.logging_config import log_with_context
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformationManager
from indexer.transform.registry import TransformerRegistry


class DebugSession:
    """Interactive debugging session using indexer architecture"""
    
    def __init__(self, config_path: str = None):
        self.testing_env = get_testing_environment(config_path, log_level="DEBUG")
        self.logger = self.testing_env.get_logger("debug.session")
        
        # Get services
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformationManager)
        self.transformer_registry = self.testing_env.get_service(TransformerRegistry)
        
        print("🔧 BLOCKCHAIN INDEXER DEBUG SESSION")
        print("=" * 50)
        print("Services loaded and ready for debugging")
        print()
    
    def quick_block_check(self, block_number: int):
        """Quick check of a block's transformation potential"""
        print(f"🔍 Quick check for block {block_number}")
        print("-" * 30)
        
        # Get raw block
        raw_block = self.storage_handler.get_rpc_block(block_number)
        if not raw_block:
            print(f"❌ Block {block_number} not found")
            return
        
        print(f"✅ Block found: {len(raw_block.transactions)} transactions")
        
        # Decode block
        decoded_block = self.block_decoder.decode_block(raw_block)
        decoded_tx_count = len(decoded_block.transactions) if decoded_block.transactions else 0
        print(f"✅ Decoded: {decoded_tx_count} transactions")
        
        if not decoded_block.transactions:
            print("⚠️  No transactions to analyze")
            return
        
        # Analyze first transaction
        first_tx_hash = next(iter(decoded_block.transactions.keys()))
        first_tx = decoded_block.transactions[first_tx_hash]
        
        total_logs = len(first_tx.logs)
        decoded_logs = sum(1 for log in first_tx.logs.values() if hasattr(log, 'name'))
        
        print(f"📄 First TX {first_tx_hash[:10]}...:")
        print(f"   Logs: {total_logs} (decoded: {decoded_logs})")
        print(f"   Success: {first_tx.tx_success}")
        
        # Show decoded log details
        if decoded_logs > 0:
            print("   Decoded events:")
            for log_idx, log in first_tx.logs.items():
                if hasattr(log, 'name'):
                    print(f"     {log_idx}: {log.name} from {log.contract[:10]}...")
        
        return decoded_block
    
    def analyze_transaction(self, tx_hash: str, block_number: Optional[int] = None):
        """Deep analysis of a specific transaction"""
        print(f"🔬 Analyzing transaction {tx_hash}")
        print("-" * 40)
        
        # If block number provided, get the transaction from that block
        if block_number:
            raw_block = self.storage_handler.get_rpc_block(block_number)
            if raw_block:
                decoded_block = self.block_decoder.decode_block(raw_block)
                if decoded_block.transactions and tx_hash in decoded_block.transactions:
                    transaction = decoded_block.transactions[tx_hash]
                else:
                    print(f"❌ Transaction {tx_hash} not found in block {block_number}")
                    return
            else:
                print(f"❌ Block {block_number} not found")
                return
        else:
            print("❌ Block number required for transaction analysis")
            return
        
        print(f"📄 Transaction Details:")
        print(f"   Hash: {transaction.tx_hash}")
        print(f"   Success: {transaction.tx_success}")
        print(f"   From: {transaction.origin_from}")
        print(f"   To: {transaction.origin_to}")
        print(f"   Logs: {len(transaction.logs)}")
        
        # Show all logs
        decoded_logs = {}
        for log_idx, log in transaction.logs.items():
            if hasattr(log, 'name'):
                decoded_logs[log_idx] = log
                print(f"   📋 Log {log_idx}: {log.name} from {log.contract}")
                
                # Check if transformer exists for this contract
                transformer = self.transformer_registry.get_transformer(log.contract)
                if transformer:
                    transformer_name = type(transformer).__name__
                    print(f"       🔧 Transformer: {transformer_name}")
                    
                    # Check event priorities
                    transfer_priority = self.transformer_registry.get_transfer_priority(log.contract, log.name)
                    log_priority = self.transformer_registry.get_log_priority(log.contract, log.name)
                    
                    if transfer_priority is not None:
                        print(f"       📤 Transfer priority: {transfer_priority}")
                    if log_priority is not None:
                        print(f"       📋 Log priority: {log_priority}")
                else:
                    print(f"       ❌ No transformer found")
        
        # Run transformation and analyze results
        print(f"\n🔄 Running transformation...")
        processed, transformed_tx = self.transform_manager.process_transaction(transaction)
        
        transfer_count = len(transformed_tx.transfers) if transformed_tx.transfers else 0
        event_count = len(transformed_tx.events) if transformed_tx.events else 0
        error_count = len(transformed_tx.errors) if transformed_tx.errors else 0
        
        print(f"✅ Transformation complete:")
        print(f"   Processed: {processed}")
        print(f"   Transfers: {transfer_count}")
        print(f"   Events: {event_count}")
        print(f"   Errors: {error_count}")
        
        # Show detailed results
        if transformed_tx.transfers:
            print(f"\n📤 Transfers created:")
            for transfer_id, transfer in transformed_tx.transfers.items():
                transfer_type = type(transfer).__name__
                print(f"   {transfer_id}: {transfer_type}")
                print(f"     Token: {transfer.token}")
                print(f"     Amount: {transfer.amount}")
                print(f"     {transfer.from_address} → {transfer.to_address}")
        
        if transformed_tx.events:
            print(f"\n🎯 Events created:")
            for event_id, event in transformed_tx.events.items():
                event_type = type(event).__name__
                print(f"   {event_id}: {event_type}")
        
        if transformed_tx.errors:
            print(f"\n❌ Errors:")
            for error_id, error in transformed_tx.errors.items():
                print(f"   {error_id}: {error.error_type}")
                print(f"     {error.message}")
    
    def test_transformer(self, contract_address: str, block_number: int):
        """Test a specific transformer with a block"""
        print(f"🔧 Testing transformer for {contract_address}")
        print("-" * 50)
        
        # Get transformer
        transformer = self.transformer_registry.get_transformer(contract_address)
        if not transformer:
            print(f"❌ No transformer found for {contract_address}")
            return
        
        transformer_name = type(transformer).__name__
        print(f"✅ Found transformer: {transformer_name}")
        
        # Get transformer configuration
        all_transformers = self.transformer_registry.get_all_contracts()
        transformer_config = all_transformers.get(contract_address.lower())
        
        if transformer_config:
            print(f"📋 Configuration:")
            print(f"   Transfer events: {transformer_config.transfer_priorities}")
            print(f"   Log events: {transformer_config.log_priorities}")
            print(f"   Active: {transformer_config.active}")
        
        # Get block and find relevant transactions
        raw_block = self.storage_handler.get_rpc_block(block_number)
        if not raw_block:
            print(f"❌ Block {block_number} not found")
            return
        
        decoded_block = self.block_decoder.decode_block(raw_block)
        if not decoded_block.transactions:
            print(f"⚠️  No transactions in block")
            return
        
        # Find transactions with logs from this contract
        relevant_txs = []
        for tx_hash, transaction in decoded_block.transactions.items():
            for log_idx, log in transaction.logs.items():
                if hasattr(log, 'contract') and log.contract.lower() == contract_address.lower():
                    relevant_txs.append((tx_hash, transaction))
                    break
        
        print(f"📄 Found {len(relevant_txs)} transactions with {contract_address} activity")
        
        # Test transformer on each relevant transaction
        for tx_hash, transaction in relevant_txs[:3]:  # Test first 3
            print(f"\n   Testing TX {tx_hash[:10]}...")
            
            try:
                processed, transformed_tx = self.transform_manager.process_transaction(transaction)
                
                transfer_count = len(transformed_tx.transfers) if transformed_tx.transfers else 0
                event_count = len(transformed_tx.events) if transformed_tx.events else 0
                error_count = len(transformed_tx.errors) if transformed_tx.errors else 0
                
                print(f"     Result: Processed={processed}, Transfers={transfer_count}, Events={event_count}, Errors={error_count}")
                
                if transformed_tx.errors:
                    for error_id, error in transformed_tx.errors.items():
                        print(f"     ❌ {error.error_type}: {error.message}")
                        
            except Exception as e:
                print(f"     💥 Exception: {e}")
    
    def interactive_menu(self):
        """Interactive debugging menu"""
        while True:
            print(f"\n🔧 DEBUG MENU")
            print("-" * 20)
            print("1. Quick block check")
            print("2. Analyze specific transaction")
            print("3. Test specific transformer")
            print("4. List available transformers")
            print("5. Check storage summary")
            print("0. Exit")
            
            try:
                choice = input("\nSelect option: ").strip()
                
                if choice == "0":
                    print("👋 Goodbye!")
                    break
                elif choice == "1":
                    block_num = int(input("Enter block number: "))
                    self.quick_block_check(block_num)
                elif choice == "2":
                    tx_hash = input("Enter transaction hash: ").strip()
                    block_num = int(input("Enter block number: "))
                    self.analyze_transaction(tx_hash, block_num)
                elif choice == "3":
                    contract = input("Enter contract address: ").strip()
                    block_num = int(input("Enter block number: "))
                    self.test_transformer(contract, block_num)
                elif choice == "4":
                    self.list_transformers()
                elif choice == "5":
                    self.check_storage_summary()
                else:
                    print("❌ Invalid option")
                    
            except ValueError:
                print("❌ Invalid input")
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def list_transformers(self):
        """List all available transformers"""
        print(f"\n🔧 AVAILABLE TRANSFORMERS")
        print("-" * 30)
        
        all_transformers = self.transformer_registry.get_all_contracts()
        
        for address, transformer_config in all_transformers.items():
            transformer_name = type(transformer_config.instance).__name__
            active_status = "✅" if transformer_config.active else "❌"
            
            print(f"{active_status} {transformer_name}")
            print(f"   Contract: {address}")
            print(f"   Transfer events: {list(transformer_config.transfer_priorities.keys())}")
            print(f"   Log events: {list(transformer_config.log_priorities.keys())}")
            print()
    
    def check_storage_summary(self):
        """Check storage processing summary"""
        print(f"\n📊 STORAGE SUMMARY")
        print("-" * 20)
        
        summary = self.storage_handler.get_processing_summary()
        
        print(f"Processing blocks: {summary['processing_count']}")
        print(f"Complete blocks: {summary['complete_count']}")
        
        if summary['latest_complete']:
            print(f"Latest complete: {summary['latest_complete']}")
        if summary['oldest_processing']:
            print(f"Oldest processing: {summary['oldest_processing']}")
        
        if summary.get('processing_blocks'):
            print(f"Sample processing blocks: {summary['processing_blocks'][:5]}")


def main():
    """Main debug session"""
    print("Starting debug session...")
    
    try:
        debug_session = DebugSession()
        
        if len(sys.argv) > 1:
            # Command line mode
            command = sys.argv[1]
            
            if command == "check" and len(sys.argv) > 2:
                block_number = int(sys.argv[2])
                debug_session.quick_block_check(block_number)
            elif command == "analyze" and len(sys.argv) > 3:
                tx_hash = sys.argv[2]
                block_number = int(sys.argv[3])
                debug_session.analyze_transaction(tx_hash, block_number)
            elif command == "test" and len(sys.argv) > 3:
                contract = sys.argv[2]
                block_number = int(sys.argv[3])
                debug_session.test_transformer(contract, block_number)
            else:
                print("Usage:")
                print("  python testing/scripts/debug_session.py check <block_number>")
                print("  python testing/scripts/debug_session.py analyze <tx_hash> <block_number>")
                print("  python testing/scripts/debug_session.py test <contract_address> <block_number>")
                print("  python testing/scripts/debug_session.py  # Interactive mode")
        else:
            # Interactive mode
            debug_session.interactive_menu()
            
    except Exception as e:
        print(f"❌ Debug session failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()