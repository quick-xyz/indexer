# test_pipeline.py
"""
Simple pipeline testing script for blockchain indexer
Run from project root directory
"""

import json
import os
from pathlib import Path
from typing import Dict, Any
import msgspec
from datetime import datetime

# Ensure we can import the indexer
import sys
sys.path.append(str(Path(__file__).parent))

from indexer import create_indexer
from indexer.types import EvmFilteredBlock, Block, ProcessingMetadata, BlockStatus, TransactionStatus
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from legacy_transformers.manager_simple import TransformationManager


def test_config_loading():
    """Test 1: Verify configuration loading and dependency injection works"""
    print("=== Test 1: Configuration Loading ===")
    
    try:
        # Load config from your existing config.json
        config_path = Path("config/config.json")
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found at {config_path}")
        
        print(f"Loading config from: {config_path}")
        
        # Create indexer container
        container = create_indexer(config_path=str(config_path))
        print("✅ Indexer container created successfully")
        
        # Test dependency injection
        rpc_client = container.get(QuickNodeRpcClient)
        storage_handler = container.get(GCSHandler)
        block_decoder = container.get(BlockDecoder)
        transform_manager = container.get(TransformationManager)
        
        print("✅ All services resolved from container")
        print(f"   - RPC Client: {type(rpc_client).__name__}")
        print(f"   - Storage Handler: {type(storage_handler).__name__}")
        print(f"   - Block Decoder: {type(block_decoder).__name__}")
        print(f"   - Transform Manager: {type(transform_manager).__name__}")
        
        return container
        
    except Exception as e:
        print(f"❌ Config loading failed: {e}")
        raise


def test_gcs_connection(container):
    """Test 2: Verify GCS connection"""
    print("\n=== Test 2: GCS Connection ===")
    
    try:
        storage_handler = container.get(GCSHandler)
        
        # Test basic connection
        print("Testing GCS connection...")
        blobs = storage_handler.list_blobs(prefix=storage_handler.storage_config.rpc_prefix)
        print(f"✅ Connected to GCS bucket: {storage_handler.bucket_name}")
        print(f"   Found {len(blobs)} RPC blobs with prefix '{storage_handler.storage_config.rpc_prefix}'")    
        # List some available blocks for reference
        rpc_blocks = []
        for blob in blobs[:10]:  # Just check first 10
            if blob.name.endswith('.json'):
                # Extract block number from filename if possible
                try:
                    # Assuming format like: quicknode/avalanche-mainnet_block_with_receipts_000012345-000012345.json
                    parts = blob.name.split('_')
                    if len(parts) >= 4:
                        block_range = parts[-1].replace('.json', '')
                        block_num = block_range.split('-')[0]
                        rpc_blocks.append(int(block_num))
                except:
                    pass
        
        if rpc_blocks:
            rpc_blocks.sort()
            print(f"   Sample available blocks: {rpc_blocks[:5]}")
        else:
            print("   Could not parse block numbers from filenames")
            
    except Exception as e:
        print(f"❌ GCS connection failed: {e}")
        raise


def test_block_retrieval(container, block_number: int):
    """Test 3: Retrieve and examine a specific block"""
    print(f"\n=== Test 3: Block Retrieval (Block {block_number}) ===")
    
    try:
        storage_handler = container.get(GCSHandler)
        
        # Get raw block from GCS
        print(f"Retrieving block {block_number} from GCS...")
        raw_block = storage_handler.get_rpc_block(block_number)
        
        if raw_block is None:
            print(f"❌ Block {block_number} not found in GCS")
            return None
            
        print(f"✅ Retrieved block {block_number}")
        print(f"   Block timestamp: {raw_block.timestamp}")
        print(f"   Transaction count: {len(raw_block.transactions)}")
        print(f"   Receipt count: {len(raw_block.receipts)}")
        
        # Quick validation
        if len(raw_block.transactions) != len(raw_block.receipts):
            print(f"⚠️  Warning: Transaction count ({len(raw_block.transactions)}) != Receipt count ({len(raw_block.receipts)})")
        
        return raw_block
        
    except Exception as e:
        print(f"❌ Block retrieval failed: {e}")
        raise


def test_block_decoding(container, raw_block: EvmFilteredBlock):
    """Test 4: Decode raw block into structured transactions"""
    print(f"\n=== Test 4: Block Decoding ===")
    
    try:
        block_decoder = container.get(BlockDecoder)
        
        print("Decoding block...")
        decoded_block = block_decoder.decode_block(raw_block)
        
        print(f"✅ Block decoded successfully")
        print(f"   Block number: {decoded_block.block_number}")
        print(f"   Decoded transactions: {len(decoded_block.transactions) if decoded_block.transactions else 0}")
        
        if decoded_block.transactions:
            # Examine first transaction
            first_tx_hash = next(iter(decoded_block.transactions.keys()))
            first_tx = decoded_block.transactions[first_tx_hash]
            
            print(f"   Sample transaction:")
            print(f"     Hash: {first_tx.tx_hash}")
            print(f"     From: {first_tx.origin_from}")
            print(f"     To: {first_tx.origin_to}")
            print(f"     Success: {first_tx.tx_success}")
            print(f"     Logs: {len(first_tx.logs)}")
            
            # Count decoded vs encoded logs
            decoded_logs = sum(1 for log in first_tx.logs.values() if hasattr(log, 'name'))
            print(f"     Decoded logs: {decoded_logs}/{len(first_tx.logs)}")
        
        return decoded_block
        
    except Exception as e:
        print(f"❌ Block decoding failed: {e}")
        raise


def test_transformation(container, decoded_block: Block):
    """Test 5: Transform decoded block through transformer pipeline"""
    print(f"\n=== Test 5: Transformation ===")
    
    try:
        transform_manager = container.get(TransformationManager)
        
        if not decoded_block.transactions:
            print("⚠️  No transactions to transform")
            return decoded_block
        
        transformed_transactions = {}
        total_processed = 0
        total_errors = 0
        total_transfers = 0
        total_events = 0
        
        for tx_hash, transaction in decoded_block.transactions.items():
            print(f"   Processing transaction {tx_hash[:10]}...")
            
            processed, transformed_tx = transform_manager.process_transaction(transaction)

            # Add this right after the transform_manager.process_transaction() call:

            print(f"     Processed: {processed}")
            print(f"     Transfers: {len(transformed_tx.transfers) if transformed_tx.transfers else 0}")
            print(f"     Events: {len(transformed_tx.events) if transformed_tx.events else 0}")
            print(f"     Errors: {len(transformed_tx.errors) if transformed_tx.errors else 0}")

            # Show the actual transfer objects if any exist
            if transformed_tx.transfers:
                print(f"     Transfer details:")
                for transfer_id, transfer in transformed_tx.transfers.items():
                    print(f"       {transfer_id}: {transfer.token} {transfer.amount} {transfer.from_address} → {transfer.to_address}")
            else:
                print(f"     ⚠️  No transfers created despite having Transfer logs")


            transformed_transactions[tx_hash] = transformed_tx
            
            if processed:
                total_processed += 1
                
            # Count results
            if transformed_tx.transfers:
                total_transfers += len(transformed_tx.transfers)
            if transformed_tx.events:
                total_events += len(transformed_tx.events)
            if transformed_tx.errors:
                total_errors += len(transformed_tx.errors)
        
        # Update block with transformed transactions
        transformed_block = msgspec.convert(decoded_block, type=Block)
        transformed_block.transactions = transformed_transactions
        
        print(f"✅ Transformation completed")
        print(f"   Transactions processed: {total_processed}/{len(decoded_block.transactions)}")
        print(f"   Total transfers: {total_transfers}")
        print(f"   Total events: {total_events}")
        print(f"   Total errors: {total_errors}")

        print("Debug: Transformation errors:")
        for tx_hash, tx in transformed_block.transactions.items():
            if tx.errors:
                for error_id, error in tx.errors.items():
                    print(f"  Error: {error.message}")
                    print(f"  Type: {error.error_type}")
                    if error.context:
                        print(f"  Context: {error.context}")

        print(f"\n🔍 Debug: All logs breakdown:")
        decoded_contracts = set()
        undecoded_contracts = set()

        for log_index, log in transaction.logs.items():
            if hasattr(log, 'name'):  # DecodedLog
                print(f"   ✅ Log {log_index}: {log.name} from {log.contract}")
                decoded_contracts.add(log.contract)
            else:  # EncodedLog  
                print(f"   ❌ Log {log_index}: Undecoded from {log.contract}")
                undecoded_contracts.add(log.contract)

        print(f"\n📊 Summary:")
        print(f"   Decoded contracts: {len(decoded_contracts)}")
        for addr in sorted(decoded_contracts):
            print(f"     {addr}")
        print(f"   Undecoded contracts: {len(undecoded_contracts)}")
        for addr in sorted(undecoded_contracts):
            print(f"     {addr}")

        return transformed_block
        
        
    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        raise


def test_storage(container, transformed_block: Block):
    """Test 6: Store transformed block with proper processing flow"""
    print(f"\n=== Test 6: Storage ===")
    
    try:
        storage_handler = container.get(GCSHandler)
        
        # Check if block has any errors
        has_errors = any(
            tx.errors for tx in transformed_block.transactions.values() 
            if tx.errors
        )
        
        error_count = sum(
            len(tx.errors) for tx in transformed_block.transactions.values()
            if tx.errors
        )
        
        # Set processing metadata
        from datetime import datetime
        if not transformed_block.processing_metadata:
            transformed_block.processing_metadata = ProcessingMetadata()
            
        transformed_block.processing_metadata.completed_at = datetime.utcnow().isoformat()
        transformed_block.processing_metadata.error_count = error_count
        
        if has_errors:
            print(f"⚠️  Block has {error_count} errors - storing in processing/")
            transformed_block.processing_metadata.error_stage = "transform"
            
            success = storage_handler.save_processing_block(
                transformed_block.block_number, 
                transformed_block
            )
            
            if success:
                print(f"✅ Block {transformed_block.block_number} stored in processing/ (has errors)")
                print("   💡 Block will remain in processing/ until errors are resolved")
            else:
                print(f"❌ Failed to store block in processing/")
                
        else:
            print(f"✅ Block processed successfully - storing in complete/")
            
            success = storage_handler.save_complete_block(
                transformed_block.block_number, 
                transformed_block
            )
            
            if success:
                print(f"✅ Block {transformed_block.block_number} stored in complete/")
                print("   🧹 Removed from processing/ (if it existed)")
                
                # Verify by reading back
                if has_errors:
                    stored_block = storage_handler.get_processing_block(transformed_block.block_number)
                    if stored_block:
                        print("✅ Verification: Block successfully read back from processing/")
                else:
                    stored_block = storage_handler.get_complete_block(transformed_block.block_number)
                    if stored_block:
                        print("✅ Verification: Block successfully read back from complete/")
            else:
                print(f"❌ Failed to store block in complete/")
        
        # Show processing summary
        summary = storage_handler.get_processing_summary()
        print(f"\n📊 Processing Summary:")
        print(f"   Processing: {summary['processing_count']} blocks")
        print(f"   Complete: {summary['complete_count']} blocks")
        if summary['latest_complete']:
            print(f"   Latest complete: {summary['latest_complete']}")
        if summary['oldest_processing']:
            print(f"   Oldest processing: {summary['oldest_processing']}")
            
        return success
        
    except Exception as e:
        print(f"❌ Storage failed: {e}")
        raise


def main():
    """Run the complete testing pipeline"""
    print("🚀 Starting Blockchain Indexer Pipeline Test\n")
    
    # Get block number from command line argument or prompt user
    import sys
    
    if len(sys.argv) > 1:
        try:
            test_block_number = int(sys.argv[1])
            print(f"Using block number from command line: {test_block_number}")
        except ValueError:
            print("❌ Invalid block number provided. Please provide a valid integer.")
            return
    else:
        try:
            test_block_number = int(input("Enter block number to test: "))
        except ValueError:
            print("❌ Invalid block number. Please provide a valid integer.")
            return
    
    try:
        # Phase 1: Setup
        container = test_config_loading()
        # test_gcs_connection(container) # Validated, not needed for every run
        
        # Phase 2: Data Pipeline
        raw_block = test_block_retrieval(container, test_block_number)
        if raw_block is None:
            return
            
        decoded_block = test_block_decoding(container, raw_block)
        transformed_block = test_transformation(container, decoded_block)
        test_storage(container, transformed_block)
        
        print(f"\n🎉 Pipeline test completed successfully!")
        print(f"   Processed block: {test_block_number}")
        
    except Exception as e:
        print(f"\n💥 Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()