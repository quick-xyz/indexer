#!/usr/bin/env python3
"""
Transaction Inspector

Inspect what's actually in a transaction before it hits the database writer.
"""

import sys
import json
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Use the testing environment which properly handles DI container
sys.path.insert(0, str(PROJECT_ROOT / "testing"))
from testing import get_testing_environment


def inspect_transaction_data(block_number: int, tx_hash: str, model_name: str = None):
    """Inspect transaction data to see actual event types"""
    print(f"🔍 Inspecting transaction data for block {block_number}")
    
    # Initialize environment with proper DI container
    env = get_testing_environment(model_name=model_name)
    
    # Get GCS handler from the container
    from indexer.storage.gcs_handler import GCSHandler
    gcs = env.get_service(GCSHandler)
    
    # Get block from GCS
    print(f"📥 Loading block from GCS...")
    block_data = gcs.get_complete_block(block_number)
    
    if not block_data:
        print(f"❌ Block {block_number} not found in GCS")
        return
    
    print(f"✅ Block loaded: {len(block_data.transactions)} transactions")
    
    # Find the specific transaction
    if tx_hash in block_data.transactions:
        tx = block_data.transactions[tx_hash]
        
        print(f"\n📋 Transaction: {tx_hash}")
        print(f"   Success: {getattr(tx, 'tx_success', 'Unknown')}")
        print(f"   Events: {len(tx.events or {})}")
        print(f"   Positions: {len(tx.positions or {})}")
        print(f"   Signals: {len(tx.signals or {})}")
        print(f"   Errors: {len(tx.errors or {}) if hasattr(tx, 'errors') and tx.errors else 0}")
        
        if tx.events:
            print(f"\n🎯 ACTUAL EVENT TYPES AND DATA:")
            for i, (event_id, event) in enumerate(tx.events.items(), 1):
                print(f"\n   Event {i}: {event_id}")
                print(f"   Type: {type(event)} (name: {type(event).__name__})")
                print(f"   Module: {type(event).__module__}")
                print(f"   String representation: {str(event)}")
                
                # Check for attributes
                if hasattr(event, '__dict__'):
                    print(f"   Has __dict__: True")
                    attrs = {k: v for k, v in event.__dict__.items() if not k.startswith('_')}
                    print(f"   Attributes: {attrs}")
                else:
                    print(f"   Has __dict__: False")
                
                if hasattr(event, 'to_dict'):
                    print(f"   Has to_dict: True")
                    try:
                        data = event.to_dict()
                        print(f"   to_dict() result: {data}")
                    except Exception as e:
                        print(f"   to_dict() error: {e}")
                else:
                    print(f"   Has to_dict: False")
                
                # Check parent classes
                print(f"   MRO (Method Resolution Order): {[cls.__name__ for cls in type(event).__mro__]}")
                
                # Check if it's a msgspec object
                try:
                    import msgspec
                    if hasattr(event, '__struct_fields__'):
                        print(f"   Is msgspec Struct: True")
                        print(f"   Struct fields: {event.__struct_fields__}")
                    else:
                        print(f"   Is msgspec Struct: False")
                except ImportError:
                    print(f"   msgspec not available for inspection")
        
        if tx.positions:
            print(f"\n📍 POSITION TYPES AND DATA:")
            for i, (pos_id, position) in enumerate(tx.positions.items(), 1):
                print(f"\n   Position {i}: {pos_id}")
                print(f"   Type: {type(position)} (name: {type(position).__name__})")
                print(f"   String representation: {str(position)}")
                
                if hasattr(position, '__dict__'):
                    attrs = {k: v for k, v in position.__dict__.items() if not k.startswith('_')}
                    print(f"   Attributes: {attrs}")
                
                # Check if it's a msgspec object
                try:
                    import msgspec
                    if hasattr(position, '__struct_fields__'):
                        print(f"   Is msgspec Struct: True")
                        print(f"   Struct fields: {position.__struct_fields__}")
                    else:
                        print(f"   Is msgspec Struct: False")
                except ImportError:
                    print(f"   msgspec not available for inspection")
        
        if tx.signals:
            print(f"\n📡 SIGNAL TYPES:")
            for signal_id, signal in tx.signals.items():
                print(f"   Signal {signal_id}: {type(signal).__name__} = {str(signal)}")
    else:
        print(f"❌ Transaction {tx_hash} not found in block")
        print(f"Available transactions: {list(block_data.transactions.keys())}")


def inspect_block_65297576():
    """Inspect the specific block that was failing"""
    block_number = 65297576
    tx_hash = "0x9aadf86e90c7aea93786fb4f1b0376bbc191330df68bebb5238431b905402496"
    
    inspect_transaction_data(block_number, tx_hash)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # Default to the failing transaction
        inspect_block_65297576()
    elif len(sys.argv) == 2:
        block_number = int(sys.argv[1])
        # Get first transaction
        env = get_testing_environment()
        from indexer.storage.gcs_handler import GCSHandler
        gcs = env.get_service(GCSHandler)
        block_data = gcs.get_complete_block(block_number)
        if block_data and block_data.transactions:
            tx_hash = next(iter(block_data.transactions.keys()))
            inspect_transaction_data(block_number, tx_hash)
        else:
            print(f"❌ No transactions found in block {block_number}")
    elif len(sys.argv) == 3:
        block_number = int(sys.argv[1])
        tx_hash = sys.argv[2]
        inspect_transaction_data(block_number, tx_hash)
    else:
        print("Usage:")
        print("  python transaction_inspector.py  # Inspect the known failing transaction")
        print("  python transaction_inspector.py <block_number>  # Inspect first transaction in block")
        print("  python transaction_inspector.py <block_number> <tx_hash>  # Inspect specific transaction")