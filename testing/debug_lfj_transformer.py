# testing/debug_lfj_transformer.py
"""
Debug script specifically for LfjPoolTransformer Swap processing
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.registry import TransformRegistry


def debug_lfj_swap_processing():
    """Debug the specific LFJ pool swap that's failing"""
    
    # Initialize testing environment
    testing_env = get_testing_environment(log_level="DEBUG")
    logger = testing_env.get_logger("debug.lfj")
    
    # Get services
    storage_handler = testing_env.get_service(GCSHandler)
    block_decoder = testing_env.get_service(BlockDecoder)
    transformer_registry = testing_env.get_service(TransformRegistry)
    
    # Get the problematic transaction
    block_number = 63269916
    tx_hash = "0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b"
    
    print(f"🔍 Debugging LFJ Pool Swap Processing")
    print(f"Block: {block_number}")
    print(f"TX: {tx_hash}")
    print("=" * 60)
    
    # Get and decode the block
    raw_block = storage_handler.get_rpc_block(block_number)
    decoded_block = block_decoder.decode_block(raw_block)
    transaction = decoded_block.transactions[tx_hash]
    
    # Find the LFJ pool transformer
    lfj_pool_address = "0x39888258d60fed9228f89e13eb57a92f1fa832eb"
    transformer = transformer_registry.get_transformer(lfj_pool_address)
    
    print(f"🏊 LFJ Pool Transformer Info:")
    print(f"   Address: {lfj_pool_address}")
    print(f"   Transformer: {type(transformer).__name__ if transformer else 'None'}")
    print(f"   Contract Address: {transformer.contract_address if transformer else 'N/A'}")
    print(f"   Base Token: {transformer.base_token if transformer else 'N/A'}")
    print(f"   Quote Token: {transformer.quote_token if transformer else 'N/A'}")
    print()
    
    # Find the Swap log (index 23)
    swap_log = transaction.logs[23]
    print(f"🔄 Swap Log Analysis:")
    print(f"   Index: 23")
    print(f"   Name: {swap_log.name}")
    print(f"   Contract: {swap_log.contract}")
    print(f"   Attributes: {swap_log.attributes}")
    print()
    
    # Test the get_in_out_amounts method specifically
    print(f"🧮 Testing get_in_out_amounts method:")
    try:
        base_amount, quote_amount = transformer.get_in_out_amounts(swap_log)
        print(f"   Base amount: {base_amount}")
        print(f"   Quote amount: {quote_amount}")
        print(f"   ✅ Method executed successfully")
    except Exception as e:
        print(f"   ❌ Exception in get_in_out_amounts: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return
    print()
    
    # Test attribute extraction
    print(f"📊 Raw Swap Attributes:")
    for key, value in swap_log.attributes.items():
        print(f"   {key}: {value} (type: {type(value).__name__})")
    print()
    
    # Calculate amounts manually
    print(f"🔢 Manual Amount Calculation:")
    try:
        from indexer.utils.amounts import amount_to_int, amount_to_str
        
        amount0_in = amount_to_int(swap_log.attributes.get("amount0In", 0))
        amount0_out = amount_to_int(swap_log.attributes.get("amount0Out", 0))
        amount1_in = amount_to_int(swap_log.attributes.get("amount1In", 0))
        amount1_out = amount_to_int(swap_log.attributes.get("amount1Out", 0))
        
        print(f"   amount0_in: {amount0_in}")
        print(f"   amount0_out: {amount0_out}")
        print(f"   amount1_in: {amount1_in}")
        print(f"   amount1_out: {amount1_out}")
        
        amount0 = amount_to_str(amount0_in - amount0_out)
        amount1 = amount_to_str(amount1_in - amount1_out)
        
        print(f"   Net amount0: {amount0}")
        print(f"   Net amount1: {amount1}")
        
        # Check base/quote logic
        if transformer.token0 == transformer.base_token:
            calculated_base = amount0
            calculated_quote = amount1
        else:
            calculated_base = amount1
            calculated_quote = amount0
            
        print(f"   Calculated base: {calculated_base}")
        print(f"   Calculated quote: {calculated_quote}")
        
    except Exception as e:
        print(f"   ❌ Exception in manual calculation: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    print()
    
    # Test the full _handle_swap method
    print(f"🏊 Testing _handle_swap method:")
    try:
        result = transformer._handle_swap(swap_log, transaction)
        print(f"   Result keys: {list(result.keys())}")
        print(f"   Transfers: {len(result.get('transfers', {}))}")
        print(f"   Events: {len(result.get('events', {}))}")
        print(f"   Errors: {len(result.get('errors', {}))}")
        
        if result.get('errors'):
            print(f"   🚨 Errors found:")
            for error_id, error in result['errors'].items():
                print(f"     {error_id}: {error.message}")
                
    except Exception as e:
        print(f"   ❌ Exception in _handle_swap: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    print()
    
    # Check available transfers for the swap
    print(f"💰 Available Transfers for Swap:")
    unmatched_transfers = transformer._get_unmatched_transfers(transaction)
    swap_transfers = transformer._get_swap_transfers(unmatched_transfers)
    
    print(f"   Total unmatched transfers: {len(unmatched_transfers)}")
    print(f"   Base swaps: {len(swap_transfers['base_swaps'])}")
    print(f"   Quote swaps: {len(swap_transfers['quote_swaps'])}")
    
    for category, transfers in swap_transfers.items():
        if transfers:
            print(f"   {category}:")
            for transfer_id, transfer in transfers.items():
                print(f"     {transfer_id}: {transfer.token} {transfer.amount} {transfer.from_address} → {transfer.to_address}")


if __name__ == "__main__":
    debug_lfj_swap_processing()