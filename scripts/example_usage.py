def example_usage():
    """Example of how to use the refactored indexer"""
    
    # Create indexer container
    indexer = create_indexer(config_path="config/config.json")
    
    # Get services (created on-demand with dependencies automatically injected)
    rpc = indexer.get(QuickNodeRPCClient)
    storage = indexer.get(GCSHandler)
    decoder = indexer.get(BlockDecoder)
    transformer = indexer.get(TransformManager)
    
    # OR use convenience functions
    rpc = get_rpc_client(indexer)
    decoder = get_block_decoder(indexer)
    
    # Example: Process a single block
    latest_block_number = rpc.get_latest_block_number()
    
    # Get raw block from storage or RPC
    raw_block = storage.get_rpc_block(latest_block_number)
    if not raw_block:
        # Fetch from RPC and optionally store
        block_data = rpc.get_block_with_receipts(latest_block_number)
        # Convert block_data to EvmFilteredBlock format...
    
    # Decode the block
    decoded_block = decoder.decode_block(raw_block)
    
    # Transform transactions
    transformed_transactions = {}
    for tx_hash, transaction in decoded_block.transactions.items():
        success, transformed_tx = transformer.process_transaction(transaction)
        if success:
            transformed_transactions[tx_hash] = transformed_tx
    
    # Save processed block
    processed_block = decoded_block.copy(deep=True)
    processed_block.transactions = transformed_transactions
    storage.save_decoded_block(latest_block_number, processed_block)
    
    print(f"Processed block {latest_block_number} with {len(transformed_transactions)} transactions")

if __name__ == "__main__":
    example_usage()