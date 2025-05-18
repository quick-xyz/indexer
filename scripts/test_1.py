from indexer.component_registry import registry

# Init
block = 58219691
gcs_handler = registry.get("gcs_handler")
decoder = registry.get("decoder")

# GET BLOCK
print(f"Getting block {block} from GCS")
evm_block_obj = gcs_handler.get_rpc_block(block)
print(f"Block received from GCS: {evm_block_obj}")

# DECODE BLOCK
print(f"Decoding block...")
decoded_block_obj = decoder.decode_block(evm_block_obj)
print(f"Block decoded: {decoded_block_obj}")

# STORE BLOCK
print(f"Storing decoded block...")
gcs_handler.save_decoded_block(block,decoded_block_obj)
print(f"Decoded block stored in GCS")