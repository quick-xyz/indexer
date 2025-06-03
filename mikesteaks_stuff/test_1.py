from indexer.component_registry import registry
import indexer
from datetime import datetime, timezone

ind = indexer.create_indexer()


# Init
block = 58219691
gcs_handler = registry.get("gcs_handler")
decoder = registry.get("block_decoder")

print("gcs_handler: {}".format(gcs_handler))
print("decoder: {}".format(decoder))

time = datetime(2025, 5, 15, 21, 0, 14)  # 5/15/25 9:00:14 PM

blocks = gcs_handler.list_blobs_updated_since(time)

print(f"blocks since time {time}: {blocks}")

# GET BLOCK
print(f"Getting block {block} from GCS")
evm_block_obj = gcs_handler.get_rpc_block(block)
# print(f"Block received from GCS: {evm_block_obj}")

# DECODE BLOCK
print(f"Decoding block...")
decoded_block_obj = decoder.decode_block(evm_block_obj)
# print(f"Block decoded: {decoded_block_obj}")

# STORE BLOCK
print(f"Storing decoded block...")
gcs_handler.save_decoded_block(block,decoded_block_obj)
print(f"Decoded block stored in GCS")
