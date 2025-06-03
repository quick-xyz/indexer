from flask import Flask, request, jsonify
import os

from indexer.component_registry import registry
import indexer



ind = indexer.create_indexer()

app = Flask(__name__)

os.environ["SECRET_KEY"] = os.urandom(32).hex()



# Define a route for the root URL
@app.route("/wesmol")
def hello_world():
	return "We smol!"

@app.route("/wesmol/test", methods=["GET"])
def run_test():
	# Init
	block = request.args.get("block_number")

	if not block:
		return {"error": "Missing block_number parameter."}, 400

	gcs_handler = registry.get("gcs_handler")
	decoder = registry.get("block_decoder")

	print("gcs_handler: {}".format(gcs_handler))
	print("decoder: {}".format(decoder))

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

	return {"status": true, "message": f"Block {block} decoded."}, 200


# Run the application if this script is executed directly
if __name__ == "__main__":
	app.run(port=os.getenv("INDEXER_FLASK_PORT", 8080), debug=os.getenv("LOG_LEVEL"))