# scripts/local_process.py

from pathlib import Path
from indexer import create_indexer

def main():
    # Config file is in repository config/ directory
    config_path = Path(__file__).parent.parent / "config" / "config.json"
    
    # Create indexer instance
    indexer = create_indexer(config_path=str(config_path))
    
    # Process a single block
    rpc = indexer.get(QuickNodeRpcClient)
    latest_block = rpc.get_latest_block_number()
    print(f"Latest block: {latest_block}")

if __name__ == "__main__":
    main()