# scripts/deploy.py (Cloud deployment script)
import os
from pathlib import Path
from indexer import create_indexer

def main():
    # In cloud, config path comes from environment or default
    config_path = os.getenv("CONFIG_PATH", "/app/config/config.json")
    
    # Create indexer with production config
    indexer = create_indexer(config_path=config_path)
    
    # Start continuous processing
    orchestrator = indexer.get(PipelineOrchestrator)
    orchestrator.start_continuous_processing()

if __name__ == "__main__":
    main()