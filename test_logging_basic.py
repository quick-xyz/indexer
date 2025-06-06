# test_logging_basic.py
try:
    from indexer import create_indexer
    from pathlib import Path
    
    config_path = Path("config/config.json")
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    container = create_indexer(config_path=str(config_path))

    print("✅ Basic indexer creation works with default config")
    
except Exception as e:
    print(f"❌ Error: {e}")