# scripts/test_config.py

"""
Phase 1 Test: Configuration Loading and Validation
"""
import os
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_config_loading():
    """Test basic configuration loading functionality"""
    print("🔧 Testing Configuration Loading...")
    
    try:
        from indexer.core.config import IndexerConfig
        
        # Test 1: Load from file
        config_path = project_root / "config" / "config.json"
        if not config_path.exists():
            print(f"❌ Config file not found: {config_path}")
            return False
            
        config = IndexerConfig.from_file(str(config_path))
        print(f"✅ Configuration loaded from file")
        print(f"   Name: {config.name}")
        print(f"   Version: {config.version}")
        print(f"   Contracts: {len(config.contracts)}")
        print(f"   Addresses: {len(config.addresses)}")
        
        # Test 2: Environment variables
        required_env_vars = [
            "INDEXER_DB_USER",
            "INDEXER_DB_PASSWORD", 
            "INDEXER_DB_NAME",
            "INDEXER_DB_HOST",
            "INDEXER_AVAX_RPC"
        ]
        
        missing_vars = []
        for var in required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"⚠️  Missing environment variables: {missing_vars}")
            print("   Set these in your .env file for full functionality")
        else:
            print("✅ All required environment variables present")
            
        # Test 3: Configuration structure
        assert hasattr(config, 'storage'), "Missing storage config"
        assert hasattr(config, 'contracts'), "Missing contracts config"
        assert hasattr(config, 'database'), "Missing database config"
        assert hasattr(config, 'rpc'), "Missing RPC config"
        
        print("✅ Configuration structure validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")
        return False

def test_contract_abi_loading():
    """Test contract ABI file loading"""
    print("\n🔧 Testing Contract ABI Loading...")
    
    try:
        from indexer.core.config import IndexerConfig
        
        config_path = project_root / "config" / "config.json"
        config = IndexerConfig.from_file(str(config_path))
        
        contracts_loaded = 0
        contracts_failed = 0
        
        for address, contract in config.contracts.items():
            if contract.abi and len(contract.abi) > 0:
                contracts_loaded += 1
                print(f"   ✅ {contract.name} ({address[:8]}...): {len(contract.abi)} ABI entries")
            else:
                contracts_failed += 1
                print(f"   ❌ {contract.name} ({address[:8]}...): No ABI loaded")
        
        print(f"✅ Loaded {contracts_loaded} contracts successfully")
        if contracts_failed > 0:
            print(f"⚠️  {contracts_failed} contracts failed to load ABIs")
            
        return contracts_loaded > 0
        
    except Exception as e:
        print(f"❌ Contract ABI loading failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("PHASE 1: Configuration and ABI Loading Tests")
    print("=" * 60)
    
    success = True
    success &= test_config_loading()
    success &= test_contract_abi_loading()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 Phase 1 tests PASSED - Configuration system working")
    else:
        print("💥 Phase 1 tests FAILED - Check configuration setup")
        sys.exit(1)


