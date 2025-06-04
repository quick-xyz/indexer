# scripts/test_container.py

"""
Phase 1 Test: Dependency Injection Container
"""
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_container_creation():
    """Test basic container creation and service registration"""
    print("🔧 Testing Container Creation...")
    
    try:
        from indexer.core.container import IndexerContainer
        from indexer.core.config import IndexerConfig
        
        # Load config
        config_path = project_root / "config" / "config.json"
        config = IndexerConfig.from_file(str(config_path))
        
        # Create container
        container = IndexerContainer(config)
        print("✅ Container created successfully")
        
        # Test service registration
        class TestService:
            def __init__(self, config):
                self.config = config
                
        container.register_singleton(TestService, TestService)
        print("✅ Service registration successful")
        
        # Test service retrieval
        service = container.get(TestService)
        assert service is not None, "Service should not be None"
        assert service.config == config, "Service should have config injected"
        print("✅ Service retrieval and dependency injection working")
        
        # Test singleton behavior
        service2 = container.get(TestService)
        assert service is service2, "Should return same instance for singleton"
        print("✅ Singleton behavior working")
        
        return True
        
    except Exception as e:
        print(f"❌ Container test failed: {e}")
        return False

def test_service_registration():
    """Test the actual service registration from the package"""
    print("\n🔧 Testing Service Registration...")
    
    try:
        from indexer import create_indexer
        
        # Create indexer (this runs service registration)
        config_path = project_root / "config" / "config.json"
        indexer = create_indexer(config_path=str(config_path))
        
        print("✅ Indexer created successfully")
        
        # Test that we can get registered services
        from indexer.contracts.registry import ContractRegistry
        from indexer.contracts.manager import ContractManager
        
        registry = indexer.get(ContractRegistry)
        assert registry is not None, "ContractRegistry should be available"
        print(f"✅ ContractRegistry: {registry.get_contract_count()} contracts")
        
        manager = indexer.get(ContractManager)
        assert manager is not None, "ContractManager should be available"
        print("✅ ContractManager created")
        
        # Test dependency injection worked
        assert manager.registry is registry, "Manager should have registry injected"
        print("✅ Dependency injection working between services")
        
        return True
        
    except Exception as e:
        print(f"❌ Service registration test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("PHASE 1: Container and Service Registration Tests")
    print("=" * 60)
    
    success = True
    success &= test_container_creation()
    success &= test_service_registration()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 Phase 1 tests PASSED - Container system working")
    else:
        print("💥 Phase 1 tests FAILED - Check container setup")
        sys.exit(1)


