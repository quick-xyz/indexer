#!/usr/bin/env python3
# testing/diagnostics/di_diagnostic.py
"""
DI Container Diagnostic

Verifies that the dependency injection container is properly set up
and all required services can be instantiated.
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from typing import List, Tuple, Type, Any


class DIContainerDiagnostic:
    """Check DI container and service initialization."""
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name
        self.results: List[Tuple[str, bool, str]] = []
        
    def run(self) -> bool:
        """Run all DI container checks."""
        print("🔧 DI Container Diagnostic")
        print("=" * 60)
        
        # Test environment initialization
        if not self._test_environment_init():
            return False
            
        # Test core services
        self._test_core_services()
        
        # Test database services
        self._test_database_services()
        
        # Test pipeline services
        self._test_pipeline_services()
        
        # Print results
        self._print_results()
        
        return all(result[1] for result in self.results)
    
    def _test_environment_init(self) -> bool:
        """Test basic environment initialization."""
        print("\n📦 Testing environment initialization...")
        
        try:
            self.env = get_testing_environment(model_name=self.model_name)
            self.results.append(("Environment initialization", True, "OK"))
            
            # Check config
            config = self.env.get_config()
            self.results.append((
                f"Config loaded ({config.model_name} v{config.model_version})", 
                True, 
                "OK"
            ))
            
            return True
            
        except Exception as e:
            self.results.append(("Environment initialization", False, str(e)))
            print(f"❌ Failed to initialize environment: {e}")
            return False
    
    def _test_core_services(self):
        """Test core infrastructure services."""
        print("\n🏗️ Testing core services...")
        
        core_services = [
            ("SecretsService", "indexer.core.secrets_service", "SecretsService"),
            ("QuickNodeRpcClient", "indexer.clients.quicknode_rpc", "QuickNodeRpcClient"),
        ]
        
        self._test_services(core_services)
    
    def _test_database_services(self):
        """Test database-related services."""
        print("\n🗄️ Testing database services...")
        
        db_services = [
            ("ModelDatabaseManager", "indexer.database.connection", "ModelDatabaseManager"),
            ("InfrastructureDatabaseManager", "indexer.database.connection", "InfrastructureDatabaseManager"),
            ("RepositoryManager", "indexer.database.repository", "RepositoryManager"),
        ]
        
        self._test_services(db_services)
    
    def _test_pipeline_services(self):
        """Test pipeline component services."""
        print("\n🔄 Testing pipeline services...")
        
        pipeline_services = [
            ("GCSHandler", "indexer.storage.gcs_handler", "GCSHandler"),
            ("BlockDecoder", "indexer.decode.block_decoder", "BlockDecoder"),
            ("TransformManager", "indexer.transform.manager", "TransformManager"),
            ("TransformRegistry", "indexer.transform.registry", "TransformRegistry"),
            ("ContractRegistry", "indexer.contracts.registry", "ContractRegistry"),
            ("ContractManager", "indexer.contracts.manager", "ContractManager"),
        ]
        
        self._test_services(pipeline_services)
    
    def _test_services(self, services: List[Tuple[str, str, str]]):
        """Test a list of services."""
        for display_name, module_path, class_name in services:
            try:
                # Import the service class
                module = __import__(module_path, fromlist=[class_name])
                service_class = getattr(module, class_name)
                
                # Try to get from container
                service = self.env.get_service(service_class)
                
                if service:
                    self.results.append((display_name, True, type(service).__name__))
                else:
                    self.results.append((display_name, False, "Service is None"))
                    
            except Exception as e:
                error_msg = str(e).split('\n')[0]  # First line only
                self.results.append((display_name, False, error_msg))
    
    def _print_results(self):
        """Print diagnostic results."""
        print("\n📊 Results:")
        print("-" * 60)
        
        for service_name, success, info in self.results:
            status = "✅" if success else "❌"
            print(f"{status} {service_name:<30} {info}")
        
        # Summary
        total = len(self.results)
        passed = sum(1 for _, success, _ in self.results if success)
        
        print("-" * 60)
        print(f"Total: {passed}/{total} services initialized successfully")
        
        if passed < total:
            print("\n💡 Common fixes:")
            print("  - Check environment variables in .env file")
            print("  - Verify database connection settings")
            print("  - Ensure GCP credentials are configured")
            print("  - Run database migrations")


def main():
    """Run DI container diagnostic."""
    import argparse
    
    parser = argparse.ArgumentParser(description='DI Container Diagnostic')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        diagnostic = DIContainerDiagnostic(model_name=args.model)
        success = diagnostic.run()
        
        if success:
            print("\n✅ All DI container checks passed!")
        else:
            print("\n❌ Some DI container checks failed")
            
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 Diagnostic failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()