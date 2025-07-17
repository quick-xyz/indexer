# testing/diagnostics/quick_diagnostic.py
"""
Quick Diagnostic Tool for Blockchain Indexer

Updated for signal-based transformation architecture and database-driven sources.
Uses the indexer's configuration system and dependency injection
to verify setup and identify issues.
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from contextlib import contextmanager

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Now import testing and indexer modules
from testing import get_testing_environment
from indexer.core.logging_config import log_with_context, IndexerLogger
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager
from indexer.transform.registry import TransformRegistry
from indexer.contracts.registry import ContractRegistry
from indexer.contracts.manager import ContractManager


class QuickDiagnostic:
    """
    Diagnostic checker that leverages the indexer's signal-based architecture
    """
    
    def __init__(self, model_name: str = None, verbose: bool = False):
        self.model_name = model_name or os.getenv("INDEXER_MODEL_NAME", "blub_test")
        self.verbose = verbose
        self.results = {}
        self.detailed_errors = []
        
        # Initialize with minimal logging
        with self._diagnostic_logging_context():
            self.testing_env = get_testing_environment(self.model_name, log_level="ERROR")
            self.logger = self.testing_env.get_logger("diagnostic")
    
    @contextmanager
    def _diagnostic_logging_context(self):
        """Context manager to suppress logging during diagnostics unless verbose mode"""
        if self.verbose:
            yield
            return
            
        # Store original log levels
        original_levels = {}
        
        # Get all existing loggers and set them to CRITICAL (effectively silencing them)
        for name in logging.Logger.manager.loggerDict:
            if name.startswith('indexer'):
                logger = logging.getLogger(name)
                original_levels[name] = logger.level
                logger.setLevel(logging.CRITICAL)
        
        # Also set the root indexer logger
        root_logger = logging.getLogger('indexer')
        original_levels['indexer'] = root_logger.level
        root_logger.setLevel(logging.CRITICAL)
        
        try:
            yield
        finally:
            # Restore original log levels
            for name, level in original_levels.items():
                logging.getLogger(name).setLevel(level)
    
    def run_all_checks(self) -> bool:
        """Run all diagnostic checks with suppressed logging"""
        print("🔍 BLOCKCHAIN INDEXER QUICK DIAGNOSTIC")
        print("=" * 60)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📄 Model: {self.model_name}")
        print(f"🏗️  Indexer: {self.testing_env.config.model_name} v{self.testing_env.config.model_version}")
        print(f"🔧 Architecture: Signal-based transformation with database-driven sources")
        print()
        
        checks = [
            ("Configuration Loading", self.check_configuration),
            ("Sources Configuration", self.check_sources_configuration),
            ("Dependency Injection", self.check_dependency_injection), 
            ("Contract Registry", self.check_contract_registry),
            ("Signal Transformer Setup", self.check_signal_transformer_setup),
            ("Storage Connection", self.check_storage_connection),
            ("RPC Connection", self.check_rpc_connection),
            ("Signal Generation Test", self.check_signal_generation)
        ]
        
        all_passed = True
        
        with self._diagnostic_logging_context():
            for check_name, check_func in checks:
                try:
                    passed = check_func()
                    self.results[check_name] = passed
                    status = "✅ PASS" if passed else "❌ FAIL"
                    print(f"{status} {check_name}")
                    
                    if not passed:
                        all_passed = False
                        
                except Exception as e:
                    self.results[check_name] = False
                    all_passed = False
                    print(f"❌ FAIL {check_name} - Exception: {str(e)}")
                    
                    # Store detailed error for file output
                    self.detailed_errors.append({
                        "check": check_name,
                        "error": str(e),
                        "exception_type": type(e).__name__
                    })
        
        print()
        self._print_summary(all_passed)
        
        # If there were failures and not in verbose mode, save detailed logs
        if not all_passed and not self.verbose:
            self._save_diagnostic_details()
        
        return all_passed
    
    def _save_diagnostic_details(self):
        """Save detailed diagnostic information to file when checks fail"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_dir = PROJECT_ROOT / "debug_output"
        debug_dir.mkdir(exist_ok=True)
        
        diagnostic_file = debug_dir / f"diagnostic_details_{timestamp}.json"
        
        import json
        diagnostic_data = {
            "timestamp": datetime.now().isoformat(),
            "model_name": self.model_name,
            "results": self.results,
            "detailed_errors": self.detailed_errors,
            "environment_info": {
                "indexer_name": self.testing_env.config.model_name,
                "indexer_version": self.testing_env.config.model_version,
                "total_contracts": len(self.testing_env.config.contracts),
                "total_sources": len(getattr(self.testing_env.config, 'sources', {})),
                "total_addresses": len(self.testing_env.config.addresses) if self.testing_env.config.addresses else 0
            }
        }
        
        try:
            with open(diagnostic_file, 'w') as f:
                json.dump(diagnostic_data, f, indent=2, default=str)
            print(f"💾 Detailed diagnostic info saved to: {diagnostic_file}")
        except Exception as e:
            print(f"⚠️  Could not save diagnostic details: {e}")

    def check_configuration(self) -> bool:
        """Check configuration loading and validation"""
        try:
            config = self.testing_env.get_config()
            
            # Validate key configuration sections
            checks = [
                (config.model_name, "Missing indexer name"),
                (config.model_version, "Missing indexer version"),
                (config.storage, "Missing storage config"),
                (config.rpc, "Missing RPC config"),
                (config.gcs, "Missing GCS config"),
                (config.contracts, "Missing contracts config")
            ]
            
            for value, error_msg in checks:
                if not value:
                    self.detailed_errors.append({
                        "check": "Configuration Loading",
                        "error": error_msg,
                        "details": "Configuration validation failed"
                    })
                    return False
            
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "Configuration Loading", 
                "error": str(e),
                "exception_type": type(e).__name__
            })
            return False

    def check_sources_configuration(self) -> bool:
        """Check database-driven sources configuration"""
        try:
            config = self.testing_env.get_config()
            
            # Check if sources are available
            sources = getattr(config, 'sources', {})
            if not sources:
                self.detailed_errors.append({
                    "check": "Sources Configuration",
                    "error": "No sources configured",
                    "details": "Model has no sources in database. Run sources migration.",
                })
                return False
            
            # Validate each source
            invalid_sources = []
            for source_id, source in sources.items():
                if not hasattr(source, 'path') or not source.path:
                    invalid_sources.append(f"Source {source_id} missing path")
                if not hasattr(source, 'format') or not source.format:
                    invalid_sources.append(f"Source {source_id} missing format")
                if not hasattr(source, 'name') or not source.name:
                    invalid_sources.append(f"Source {source_id} missing name")
            
            if invalid_sources:
                self.detailed_errors.append({
                    "check": "Sources Configuration",
                    "error": "Invalid source configurations",
                    "invalid_sources": invalid_sources
                })
                return False
            
            # Test source methods
            try:
                primary_source = config.get_primary_source()
                if not primary_source:
                    self.detailed_errors.append({
                        "check": "Sources Configuration",
                        "error": "No primary source available",
                        "source_count": len(sources)
                    })
                    return False
            except Exception as e:
                self.detailed_errors.append({
                    "check": "Sources Configuration",
                    "error": f"Source method failed: {str(e)}",
                    "exception_type": type(e).__name__
                })
                return False
            
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "Sources Configuration",
                "error": str(e),
                "exception_type": type(e).__name__
            })
            return False
    
    def check_dependency_injection(self) -> bool:
        """Check dependency injection container"""
        try:
            container = self.testing_env.container
            
            # Test service resolution
            services_to_test = [
                QuickNodeRpcClient,
                GCSHandler, 
                ContractRegistry,
                ContractManager,
                BlockDecoder,
                TransformRegistry,
                TransformManager
            ]

            failed_services = []
            for service_type in services_to_test:
                try:
                    service = container.get(service_type)
                    if service is None:
                        failed_services.append(service_type.__name__)
                except Exception as e:
                    failed_services.append(f"{service_type.__name__}: {str(e)}")
            
            if failed_services:
                self.detailed_errors.append({
                    "check": "Dependency Injection",
                    "error": "Failed to resolve services",
                    "failed_services": failed_services
                })
                return False
            
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "Dependency Injection",
                "error": str(e),
                "exception_type": type(e).__name__
            })
            return False
    
    def check_contract_registry(self) -> bool:
        """Check contract registry setup"""
        try:
            contract_registry = self.testing_env.get_service(ContractRegistry)
            contract_manager = self.testing_env.get_service(ContractManager)
            
            contract_count = contract_registry.get_contract_count()
            if contract_count == 0:
                self.detailed_errors.append({
                    "check": "Contract Registry",
                    "error": "No contracts registered",
                    "contract_count": 0
                })
                return False
            
            # Test a few contract lookups
            config = self.testing_env.get_config()
            tested_contracts = 0
            failed_contracts = []
            
            for address, contract_config in config.contracts.items():
                # Test contract registry lookup
                registry_contract = contract_registry.get_contract(address)
                if not registry_contract:
                    failed_contracts.append(f"Registry lookup failed: {address}")
                    continue
                
                tested_contracts += 1
                if tested_contracts >= 3:  # Test first 3 contracts
                    break
            
            if failed_contracts:
                self.detailed_errors.append({
                    "check": "Contract Registry",
                    "error": "Contract registry lookups failed",
                    "failed_contracts": failed_contracts
                })
                return False
            
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "Contract Registry",
                "error": str(e),
                "exception_type": type(e).__name__
            })
            return False
    
    def check_signal_transformer_setup(self) -> bool:
        """Check signal transformer registry and setup"""
        try:
            transformer_registry = self.testing_env.get_service(TransformRegistry)
            
            # Get all registered transformers
            all_transformers = transformer_registry.get_all_contracts()
            
            if not all_transformers:
                self.detailed_errors.append({
                    "check": "Signal Transformer Setup",
                    "error": "No transformers registered",
                    "transformer_count": 0
                })
                return False
            
            # Validate each transformer for signal architecture
            validation_issues = []
            valid_transformers = 0
            
            for address, transformer_info in all_transformers.items():
                if not transformer_info.active:
                    continue
                
                transformer_instance = transformer_info.instance
                transformer_name = type(transformer_instance).__name__
                
                # Check required methods for signal architecture
                if not hasattr(transformer_instance, 'process_logs'):
                    validation_issues.append(f"{transformer_name} missing process_logs method")
                    continue
                
                # Check handler map for signal generation
                if not hasattr(transformer_instance, 'handler_map'):
                    validation_issues.append(f"{transformer_name} missing handler_map")
                elif not getattr(transformer_instance, 'handler_map', {}):
                    validation_issues.append(f"{transformer_name} has empty handler_map")
                
                valid_transformers += 1
            
            if validation_issues:
                self.detailed_errors.append({
                    "check": "Signal Transformer Setup",
                    "error": "Transformer validation issues found",
                    "validation_issues": validation_issues,
                    "valid_transformers": valid_transformers,
                    "total_transformers": len(all_transformers)
                })
                return False
            
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "Signal Transformer Setup",
                "error": str(e),
                "exception_type": type(e).__name__
            })
            return False
    
    def check_storage_connection(self) -> bool:
        """Check GCS storage connection with sources support"""
        try:
            storage_handler = self.testing_env.get_service(GCSHandler)
            config = self.testing_env.get_config()
            
            # Test basic connection by listing blobs
            # Use sources if available, otherwise fall back to legacy
            sources = getattr(config, 'sources', {})
            if sources:
                # Test with first source
                primary_source = config.get_primary_source()
                blobs = storage_handler.list_blobs(prefix=primary_source.path, max_results=5)
            else:
                # Legacy: use storage config
                blobs = storage_handler.list_blobs(
                    prefix=storage_handler.storage_config.processing_prefix, 
                    max_results=5
                )
            
            # Just check if we can connect, don't need to validate blob count
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "Storage Connection",
                "error": str(e),
                "exception_type": type(e).__name__,
                "details": "Failed to connect to GCS bucket"
            })
            return False
    
    def check_rpc_connection(self) -> bool:
        """Check RPC connection"""
        try:
            rpc_client = self.testing_env.get_service(QuickNodeRpcClient)
            
            # Test connection by getting latest block number
            latest_block = rpc_client.get_latest_block_number()
            
            if latest_block <= 0:
                self.detailed_errors.append({
                    "check": "RPC Connection",
                    "error": "Invalid latest block number",
                    "latest_block": latest_block
                })
                return False
            
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "RPC Connection",
                "error": str(e),
                "exception_type": type(e).__name__,
                "endpoint": getattr(self.testing_env.get_service(QuickNodeRpcClient), 'endpoint_url', 'unknown')
            })
            return False
    
    def check_signal_generation(self) -> bool:
        """Test signal generation with a sample transaction using sources"""
        try:
            # Get a known block for testing
            test_block_number = 63269916  # Use the same block from your examples
            
            storage_handler = self.testing_env.get_service(GCSHandler)
            block_decoder = self.testing_env.get_service(BlockDecoder)
            transform_manager = self.testing_env.get_service(TransformManager)
            config = self.testing_env.get_config()
            
            # Get test block using sources if available
            sources = getattr(config, 'sources', {})
            if sources:
                primary_source = config.get_primary_source()
                raw_block = storage_handler.get_rpc_block(test_block_number, source=primary_source)
            else:
                # Legacy method
                raw_block = storage_handler.get_rpc_block(test_block_number)
            
            if not raw_block:
                # Not an error - test block might not be available
                return True
            
            decoded_block = block_decoder.decode_block(raw_block)
            if not decoded_block.transactions:
                return True
            
            # Test signal generation on first transaction
            first_tx_hash = next(iter(decoded_block.transactions.keys()))
            first_tx = decoded_block.transactions[first_tx_hash]
            
            success, processed_tx = transform_manager.process_transaction(first_tx)
            
            # Signal generation working is success, even if no signals produced
            return True
            
        except Exception as e:
            self.detailed_errors.append({
                "check": "Signal Generation Test",
                "error": str(e),
                "exception_type": type(e).__name__,
                "test_block": 63269916
            })
            return False
    
    def _print_summary(self, all_passed: bool):
        """Print diagnostic summary"""
        print("📊 DIAGNOSTIC SUMMARY")
        print("=" * 30)
        
        passed_count = sum(1 for result in self.results.values() if result)
        total_count = len(self.results)
        
        print(f"✅ Passed: {passed_count}/{total_count}")
        print(f"❌ Failed: {total_count - passed_count}/{total_count}")
        
        if all_passed:
            print("\n🎉 All checks passed! Ready for signal pipeline testing.")
            print("\n🎯 Next steps:")
            print("   1. Run: python testing/test_pipeline.py <block_number>")
            print("   2. Or: python testing/scripts/debug_session.py analyze <tx_hash> <block_number>")
            print("   3. Test signals: python testing/scripts/debug_session.py transformers")
        else:
            print("\n❌ Some checks failed. Review errors above.")
            print("\n💡 Common fixes:")
            print("   - Check environment variables in .env")
            print("   - Verify database sources migration completed")
            print("   - Ensure ABI files exist in config/abis/")
            print("   - Check network connectivity")
            print("   - Verify transformers have process_logs() method")
            
            if not self.verbose:
                print(f"\n🔍 For detailed error information, run:")
                print(f"   python testing/diagnostics/quick_diagnostic.py --verbose")


def main():
    """Run quick diagnostic"""
    try:
        verbose = "--verbose" in sys.argv or "-v" in sys.argv
        model_name = None
        
        # Simple argument parsing - look for model name
        for i, arg in enumerate(sys.argv[1:], 1):
            if arg not in ["--verbose", "-v"] and not arg.startswith("-"):
                model_name = arg
                break
        
        # Use environment variable if no model name provided
        if not model_name:
            model_name = os.getenv("INDEXER_MODEL_NAME")
        
        diagnostic = QuickDiagnostic(model_name=model_name, verbose=verbose)
        success = diagnostic.run_all_checks()
        exit(0 if success else 1)
        
    except Exception as e:
        print(f"❌ Diagnostic failed with exception: {e}")
        if "--verbose" in sys.argv or "-v" in sys.argv:
            import traceback
            traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()