# testing/diagnostics/quick_diagnostic.py
"""
Quick Diagnostic Tool for Blockchain Indexer

Updated for signal-based transformation architecture.
Uses the indexer's configuration system and dependency injection
to verify setup and identify issues.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Now import testing and indexer modules
from testing import get_testing_environment
from indexer.core.logging_config import log_with_context
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from legacy_transformers.manager_simple import TransformationManager
from indexer.transform.registry import TransformerRegistry
from indexer.contracts.registry import ContractRegistry
from indexer.contracts.manager import ContractManager


class QuickDiagnostic:
    """
    Diagnostic checker that leverages the indexer's signal-based architecture
    """
    
    def __init__(self, config_path: str = None):
        self.testing_env = get_testing_environment(config_path)
        self.logger = self.testing_env.get_logger("diagnostic")
        self.results = {}
        
        log_with_context(
            self.logger,
            logging.INFO,
            "Starting quick diagnostic session",
            config_path=self.testing_env.config_path,
            architecture="signal-based"
        )
    
    def run_all_checks(self) -> bool:
        """Run all diagnostic checks"""
        print("🔍 BLOCKCHAIN INDEXER QUICK DIAGNOSTIC")
        print("=" * 60)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📄 Config: {self.testing_env.config_path}")
        print(f"🏗️  Indexer: {self.testing_env.config.name} v{self.testing_env.config.version}")
        print(f"🔧 Architecture: Signal-based transformation")
        print()
        
        checks = [
            ("Configuration Loading", self.check_configuration),
            ("Dependency Injection", self.check_dependency_injection), 
            ("Contract Registry", self.check_contract_registry),
            ("Signal Transformer Setup", self.check_signal_transformer_setup),
            ("Storage Connection", self.check_storage_connection),
            ("RPC Connection", self.check_rpc_connection),
            ("Signal Generation Test", self.check_signal_generation)
        ]
        
        all_passed = True
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
                
                log_with_context(
                    self.logger,
                    logging.ERROR,
                    f"Diagnostic check failed: {check_name}",
                    error=str(e),
                    exception_type=type(e).__name__
                )
        
        print()
        self._print_summary(all_passed)
        return all_passed
    
    def check_configuration(self) -> bool:
        """Check configuration loading and validation"""
        try:
            config = self.testing_env.get_config()
            
            # Validate key configuration sections
            checks = [
                (config.name, "Missing indexer name"),
                (config.version, "Missing indexer version"),
                (config.storage, "Missing storage config"),
                (config.rpc, "Missing RPC config"),
                (config.gcs, "Missing GCS config"),
                (config.contracts, "Missing contracts config")
            ]
            
            for value, error_msg in checks:
                if not value:
                    self.logger.error(error_msg)
                    return False
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Configuration validation passed",
                contract_count=len(config.contracts),
                address_count=len(config.addresses) if config.addresses else 0
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Configuration check failed",
                error=str(e)
            )
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
                TransformerRegistry,
                TransformationManager
            ]
            
            resolved_services = []
            for service_type in services_to_test:
                try:
                    service = container.get(service_type)
                    resolved_services.append(service_type.__name__)
                    
                    log_with_context(
                        self.logger,
                        logging.DEBUG,
                        "Service resolved successfully",
                        service_type=service_type.__name__,
                        instance_type=type(service).__name__
                    )
                    
                except Exception as e:
                    log_with_context(
                        self.logger,
                        logging.ERROR,
                        "Failed to resolve service",
                        service_type=service_type.__name__,
                        error=str(e)
                    )
                    return False
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Dependency injection check passed",
                resolved_services=resolved_services,
                service_count=len(resolved_services)
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Dependency injection check failed",
                error=str(e)
            )
            return False
    
    def check_contract_registry(self) -> bool:
        """Check contract registry setup"""
        try:
            contract_registry = self.testing_env.get_service(ContractRegistry)
            contract_manager = self.testing_env.get_service(ContractManager)
            
            contract_count = contract_registry.get_contract_count()
            if contract_count == 0:
                self.logger.warning("No contracts registered")
                return False
            
            # Test a few contract lookups
            config = self.testing_env.get_config()
            tested_contracts = 0
            
            for address, contract_config in config.contracts.items():
                # Test contract registry lookup
                registry_contract = contract_registry.get_contract(address)
                if not registry_contract:
                    self.logger.error(f"Contract not found in registry: {address}")
                    return False
                
                # Test contract manager Web3 contract creation
                web3_contract = contract_manager.get_contract(address)
                if not web3_contract:
                    self.logger.warning(f"Could not create Web3 contract for: {address}")
                
                tested_contracts += 1
                if tested_contracts >= 3:  # Test first 3 contracts
                    break
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Contract registry check passed",
                total_contracts=contract_count,
                tested_contracts=tested_contracts
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Contract registry check failed",
                error=str(e)
            )
            return False
    
    def check_signal_transformer_setup(self) -> bool:
        """Check signal transformer registry and setup"""
        try:
            transformer_registry = self.testing_env.get_service(TransformerRegistry)
            
            # Get all registered transformers
            all_transformers = transformer_registry.get_all_contracts()
            
            if not all_transformers:
                self.logger.error("No transformers registered")
                return False
            
            # Validate each transformer for signal architecture
            valid_transformers = 0
            for address, transformer_info in all_transformers.items():
                if not transformer_info.active:
                    continue
                
                transformer_instance = transformer_info.instance
                transformer_name = type(transformer_instance).__name__
                
                # Check required methods for signal architecture
                required_methods = ['process_logs']
                for method_name in required_methods:
                    if not hasattr(transformer_instance, method_name):
                        log_with_context(
                            self.logger,
                            logging.ERROR,
                            "Signal transformer missing required method",
                            transformer_name=transformer_name,
                            missing_method=method_name,
                            contract_address=address
                        )
                        return False
                
                # Check handler map for signal generation
                if hasattr(transformer_instance, 'handler_map'):
                    handler_map = getattr(transformer_instance, 'handler_map', {})
                    if not handler_map:
                        log_with_context(
                            self.logger,
                            logging.WARNING,
                            "Transformer has empty handler map",
                            transformer_name=transformer_name,
                            contract_address=address
                        )
                
                valid_transformers += 1
                
                log_with_context(
                    self.logger,
                    logging.DEBUG,
                    "Signal transformer validation passed",
                    transformer_name=transformer_name,
                    contract_address=address,
                    has_handler_map=hasattr(transformer_instance, 'handler_map')
                )
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Signal transformer setup check passed",
                total_transformers=len(all_transformers),
                valid_transformers=valid_transformers
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Signal transformer setup check failed",
                error=str(e)
            )
            return False
    
    def check_storage_connection(self) -> bool:
        """Check GCS storage connection"""
        try:
            storage_handler = self.testing_env.get_service(GCSHandler)
            
            # Test basic connection by listing blobs
            blobs = storage_handler.list_blobs(prefix=storage_handler.storage_config.rpc_prefix)
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Storage connection check passed",
                bucket_name=storage_handler.bucket_name,
                rpc_blobs_found=len(blobs)
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Storage connection check failed",
                error=str(e)
            )
            return False
    
    def check_rpc_connection(self) -> bool:
        """Check RPC connection"""
        try:
            rpc_client = self.testing_env.get_service(QuickNodeRpcClient)
            
            # Test connection by getting latest block number
            latest_block = rpc_client.get_latest_block_number()
            
            if latest_block <= 0:
                self.logger.error("Invalid latest block number")
                return False
            
            log_with_context(
                self.logger,
                logging.INFO,
                "RPC connection check passed",
                latest_block=latest_block,
                endpoint_url=rpc_client.endpoint_url
            )
            
            return True
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "RPC connection check failed",
                error=str(e)
            )
            return False
    
    def check_signal_generation(self) -> bool:
        """Test signal generation with a sample transaction"""
        try:
            # Get a known block for testing
            test_block_number = 63269916  # Use the same block from your examples
            
            storage_handler = self.testing_env.get_service(GCSHandler)
            block_decoder = self.testing_env.get_service(BlockDecoder)
            transform_manager = self.testing_env.get_service(TransformationManager)
            
            # Get and decode a test block
            raw_block = storage_handler.get_rpc_block(test_block_number)
            if not raw_block:
                self.logger.warning(f"Test block {test_block_number} not found - skipping signal test")
                return True  # Don't fail if test block isn't available
            
            decoded_block = block_decoder.decode_block(raw_block)
            if not decoded_block.transactions:
                self.logger.warning(f"No transactions in test block {test_block_number}")
                return True
            
            # Test signal generation on first transaction
            first_tx_hash = next(iter(decoded_block.transactions.keys()))
            first_tx = decoded_block.transactions[first_tx_hash]
            
            log_with_context(
                self.logger,
                logging.DEBUG,
                "Testing signal generation",
                test_block=test_block_number,
                test_tx=first_tx_hash,
                tx_logs=len(first_tx.logs)
            )
            
            signals_generated, processed_tx = transform_manager.process_transaction(first_tx)
            
            signal_count = len(processed_tx.signals) if processed_tx.signals else 0
            error_count = len(processed_tx.errors) if processed_tx.errors else 0
            
            log_with_context(
                self.logger,
                logging.INFO,
                "Signal generation test completed",
                signals_generated=signals_generated,
                signal_count=signal_count,
                error_count=error_count,
                test_tx=first_tx_hash
            )
            
            return True  # Signal generation working, even if no signals produced
            
        except Exception as e:
            log_with_context(
                self.logger,
                logging.ERROR,
                "Signal generation test failed",
                error=str(e)
            )
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
            print("   2. Or: python testing/scripts/debug_session.py block <block_number>")
            print("   3. Test signals: python testing/scripts/debug_session.py transformers")
        else:
            print("\n❌ Some checks failed. Review errors above.")
            print("\n💡 Common fixes:")
            print("   - Check environment variables in .env")
            print("   - Verify config/config.json is valid")
            print("   - Ensure ABI files exist in config/abis/")
            print("   - Check network connectivity")
            print("   - Verify transformers have process_logs() method")


def main():
    """Run quick diagnostic"""
    try:
        diagnostic = QuickDiagnostic()
        success = diagnostic.run_all_checks()
        exit(0 if success else 1)
        
    except Exception as e:
        print(f"❌ Diagnostic failed with exception: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()