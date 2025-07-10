#!/usr/bin/env python3
# testing/diagnostics/pipeline_diagnostic.py

"""
Pipeline Component Diagnostic

Verifies that all pipeline components are properly configured and can communicate.
"""

import sys
from pathlib import Path
from typing import List, Tuple, Optional

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler
from indexer.clients.quicknode_rpc import QuickNodeRpcClient
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager
from indexer.transform.registry import TransformRegistry
from indexer.contracts.registry import ContractRegistry


class PipelineDiagnostic:
    """Check pipeline component health."""
    
    def __init__(self, model_name: str = None):
        self.model_name = model_name
        self.results: List[Tuple[str, bool, str]] = []
        
    def run(self) -> bool:
        """Run all pipeline checks."""
        print("🔄 Pipeline Component Diagnostic")
        print("=" * 60)
        
        # Initialize environment
        try:
            self.env = get_testing_environment(model_name=self.model_name)
            self.config = self.env.get_config()
        except Exception as e:
            print(f"❌ Failed to initialize environment: {e}")
            return False
        
        # Test each pipeline component
        self._test_gcs_storage()
        self._test_rpc_client()
        self._test_block_decoder()
        self._test_transform_system()
        
        # Print results
        self._print_results()
        
        return all(result[1] for result in self.results)
    
    def _test_gcs_storage(self):
        """Test GCS storage handler."""
        print("\n☁️ Testing GCS Storage...")
        
        try:
            gcs = self.env.get_service(GCSHandler)
            
            # Check bucket access
            try:
                bucket = gcs.bucket
                self.results.append((
                    f"GCS Bucket ({bucket.name})", 
                    True, 
                    "Accessible"
                ))
            except Exception as e:
                self.results.append(("GCS Bucket", False, str(e)))
                return
            
            # Check if we can list blobs
            try:
                # Try to list a few blobs
                blobs = list(bucket.list_blobs(max_results=1))
                if blobs:
                    self.results.append(("GCS List Objects", True, "Can list objects"))
                else:
                    self.results.append(("GCS List Objects", True, "Bucket empty"))
            except Exception as e:
                self.results.append(("GCS List Objects", False, str(e)))
                
        except Exception as e:
            self.results.append(("GCS Storage", False, str(e)))
    
    def _test_rpc_client(self):
        """Test RPC client connectivity."""
        print("\n🌐 Testing RPC Client...")
        
        try:
            rpc = self.env.get_service(QuickNodeRpcClient)
            
            # Test basic connectivity
            try:
                # Get latest block number
                latest_block = rpc.get_latest_block_number()
                self.results.append((
                    "RPC Connection", 
                    True, 
                    f"Latest block: {latest_block:,}"
                ))
                
                # Test we can fetch a block
                block = rpc.get_block(latest_block, False)
                if block:
                    self.results.append(("RPC Block Fetch", True, "Can fetch blocks"))
                else:
                    self.results.append(("RPC Block Fetch", False, "No block returned"))
                    
            except Exception as e:
                self.results.append(("RPC Connection", False, str(e)))
                
        except Exception as e:
            self.results.append(("RPC Client", False, str(e)))
    
    def _test_block_decoder(self):
        """Test block decoder setup."""
        print("\n🔍 Testing Block Decoder...")
        
        try:
            decoder = self.env.get_service(BlockDecoder)
            contract_registry = self.env.get_service(ContractRegistry)
            
            # Check if contracts are loaded
            contract_count = len(contract_registry.contracts)
            if contract_count > 0:
                self.results.append((
                    "Contract Registry", 
                    True, 
                    f"{contract_count} contracts loaded"
                ))
            else:
                self.results.append((
                    "Contract Registry", 
                    False, 
                    "No contracts loaded"
                ))
            
            # Check ABI availability
            contracts_with_abi = sum(
                1 for c in contract_registry.contracts.values() 
                if c.abi is not None
            )
            
            if contracts_with_abi > 0:
                self.results.append((
                    "Contract ABIs", 
                    True, 
                    f"{contracts_with_abi}/{contract_count} have ABIs"
                ))
            else:
                self.results.append((
                    "Contract ABIs", 
                    False, 
                    "No contracts have ABIs loaded"
                ))
                
        except Exception as e:
            self.results.append(("Block Decoder", False, str(e)))
    
    def _test_transform_system(self):
        """Test transform system setup."""
        print("\n🔄 Testing Transform System...")
        
        try:
            transform_manager = self.env.get_service(TransformManager)
            transform_registry = self.env.get_service(TransformRegistry)
            
            # Check transformer count
            transformer_count = len(transform_registry._transformers)
            if transformer_count > 0:
                self.results.append((
                    "Transform Registry", 
                    True, 
                    f"{transformer_count} transformers loaded"
                ))
                
                # List transformer types
                transformer_types = list(transform_registry._transformers.keys())
                for t_type in transformer_types[:3]:  # Show first 3
                    self.results.append((
                        f"  → {t_type}", 
                        True,
                        "Registered"
                    ))
            else:
                self.results.append((
                    "Transform Registry", 
                    False, 
                    "No transformers loaded"
                ))
                
        except Exception as e:
            self.results.append(("Transform System", False, str(e)))
    
    def _print_results(self):
        """Print diagnostic results."""
        print("\n📊 Results:")
        print("-" * 60)
        
        for check, success, info in self.results:
            status = "✅" if success else "❌"
            print(f"{status} {check:<30} {info}")
        
        # Summary
        total = len(self.results)
        passed = sum(1 for _, success, _ in self.results if success)
        
        print("-" * 60)
        print(f"Total: {passed}/{total} pipeline checks passed")
        
        if passed < total:
            print("\n💡 Common fixes:")
            print("  - Check GCS credentials and bucket permissions")
            print("  - Verify RPC endpoint URL and authentication")
            print("  - Ensure contracts are configured in database")
            print("  - Check that transformer modules are installed")


def main():
    """Run pipeline diagnostic."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Pipeline Component Diagnostic')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        diagnostic = PipelineDiagnostic(model_name=args.model)
        success = diagnostic.run()
        
        if success:
            print("\n✅ All pipeline checks passed!")
        else:
            print("\n❌ Some pipeline checks failed")
            
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\n💥 Diagnostic failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()