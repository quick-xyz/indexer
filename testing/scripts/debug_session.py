#!/usr/bin/env python3
"""
Modular Debug Session - Creates multiple focused debug files
Updated to use create_indexer() entry point and proper DI system
"""

import sys
import json
import traceback
from pathlib import Path
from typing import Optional
from datetime import datetime

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager
from indexer.transform.registry import TransformRegistry


class ModularDebugSession:
    """Debug session using proper DI system that creates multiple focused files"""
    
    def __init__(self, model_name: str = None):
        # Use proper testing environment with DI
        self.testing_env = get_testing_environment(model_name=model_name, log_level="ERROR")
        
        # Create timestamped output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_dir = PROJECT_ROOT / "debug_output" / f"session_{timestamp}"
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        # Get services from DI container
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformManager)
        self.transformer_registry = self.testing_env.get_service(TransformRegistry)
        
        # Store config info for debug output
        self.config = self.testing_env.get_config()
    
    def analyze_transaction_modular(self, tx_hash: str, block_number: int) -> str:
        """Create multiple focused debug files for a transaction"""
        
        print(f"🔬 Analyzing transaction {tx_hash[:10]}... (modular output)")
        print(f"   Model: {self.config.model_name} v{self.config.model_version}")
        
        try:
            # Get transaction using primary source
            config = self.testing_env.get_config()
            primary_source = config.get_primary_source()
            
            if not primary_source:
                print(f"❌ No primary source configured for model: {config.model_name}")
                self._create_error_summary(tx_hash, block_number, Exception("No primary source configured"))
                return str(self.session_dir)
            
            print(f"   Using source: {primary_source.name}")
            
            raw_block = self.storage_handler.get_rpc_block(block_number, source=primary_source)
            if not raw_block:
                print(f"❌ Block {block_number} not found")
                self._create_error_summary(tx_hash, block_number, Exception(f"Block {block_number} not found"))
                return str(self.session_dir)
            
            print(f"   Raw block retrieved")
            
            decoded_block = self.block_decoder.decode_block(raw_block)
            if not decoded_block or not decoded_block.transactions:
                print(f"❌ Block decoding failed or no transactions")
                self._create_error_summary(tx_hash, block_number, Exception("Block decoding failed or no transactions"))
                return str(self.session_dir)
            
            if tx_hash not in decoded_block.transactions:
                print(f"❌ Transaction not found in block")
                available_txs = list(decoded_block.transactions.keys())[:3]
                self._create_error_summary(tx_hash, block_number, 
                    Exception(f"Transaction not found. Available: {available_txs}"))
                return str(self.session_dir)
            
            transaction = decoded_block.transactions[tx_hash]
            print(f"   Transaction found with {len(transaction.logs) if transaction.logs else 0} logs")
            
            decoded_block = self.block_decoder.decode_block(raw_block)
            if not decoded_block.transactions or tx_hash not in decoded_block.transactions:
                print(f"❌ Transaction not found in block")
                return str(self.session_dir)
            
            transaction = decoded_block.transactions[tx_hash]
            
            # Create modular files
            self._create_summary_file(tx_hash, block_number, transaction)
            self._create_logs_file(transaction)
            self._create_transformers_file()
            self._create_signals_file(transaction)
            self._create_errors_file(transaction)
            self._create_recommendations_file(transaction)
            
            print(f"✅ Debug files created in: {self.session_dir.name}")
            return str(self.session_dir)
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            self._create_error_summary(tx_hash, block_number, e)
            return str(self.session_dir)
    
    def _create_summary_file(self, tx_hash: str, block_number: int, transaction):
        """Create high-level transaction summary"""
        summary = {
            "transaction_hash": tx_hash,
            "block_number": block_number,
            "model_info": {
                "name": self.config.model_name,
                "version": self.config.model_version,
                "database": self.config.model_db_name,
                "contract_count": len(self.config.contracts) if self.config.contracts else 0,
                "sources_count": len(self.config.sources) if self.config.sources else 0
            },
            "transaction_overview": {
                "from_address": getattr(transaction, 'from_address', None),
                "to_address": getattr(transaction, 'to_address', None),
                "value": str(getattr(transaction, 'value', 0)),
                "gas_used": getattr(transaction, 'gas_used', None),
                "log_count": len(transaction.logs) if transaction.logs else 0,
                "status": getattr(transaction, 'status', None)
            },
            "processing_status": {
                "signals_generated": len(getattr(transaction, 'signals', [])),
                "events_created": len(getattr(transaction, 'events', [])),
                "errors_encountered": len(getattr(transaction, 'errors', []))
            },
            "timestamp": datetime.now().isoformat()
        }
        
        with open(self.session_dir / "1_summary.json", "w") as f:
            json.dump(summary, f, indent=2, default=str)
    
    def _create_logs_file(self, transaction):
        """Create detailed log structure analysis"""
        logs_data = {
            "log_count": len(transaction.logs) if transaction.logs else 0,
            "logs": []
        }
        
        if transaction.logs:
            for i, log in enumerate(transaction.logs):
                log_info = {
                    "index": i,
                    "address": getattr(log, 'address', None),
                    "topics": [t.hex() if hasattr(t, 'hex') else str(t) for t in getattr(log, 'topics', [])],
                    "topic_count": len(getattr(log, 'topics', [])),
                    "data": getattr(log, 'data', None)[:100] + "..." if getattr(log, 'data', None) and len(getattr(log, 'data', '')) > 100 else getattr(log, 'data', None),
                    "data_length": len(getattr(log, 'data', '')) if getattr(log, 'data', None) else 0
                }
                logs_data["logs"].append(log_info)
        
        with open(self.session_dir / "2_logs.json", "w") as f:
            json.dump(logs_data, f, indent=2, default=str)
    
    def _create_transformers_file(self):
        """Create transformer compatibility check"""
        transformers_data = {
            "registry_info": {
                "registered_transformers": [],
                "total_count": 0
            },
            "configuration": {
                "contracts": [],
                "sources": []
            }
        }
        
        # Get registered transformers
        try:
            registered = self.transformer_registry.get_all_transformers()
            transformers_data["registry_info"]["total_count"] = len(registered)
            
            for name, transformer in registered.items():
                transformers_data["registry_info"]["registered_transformers"].append({
                    "name": name,
                    "class": type(transformer).__name__,
                    "has_process_logs": hasattr(transformer, 'process_logs')
                })
        except Exception as e:
            transformers_data["registry_info"]["error"] = str(e)
        
        # Add contract configuration
        if self.config.contracts:
            for contract in self.config.contracts:
                transformers_data["configuration"]["contracts"].append({
                    "name": contract.name,
                    "address": contract.address,
                    "network": contract.network
                })
        
        # Add sources configuration
        if self.config.sources:
            for source in self.config.sources:
                transformers_data["configuration"]["sources"].append({
                    "name": source.name,
                    "path": source.path,
                    "format": source.format
                })
        
        with open(self.session_dir / "3_transformers.json", "w") as f:
            json.dump(transformers_data, f, indent=2, default=str)
    
    def _create_signals_file(self, transaction):
        """Create signal generation results"""
        signals_data = {
            "processing_attempted": False,
            "signals": [],
            "signal_count": 0,
            "processing_errors": []
        }
        
        try:
            # Process transaction to get signals
            success, processed_tx = self.transform_manager.process_transaction(transaction)
            signals_data["processing_attempted"] = True
            signals_data["success"] = success
            
            if processed_tx and hasattr(processed_tx, 'signals') and processed_tx.signals:
                signals_data["signal_count"] = len(processed_tx.signals)
                
                for signal in processed_tx.signals:
                    signal_info = {
                        "signal_type": type(signal).__name__,
                        "signal_data": str(signal)[:200] + "..." if len(str(signal)) > 200 else str(signal)
                    }
                    signals_data["signals"].append(signal_info)
            
            if processed_tx and hasattr(processed_tx, 'errors') and processed_tx.errors:
                for error in processed_tx.errors:
                    signals_data["processing_errors"].append(str(error))
                    
        except Exception as e:
            signals_data["processing_errors"].append(str(e))
        
        with open(self.session_dir / "4_signals.json", "w") as f:
            json.dump(signals_data, f, indent=2, default=str)
    
    def _create_errors_file(self, transaction):
        """Create detailed error breakdown"""
        errors_data = {
            "transaction_errors": [],
            "processing_errors": [],
            "system_errors": []
        }
        
        # Check for transaction-level errors
        if hasattr(transaction, 'errors') and transaction.errors:
            for error in transaction.errors:
                errors_data["transaction_errors"].append({
                    "error_type": type(error).__name__,
                    "message": str(error),
                    "traceback": traceback.format_exc() if hasattr(error, '__traceback__') else None
                })
        
        # Try to identify common issues
        try:
            # Check if logs are properly formatted
            if not transaction.logs:
                errors_data["system_errors"].append("No logs found in transaction")
            
            # Check transformer registry
            registered = self.transformer_registry.get_all_transformers()
            if not registered:
                errors_data["system_errors"].append("No transformers registered")
            
            # Check configuration
            if not self.config.contracts:
                errors_data["system_errors"].append("No contracts configured")
                
        except Exception as e:
            errors_data["system_errors"].append(f"Error during diagnostic: {str(e)}")
        
        with open(self.session_dir / "5_errors.json", "w") as f:
            json.dump(errors_data, f, indent=2, default=str)
    
    def _create_recommendations_file(self, transaction):
        """Create actionable next steps"""
        recommendations = {
            "immediate_actions": [],
            "investigation_needed": [],
            "configuration_checks": []
        }
        
        # Analyze transaction and suggest actions
        log_count = len(transaction.logs) if transaction.logs else 0
        
        if log_count == 0:
            recommendations["immediate_actions"].append({
                "action": "Check transaction status",
                "description": "Transaction has no logs - may have failed or be a simple transfer",
                "priority": "HIGH"
            })
        
        # Check transformer configuration
        try:
            registered = self.transformer_registry.get_all_transformers()
            if not registered:
                recommendations["configuration_checks"].append({
                    "check": "Transformer Registration",
                    "description": "No transformers are registered",
                    "suggested_fix": "Verify transformer modules are loading correctly"
                })
        except:
            pass
        
        # Check contract configuration
        if not self.config.contracts:
            recommendations["configuration_checks"].append({
                "check": "Contract Configuration",
                "description": "No contracts configured in database",
                "suggested_fix": "Add contracts to the model configuration"
            })
        
        # Check sources configuration
        if not self.config.sources:
            recommendations["configuration_checks"].append({
                "check": "Sources Configuration", 
                "description": "No sources configured in database",
                "suggested_fix": "Run sources migration: python scripts/migrate_sources.py"
            })
        
        with open(self.session_dir / "6_recommendations.json", "w") as f:
            json.dump(recommendations, f, indent=2, default=str)
    
    def _create_error_summary(self, tx_hash: str, block_number: int, error: Exception):
        """Create error summary when analysis fails"""
        error_summary = {
            "transaction_hash": tx_hash,
            "block_number": block_number,
            "model": self.config.model_name,
            "analysis_failed": True,
            "error": {
                "type": type(error).__name__,
                "message": str(error),
                "traceback": traceback.format_exc()
            },
            "timestamp": datetime.now().isoformat()
        }
        
        with open(self.session_dir / "error_summary.json", "w") as f:
            json.dump(error_summary, f, indent=2, default=str)


def print_session_summary(session_dir: Path):
    """Print summary of created files"""
    files = list(session_dir.glob("*.json"))
    
    print(f"\n📁 Debug files created in: {session_dir.name}")
    print("─" * 50)
    
    file_descriptions = {
        "1_summary.json": "High-level transaction overview",
        "2_logs.json": "Detailed log structure analysis", 
        "3_transformers.json": "Transformer compatibility check",
        "4_signals.json": "Signal generation results",
        "5_errors.json": "Detailed error breakdown",
        "6_recommendations.json": "Actionable next steps",
        "error_summary.json": "Analysis failure summary"
    }
    
    for file in sorted(files):
        desc = file_descriptions.get(file.name, "Additional debug data")
        size_kb = file.stat().st_size / 1024
        print(f"   {file.name:<25} {desc} ({size_kb:.1f}KB)")
    
    print(f"\n🔍 Quick analysis commands:")
    print(f"   cat {session_dir}/1_summary.json | jq")
    print(f"   cat {session_dir}/6_recommendations.json | jq .recommendations")


def main():
    """Main debug session with modular output"""
    
    try:
        if len(sys.argv) < 2:
            print("Usage:")
            print("  python testing/scripts/debug_session.py analyze <tx_hash> <block_number> [model_name]")
            print("\nExample:")
            print("  python testing/scripts/debug_session.py analyze 0x123... 12345678")
            print("  python testing/scripts/debug_session.py analyze 0x123... 12345678 blub_test")
            sys.exit(1)
        
        command = sys.argv[1]
        
        if command == "analyze" and len(sys.argv) >= 4:
            tx_hash = sys.argv[2]
            block_number = int(sys.argv[3])
            model_name = sys.argv[4] if len(sys.argv) > 4 else None
            
            print(f"🚀 Initializing debug session...")
            debug_session = ModularDebugSession(model_name=model_name)
            
            session_dir = debug_session.analyze_transaction_modular(tx_hash, block_number)
            print_session_summary(Path(session_dir))
            
        else:
            print("Invalid command. Use 'analyze' with transaction hash and block number.")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Debug session failed: {e}")
        if "--verbose" in sys.argv:
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()