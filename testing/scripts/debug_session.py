#!/usr/bin/env python3
"""
Modular Debug Session - Creates multiple focused debug files
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
    """Debug session that creates multiple focused files instead of one large file"""
    
    def __init__(self, config_path: str = None):
        self.testing_env = get_testing_environment(config_path, log_level="ERROR")
        
        # Create timestamped output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_dir = PROJECT_ROOT / "debug_output" / f"session_{timestamp}"
        self.session_dir.mkdir(parents=True, exist_ok=True)
        
        # Get services
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformManager)
        self.transformer_registry = self.testing_env.get_service(TransformRegistry)
    
    def analyze_transaction_modular(self, tx_hash: str, block_number: int) -> str:
        """Create multiple focused debug files for a transaction"""
        
        print(f"🔬 Analyzing transaction {tx_hash[:10]}... (modular output)")
        
        try:
            # Get transaction
            raw_block = self.storage_handler.get_rpc_block(block_number)
            if not raw_block:
                print(f"❌ Block {block_number} not found")
                return str(self.session_dir)
            
            decoded_block = self.block_decoder.decode_block(raw_block)
            if not decoded_block.transactions or tx_hash not in decoded_block.transactions:
                print(f"❌ Transaction not found in block")
                return str(self.session_dir)
            
            transaction = decoded_block.transactions[tx_hash]
            
            # Create modular files
            self._create_summary_file(tx_hash, block_number, transaction)
            self._create_logs_file(tx_hash, transaction)
            self._create_transformers_file(tx_hash, transaction)
            self._create_signals_file(tx_hash, transaction)
            self._create_errors_file(tx_hash, transaction)
            self._create_recommendations_file(tx_hash, transaction)
            
            print(f"✅ Analysis completed - 6 files created")
            print(f"📁 Session directory: {self.session_dir}")
            
            return str(self.session_dir)
            
        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            error_file = self.session_dir / "error.json"
            with open(error_file, 'w') as f:
                json.dump({
                    "error": str(e),
                    "traceback": traceback.format_exc()
                }, f, indent=2)
            return str(self.session_dir)
    
    def _create_summary_file(self, tx_hash: str, block_number: int, transaction):
        """Create high-level summary file"""
        
        decoded_logs = sum(1 for log in transaction.logs.values() if hasattr(log, 'name'))
        
        # Quick signal generation test
        try:
            signals_generated, processed_tx = self.transform_manager.process_transaction(transaction)
            signal_count = len(processed_tx.signals) if processed_tx.signals else 0
            error_count = len(processed_tx.errors) if processed_tx.errors else 0
            event_count = len(processed_tx.events) if processed_tx.events else 0
        except Exception as e:
            signal_count = 0
            error_count = 1
            event_count = 0
        
        summary = {
            "transaction_hash": tx_hash,
            "block_number": block_number,
            "analysis_time": datetime.now().isoformat(),
            "basic_info": {
                "success": transaction.tx_success,
                "from": transaction.origin_from,
                "to": transaction.origin_to,
                "value": transaction.value
            },
            "processing_summary": {
                "total_logs": len(transaction.logs),
                "decoded_logs": decoded_logs,
                "signals_generated": signal_count,
                "events_generated": event_count,
                "errors_found": error_count
            },
            "status": "SUCCESS" if error_count == 0 else "HAS_ERRORS"
        }
        
        with open(self.session_dir / "1_summary.json", 'w') as f:
            json.dump(summary, f, indent=2)
    
    def _create_logs_file(self, tx_hash: str, transaction):
        """Create detailed logs analysis file"""
        
        logs_analysis = {
            "transaction_hash": tx_hash,
            "total_logs": len(transaction.logs),
            "logs_by_type": {"decoded": {}, "encoded": {}},
            "contracts_involved": set(),
            "log_names_found": set()
        }
        
        for log_idx, log in transaction.logs.items():
            logs_analysis["contracts_involved"].add(log.contract)
            
            if hasattr(log, 'name'):
                # Decoded log
                logs_analysis["log_names_found"].add(log.name)
                logs_analysis["logs_by_type"]["decoded"][str(log_idx)] = {
                    "name": log.name,
                    "contract": log.contract,
                    "attributes": dict(log.attributes),
                    "attribute_count": len(log.attributes)
                }
            else:
                # Encoded log
                logs_analysis["logs_by_type"]["encoded"][str(log_idx)] = {
                    "contract": log.contract,
                    "signature": log.signature,
                    "topics_count": len(log.topics) if log.topics else 0
                }
        
        # Convert sets to lists for JSON serialization
        logs_analysis["contracts_involved"] = list(logs_analysis["contracts_involved"])
        logs_analysis["log_names_found"] = list(logs_analysis["log_names_found"])
        
        with open(self.session_dir / "2_logs.json", 'w') as f:
            json.dump(logs_analysis, f, indent=2)
    
    def _create_transformers_file(self, tx_hash: str, transaction):
        """Create transformer compatibility analysis"""
        
        transformer_analysis = {
            "transaction_hash": tx_hash,
            "transformer_registry_summary": self._get_safe_registry_summary(),
            "log_transformer_mapping": {},
            "compatibility_issues": []
        }
        
        for log_idx, log in transaction.logs.items():
            if not hasattr(log, 'name'):
                continue
            
            transformer = self.transformer_registry.get_transformer(log.contract)
            
            mapping_info = {
                "log_index": log_idx,
                "log_name": log.name,
                "contract": log.contract,
                "transformer_found": transformer is not None,
                "transformer_name": type(transformer).__name__ if transformer else None,
                "has_handler": False,
                "available_attributes": list(log.attributes.keys())
            }
            
            if transformer and hasattr(transformer, 'handler_map'):
                mapping_info["has_handler"] = log.name in transformer.handler_map
                mapping_info["available_handlers"] = list(transformer.handler_map.keys())
            
            transformer_analysis["log_transformer_mapping"][str(log_idx)] = mapping_info
            
            # Check for issues
            if transformer is None:
                transformer_analysis["compatibility_issues"].append({
                    "log_index": log_idx,
                    "issue": "no_transformer",
                    "description": f"No transformer found for contract {log.contract}"
                })
            elif not mapping_info["has_handler"]:
                transformer_analysis["compatibility_issues"].append({
                    "log_index": log_idx,
                    "issue": "no_handler",
                    "description": f"Transformer {mapping_info['transformer_name']} has no handler for '{log.name}'"
                })
        
        with open(self.session_dir / "3_transformers.json", 'w') as f:
            json.dump(transformer_analysis, f, indent=2)

    def _get_safe_registry_summary(self):
        """Safely get registry summary, handling missing methods"""
        try:
            # Try the method if it exists
            if hasattr(self.transformer_registry, 'get_setup_summary'):
                return self.transformer_registry.get_setup_summary()
            else:
                # Fallback to manual summary
                all_transformers = self.transformer_registry.get_all_contracts()
                return {
                    "total_contracts": len(all_transformers),
                    "active_transformers": sum(1 for t in all_transformers.values() if t.active),
                    "method_note": "get_setup_summary method not available, using fallback"
                }
        except Exception as e:
            return {
                "error": f"Failed to get registry summary: {str(e)}",
                "fallback_used": True
            }
    
    def _create_signals_file(self, tx_hash: str, transaction):
        """Create signals generation analysis"""
        
        signals_analysis = {
            "transaction_hash": tx_hash,
            "signals": {},
            "signal_types": {},
            "generation_success": False
        }
        
        try:
            signals_generated, processed_tx = self.transform_manager.process_transaction(transaction)
            signals_analysis["generation_success"] = signals_generated
            
            if processed_tx.signals:
                for signal_idx, signal in processed_tx.signals.items():
                    signal_type = type(signal).__name__
                    signals_analysis["signal_types"][signal_type] = signals_analysis["signal_types"].get(signal_type, 0) + 1
                    
                    signals_analysis["signals"][str(signal_idx)] = {
                        "type": signal_type,
                        "log_index": signal.log_index,
                        "pattern": getattr(signal, 'pattern', 'unknown'),
                        "key_attributes": self._extract_key_signal_attributes(signal)
                    }
            
        except Exception as e:
            signals_analysis["exception"] = {
                "type": type(e).__name__,
                "message": str(e)
            }
        
        with open(self.session_dir / "4_signals.json", 'w') as f:
            json.dump(signals_analysis, f, indent=2)
    
    def _create_errors_file(self, tx_hash: str, transaction):
        """Create detailed errors analysis"""
        
        errors_analysis = {
            "transaction_hash": tx_hash,
            "errors": {},
            "error_summary": {},
            "transformer_errors": {}
        }
        
        try:
            _, processed_tx = self.transform_manager.process_transaction(transaction)
            
            if processed_tx.errors:
                for error_id, error in processed_tx.errors.items():
                    errors_analysis["errors"][error_id] = {
                        "error_type": error.error_type,
                        "message": error.message,
                        "stage": error.stage,
                        "context": error.context
                    }
                    
                    # Categorize by error type
                    error_type = error.error_type
                    errors_analysis["error_summary"][error_type] = errors_analysis["error_summary"].get(error_type, 0) + 1
                    
                    # Group by transformer
                    if error.context and 'transformer_name' in error.context:
                        transformer = error.context['transformer_name']
                        if transformer not in errors_analysis["transformer_errors"]:
                            errors_analysis["transformer_errors"][transformer] = []
                        errors_analysis["transformer_errors"][transformer].append({
                            "log_index": error.context.get('log_index'),
                            "error_type": error.error_type,
                            "message": error.message
                        })
            
        except Exception as e:
            errors_analysis["processing_exception"] = {
                "type": type(e).__name__,
                "message": str(e)
            }
        
        with open(self.session_dir / "5_errors.json", 'w') as f:
            json.dump(errors_analysis, f, indent=2)
    
    def _create_recommendations_file(self, tx_hash: str, transaction):
        """Create actionable recommendations"""
        
        recommendations = {
            "transaction_hash": tx_hash,
            "recommendations": [],
            "quick_fixes": [],
            "investigation_needed": []
        }
        
        # Load data from other files for analysis
        try:
            with open(self.session_dir / "3_transformers.json") as f:
                transformer_data = json.load(f)
            with open(self.session_dir / "5_errors.json") as f:
                error_data = json.load(f)
            
            # Generate recommendations based on findings
            self._analyze_and_recommend(recommendations, transformer_data, error_data)
            
        except Exception as e:
            recommendations["analysis_error"] = str(e)
        
        with open(self.session_dir / "6_recommendations.json", 'w') as f:
            json.dump(recommendations, f, indent=2)
    
    def _extract_key_signal_attributes(self, signal) -> dict:
        """Extract key attributes from signal for summary"""
        key_attrs = {}
        
        # Common signal attributes to extract
        for attr in ['pool', 'token', 'amount', 'base_amount', 'quote_amount', 'from_address', 'to_address', 
                    'contract', 'token_in', 'amount_in', 'token_out', 'amount_out', 'sender', 'to']:
            if hasattr(signal, attr):
                value = getattr(signal, attr)
                key_attrs[attr] = str(value) if value is not None else None
        
        return key_attrs
    
    def _analyze_and_recommend(self, recommendations, transformer_data, error_data):
        """Generate specific recommendations based on analysis"""
        
        # Define events that are intentionally not handled (domain-specific exclusions)
        INTENTIONALLY_EXCLUDED_EVENTS = {
            'Sync',  # Pool state monitoring - not needed for domain model
            'Approval',  # Token approvals - not part of transfer tracking
            # Add other events you intentionally exclude
        }
        
        # Check for missing transformers
        missing_transformers = [
            issue for issue in transformer_data.get("compatibility_issues", [])
            if issue["issue"] == "no_transformer"
        ]
        
        if missing_transformers:
            recommendations["recommendations"].append({
                "priority": "MEDIUM",
                "category": "Missing Transformers",
                "description": f"{len(missing_transformers)} contracts have no transformers",
                "action": "Add transformer configurations to config.json",
                "affected_contracts": [issue["description"] for issue in missing_transformers]
            })
        
        # Check for missing handlers (filter out intentionally excluded events)
        missing_handlers = [
            issue for issue in transformer_data.get("compatibility_issues", [])
            if issue["issue"] == "no_handler"
        ]
        
        # Separate intentionally excluded from actual missing handlers
        excluded_handlers = []
        actual_missing_handlers = []
        
        for issue in missing_handlers:
            # Extract event name from description like "Transformer X has no handler for 'EventName'"
            import re
            event_match = re.search(r"no handler for '(\w+)'", issue["description"])
            if event_match:
                event_name = event_match.group(1)
                if event_name in INTENTIONALLY_EXCLUDED_EVENTS:
                    excluded_handlers.append(issue)
                else:
                    actual_missing_handlers.append(issue)
            else:
                actual_missing_handlers.append(issue)  # Default to missing if can't parse
        
        # Only flag actual missing handlers as HIGH priority
        if actual_missing_handlers:
            recommendations["recommendations"].append({
                "priority": "HIGH",
                "category": "Missing Handlers",
                "description": f"{len(actual_missing_handlers)} logs have no handlers",
                "action": "Add missing event handlers to transformers",
                "details": [issue["description"] for issue in actual_missing_handlers]
            })
        
        # Add excluded handlers as informational (low priority)
        if excluded_handlers:
            recommendations["recommendations"].append({
                "priority": "LOW", 
                "category": "Intentionally Excluded Events",
                "description": f"{len(excluded_handlers)} events are intentionally not handled",
                "action": "No action needed - these events are excluded by design",
                "details": [issue["description"] for issue in excluded_handlers],
                "note": "These events are not part of the domain model and can be safely ignored"
            })
        
        # Analyze error patterns
        if error_data.get("transformer_errors"):
            for transformer, errors in error_data["transformer_errors"].items():
                if len(errors) > 2:  # Multiple errors from same transformer
                    recommendations["recommendations"].append({
                        "priority": "HIGH",
                        "category": "Transformer Issues", 
                        "description": f"{transformer} has {len(errors)} errors",
                        "action": f"Debug {transformer} attribute handling",
                        "error_types": list(set(e["error_type"] for e in errors))
                    })
        
        # Add quick fixes section
        if not actual_missing_handlers and not error_data.get("transformer_errors"):
            recommendations["quick_fixes"].append({
                "category": "Signal Generation",
                "description": "All core transformers are working correctly",
                "status": "SUCCESS"
            })
        
        # Add investigation guidance
        if error_data.get("processing_exception"):
            recommendations["investigation_needed"].append({
                "category": "Processing Pipeline",
                "description": "Check for null reference errors in processing logic",
                "suggested_files": ["context.py", "manager.py"],
                "priority": "HIGH"
            })


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
        "6_recommendations.json": "Actionable next steps"
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
        debug_session = ModularDebugSession()
        
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == "analyze" and len(sys.argv) > 3:
                tx_hash = sys.argv[2]
                block_number = int(sys.argv[3])
                session_dir = debug_session.analyze_transaction_modular(tx_hash, block_number)
                print_session_summary(Path(session_dir))
                
            else:
                print("Usage:")
                print("  python testing/scripts/debug_session.py analyze <tx_hash> <block_number>")
        else:
            print("Modular debug session - generates multiple focused files")
            
    except Exception as e:
        print(f"❌ Debug session failed: {e}")


if __name__ == "__main__":
    main()