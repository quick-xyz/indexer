#!/usr/bin/env python3
"""
Enhanced Debug Session for Blockchain Indexer

Clean console output with comprehensive file analysis.
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


class EnhancedDebugSession:
    """Interactive debugging session with clean output and comprehensive file analysis"""
    
    def __init__(self, config_path: str = None):
        self.testing_env = get_testing_environment(config_path, log_level="ERROR")
        self.logger = self.testing_env.get_logger("debug.session")
        
        # Create output directory
        self.output_dir = PROJECT_ROOT / "debug_output"
        self.output_dir.mkdir(exist_ok=True)
        
        # Get services
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformManager)
        self.transformer_registry = self.testing_env.get_service(TransformRegistry)
    
    def analyze_transaction_to_file(self, tx_hash: str, block_number: int) -> str:
        """Deep analysis of a specific transaction with clean console output"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / f"transaction_analysis_{tx_hash[:10]}_{timestamp}.json"
        
        print(f"🔬 Analyzing transaction {tx_hash[:10]}...")
        
        analysis = {
            "metadata": {
                "tx_hash": tx_hash,
                "block_number": block_number,
                "analysis_time": datetime.now().isoformat(),
                "architecture": "signal-based",
                "indexer": {
                    "name": self.testing_env.config.name,
                    "version": self.testing_env.config.version
                }
            },
            "transaction": {},
            "signal_generation": {},
            "transformer_analysis": {},
            "errors": [],
            "summary": {},
            "recommendations": []
        }
        
        try:
            # Get transaction from block
            raw_block = self.storage_handler.get_rpc_block(block_number)
            if not raw_block:
                analysis["errors"].append(f"Block {block_number} not found")
                self._save_analysis(output_file, analysis)
                print(f"❌ Block {block_number} not found")
                return str(output_file)
            
            decoded_block = self.block_decoder.decode_block(raw_block)
            if not decoded_block.transactions or tx_hash not in decoded_block.transactions:
                analysis["errors"].append(f"Transaction {tx_hash} not found in block {block_number}")
                self._save_analysis(output_file, analysis)
                print(f"❌ Transaction not found in block")
                return str(output_file)
            
            transaction = decoded_block.transactions[tx_hash]
            
            # Analyze transaction structure
            analysis["transaction"] = self._analyze_transaction_structure(transaction)
            
            # Analyze transformers and their compatibility with logs
            analysis["transformer_analysis"] = self._analyze_transformer_compatibility(transaction)
            
            # Run signal generation and capture detailed results
            analysis["signal_generation"] = self._analyze_signal_generation(transaction)
            
            # Generate summary and recommendations
            analysis["summary"] = self._generate_analysis_summary(analysis)
            analysis["recommendations"] = self._generate_recommendations(analysis)
            
        except Exception as e:
            analysis["errors"].append({
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc()
            })
            print(f"❌ Analysis failed: {e}")
        
        # Save analysis to file
        self._save_analysis(output_file, analysis)
        
        # Print clean console summary
        self._print_analysis_summary(analysis)
        
        print(f"\n📄 Full analysis saved: {output_file}")
        
        return str(output_file)
    
    def _analyze_transaction_structure(self, transaction) -> dict:
        """Analyze basic transaction structure"""
        decoded_logs = {}
        encoded_logs = {}
        
        for log_idx, log in transaction.logs.items():
            if hasattr(log, 'name'):
                # Decoded log
                transformer = self.transformer_registry.get_transformer(log.contract)
                transformer_info = {
                    "exists": transformer is not None,
                    "name": type(transformer).__name__ if transformer else None,
                    "has_process_logs": hasattr(transformer, 'process_logs') if transformer else False,
                    "handler_available": False
                }
                
                if transformer and hasattr(transformer, 'handler_map'):
                    transformer_info["handler_available"] = log.name in transformer.handler_map
                
                decoded_logs[str(log_idx)] = {
                    "type": "decoded",
                    "name": log.name,
                    "contract": log.contract,
                    "attributes": dict(log.attributes),
                    "transformer": transformer_info
                }
            else:
                # Encoded log
                encoded_logs[str(log_idx)] = {
                    "type": "encoded",
                    "contract": log.contract,
                    "signature": log.signature,
                    "topics_count": len(log.topics) if log.topics else 0
                }
        
        return {
            "hash": transaction.tx_hash,
            "success": transaction.tx_success,
            "from": transaction.origin_from,
            "to": transaction.origin_to,
            "value": transaction.value,
            "total_logs": len(transaction.logs),
            "decoded_logs": len(decoded_logs),
            "encoded_logs": len(encoded_logs),
            "logs": {**decoded_logs, **encoded_logs}
        }
    
    def _analyze_transformer_compatibility(self, transaction) -> dict:
        """Analyze transformer compatibility with log attributes"""
        compatibility_analysis = {}
        
        for log_idx, log in transaction.logs.items():
            if not hasattr(log, 'name'):
                continue
                
            transformer = self.transformer_registry.get_transformer(log.contract)
            if not transformer:
                continue
            
            transformer_name = type(transformer).__name__
            
            # Analyze attribute compatibility
            expected_attrs = self._get_expected_attributes(transformer_name, log.name)
            actual_attrs = set(log.attributes.keys())
            
            compatibility_analysis[f"{transformer_name}_{log_idx}"] = {
                "transformer": transformer_name,
                "log_name": log.name,
                "log_index": log_idx,
                "expected_attributes": expected_attrs,
                "actual_attributes": list(actual_attrs),
                "missing_attributes": list(set(expected_attrs) - actual_attrs) if expected_attrs else [],
                "extra_attributes": list(actual_attrs - set(expected_attrs)) if expected_attrs else [],
                "compatibility_score": self._calculate_compatibility_score(expected_attrs, actual_attrs)
            }
        
        return compatibility_analysis
    
    def _get_expected_attributes(self, transformer_name: str, log_name: str) -> list:
        """Get expected attributes for transformer/log combinations"""
        attribute_map = {
            "WavaxTransformer": {
                "Transfer": ["from", "to", "value"]  # But WAVAX actually uses src, dst, wad
            },
            "TokenTransformer": {
                "Transfer": ["from", "to", "value"]
            },
            "LfjPoolTransformer": {
                "Swap": ["sender", "to", "amount0In", "amount1In", "amount0Out", "amount1Out"],
                "Sync": ["reserve0", "reserve1"]
            },
            "OdosAggregatorTransformer": {
                "Swap": ["sender", "inputAmount", "inputToken", "amountOut", "outputToken"]
            },
            "LfjAggregatorTransformer": {
                "SwapExactIn": ["sender", "to", "tokenIn", "tokenOut", "amountIn", "amountOut"]
            }
        }
        
        return attribute_map.get(transformer_name, {}).get(log_name, [])
    
    def _calculate_compatibility_score(self, expected: list, actual: set) -> float:
        """Calculate compatibility score between expected and actual attributes"""
        if not expected:
            return 1.0  # No expectations means compatible
        
        expected_set = set(expected)
        intersection = expected_set.intersection(actual)
        return len(intersection) / len(expected_set)
    
    def _analyze_signal_generation(self, transaction) -> dict:
        """Analyze signal generation with detailed error tracking"""
        signal_analysis = {
            "signals_generated": False,
            "signals": {},
            "errors": {},
            "transformer_results": {}
        }
        
        try:
            signals_generated, processed_tx = self.transform_manager.process_transaction(transaction)
            signal_analysis["signals_generated"] = signals_generated
            
            # Analyze signals
            if processed_tx.signals:
                for signal_idx, signal in processed_tx.signals.items():
                    signal_analysis["signals"][str(signal_idx)] = {
                        "type": type(signal).__name__,
                        "log_index": signal.log_index,
                        "pattern": signal.pattern,
                        "signal_data": self._serialize_signal(signal)
                    }
            
            # Analyze errors with categorization
            if processed_tx.errors:
                for error_id, error in processed_tx.errors.items():
                    signal_analysis["errors"][error_id] = {
                        "error_type": error.error_type,
                        "message": error.message,
                        "stage": error.stage,
                        "context": error.context
                    }
            
        except Exception as e:
            signal_analysis["exception"] = {
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc()
            }
        
        return signal_analysis
    
    def _generate_analysis_summary(self, analysis) -> dict:
        """Generate comprehensive analysis summary"""
        tx_data = analysis.get("transaction", {})
        signal_data = analysis.get("signal_generation", {})
        transformer_data = analysis.get("transformer_analysis", {})
        
        # Compatibility analysis
        compatibility_issues = []
        for comp_key, comp_data in transformer_data.items():
            if comp_data.get("compatibility_score", 1.0) < 1.0:
                compatibility_issues.append({
                    "transformer": comp_data["transformer"],
                    "log_index": comp_data["log_index"],
                    "log_name": comp_data["log_name"],
                    "score": comp_data["compatibility_score"],
                    "missing": comp_data["missing_attributes"]
                })
        
        # Error categorization
        error_breakdown = {}
        for error_data in signal_data.get("errors", {}).values():
            error_type = error_data["error_type"]
            error_breakdown[error_type] = error_breakdown.get(error_type, 0) + 1
        
        return {
            "total_logs": tx_data.get("total_logs", 0),
            "decoded_logs": tx_data.get("decoded_logs", 0),
            "signals_generated": len(signal_data.get("signals", {})),
            "generation_errors": len(signal_data.get("errors", {})),
            "has_exception": "exception" in signal_data,
            "compatibility_issues_count": len(compatibility_issues),
            "compatibility_issues": compatibility_issues,
            "error_breakdown": error_breakdown,
            "contracts_involved": list(set(
                log_data["contract"] for log_data in tx_data.get("logs", {}).values()
                if "contract" in log_data
            )),
            "transformers_available": list(set(
                log_data["transformer"]["name"] for log_data in tx_data.get("logs", {}).values()
                if log_data.get("transformer", {}).get("exists", False)
            ))
        }
    
    def _generate_recommendations(self, analysis) -> list:
        """Generate actionable recommendations"""
        recommendations = []
        
        transformer_data = analysis.get("transformer_analysis", {})
        signal_data = analysis.get("signal_generation", {})
        
        # Check for attribute compatibility issues
        wavax_issues = [comp for comp in transformer_data.values() 
                       if comp["transformer"] == "WavaxTransformer" and comp["compatibility_score"] < 1.0]
        
        if wavax_issues:
            recommendations.append({
                "priority": "HIGH",
                "category": "Transformer Fix",
                "issue": "WavaxTransformer attribute mismatch",
                "description": "WAVAX logs use 'src', 'dst', 'wad' but transformer expects 'from', 'to', 'value'",
                "solution": "Update WavaxTransformer._get_transfer_attributes() to use correct attribute names",
                "affected_logs": [issue["log_index"] for issue in wavax_issues]
            })
        
        # Check for transfer reconciliation error
        reconciliation_errors = [error for error in signal_data.get("errors", {}).values()
                               if "transfer_reconciliation" in error.get("message", "")]
        
        if reconciliation_errors:
            recommendations.append({
                "priority": "HIGH", 
                "category": "Code Bug",
                "issue": "Transfer reconciliation NoneType error",
                "description": "The _reconcile_transfers method has a bug with None.items()",
                "solution": "Fix the _reconcile_transfers method in TransformManager to handle None values",
                "error_message": reconciliation_errors[0].get("message", "")
            })
        
        # Check for missing transfer signals
        transfer_logs = [log for log in analysis["transaction"]["logs"].values() 
                        if log.get("name") == "Transfer"]
        transfer_signals = [signal for signal in signal_data.get("signals", {}).values()
                           if signal["type"] == "TransferSignal"]
        
        if len(transfer_logs) > len(transfer_signals):
            recommendations.append({
                "priority": "MEDIUM",
                "category": "Signal Generation",
                "issue": "Missing transfer signals",
                "description": f"Found {len(transfer_logs)} transfer logs but only {len(transfer_signals)} transfer signals",
                "solution": "Fix transformer validation to allow transfer signal generation"
            })
        
        return recommendations
    
    def _print_analysis_summary(self, analysis):
        """Print clean console summary"""
        summary = analysis["summary"]
        recommendations = analysis["recommendations"]
        
        print(f"✅ Analysis completed")
        print(f"   📊 Logs: {summary['decoded_logs']}/{summary['total_logs']} decoded")
        print(f"   🔄 Signals: {summary['signals_generated']} generated")
        print(f"   🚨 Errors: {summary['generation_errors']} found")
        
        if summary.get("compatibility_issues_count", 0) > 0:
            print(f"   ⚠️  Compatibility issues: {summary['compatibility_issues_count']}")
        
        if summary.get("error_breakdown"):
            error_summary = ", ".join(f"{k}({v})" for k, v in summary["error_breakdown"].items())
            print(f"   🔧 Error types: {error_summary}")
        
        # Print high priority recommendations
        high_priority = [r for r in recommendations if r.get("priority") == "HIGH"]
        if high_priority:
            print(f"\n🎯 Priority fixes needed:")
            for rec in high_priority[:2]:  # Show top 2
                print(f"   • {rec['issue']}: {rec['description']}")
    
    def _serialize_signal(self, signal) -> dict:
        """Serialize signal object to dictionary for JSON output"""
        try:
            signal_dict = {}
            for attr_name in dir(signal):
                if not attr_name.startswith('_') and not callable(getattr(signal, attr_name)):
                    try:
                        value = getattr(signal, attr_name)
                        signal_dict[attr_name] = str(value) if value is not None else None
                    except:
                        signal_dict[attr_name] = "serialization_error"
            return signal_dict
        except Exception:
            return {"error": "failed_to_serialize_signal"}
    
    def _save_analysis(self, output_file: Path, analysis: dict):
        """Save analysis to JSON file with pretty formatting"""
        try:
            with open(output_file, 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
        except Exception as e:
            print(f"❌ Failed to save analysis: {e}")


def main():
    """Main debug session with clean output"""
    
    try:
        debug_session = EnhancedDebugSession()
        
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == "analyze" and len(sys.argv) > 3:
                tx_hash = sys.argv[2]
                block_number = int(sys.argv[3])
                output_file = debug_session.analyze_transaction_to_file(tx_hash, block_number)
                
            else:
                print("Usage:")
                print("  python testing/scripts/debug_session.py analyze <tx_hash> <block_number>")
                print("\nExample:")
                print("  python testing/scripts/debug_session.py analyze 0xab6908d3... 63269916")
        else:
            print("Enhanced debug session - provide command to run analysis")
            
    except Exception as e:
        print(f"❌ Debug session failed: {e}")


if __name__ == "__main__":
    main()