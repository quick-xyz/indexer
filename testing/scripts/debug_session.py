# testing/scripts/debug_session.py
#!/usr/bin/env python3
"""
Enhanced Debug Session for Blockchain Indexer

Now generates structured output files for analysis and sharing.
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
from indexer.core.logging_config import log_with_context
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformationManager
from indexer.transform.registry import TransformerRegistry


class EnhancedDebugSession:
    """Interactive debugging session with file output capabilities"""
    
    def __init__(self, config_path: str = None):
        self.testing_env = get_testing_environment(config_path, log_level="DEBUG")
        self.logger = self.testing_env.get_logger("debug.session")
        
        # Create output directory
        self.output_dir = PROJECT_ROOT / "debug_output"
        self.output_dir.mkdir(exist_ok=True)
        
        # Get services
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformationManager)
        self.transformer_registry = self.testing_env.get_service(TransformerRegistry)
        
        print("🔧 ENHANCED BLOCKCHAIN INDEXER DEBUG SESSION")
        print("=" * 60)
        print(f"📁 Output directory: {self.output_dir}")
        print("Services loaded and ready for debugging")
        print()
    
    def analyze_transaction_to_file(self, tx_hash: str, block_number: int) -> str:
        """Deep analysis of a specific transaction with file output"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / f"transaction_analysis_{tx_hash[:10]}_{timestamp}.json"
        
        print(f"🔬 Analyzing transaction {tx_hash}")
        print(f"📄 Output file: {output_file}")
        print("-" * 60)
        
        analysis = {
            "metadata": {
                "tx_hash": tx_hash,
                "block_number": block_number,
                "analysis_time": datetime.now().isoformat(),
                "indexer": {
                    "name": self.testing_env.config.name,
                    "version": self.testing_env.config.version
                }
            },
            "transaction": {},
            "transformation": {},
            "errors": [],
            "summary": {}
        }
        
        try:
            # Get transaction from block
            raw_block = self.storage_handler.get_rpc_block(block_number)
            if not raw_block:
                analysis["errors"].append(f"Block {block_number} not found")
                self._save_analysis(output_file, analysis)
                return str(output_file)
            
            decoded_block = self.block_decoder.decode_block(raw_block)
            if not decoded_block.transactions or tx_hash not in decoded_block.transactions:
                analysis["errors"].append(f"Transaction {tx_hash} not found in block {block_number}")
                self._save_analysis(output_file, analysis)
                return str(output_file)
            
            transaction = decoded_block.transactions[tx_hash]
            
            # Analyze transaction structure
            analysis["transaction"] = {
                "hash": transaction.tx_hash,
                "success": transaction.tx_success,
                "from": transaction.origin_from,
                "to": transaction.origin_to,
                "value": transaction.value,
                "total_logs": len(transaction.logs),
                "decoded_logs": sum(1 for log in transaction.logs.values() if hasattr(log, 'name')),
                "logs": {}
            }
            
            # Analyze each log
            for log_idx, log in transaction.logs.items():
                if hasattr(log, 'name'):
                    # Decoded log
                    transformer = self.transformer_registry.get_transformer(log.contract)
                    transformer_info = {
                        "exists": transformer is not None,
                        "name": type(transformer).__name__ if transformer else None,
                        "transfer_priority": None,
                        "log_priority": None
                    }
                    
                    if transformer:
                        transformer_info["transfer_priority"] = self.transformer_registry.get_transfer_priority(log.contract, log.name)
                        transformer_info["log_priority"] = self.transformer_registry.get_log_priority(log.contract, log.name)
                    
                    analysis["transaction"]["logs"][str(log_idx)] = {
                        "type": "decoded",
                        "name": log.name,
                        "contract": log.contract,
                        "attributes": dict(log.attributes),
                        "transformer": transformer_info
                    }
                else:
                    # Encoded log
                    analysis["transaction"]["logs"][str(log_idx)] = {
                        "type": "encoded",
                        "contract": log.contract,
                        "signature": log.signature,
                        "topics_count": len(log.topics) if log.topics else 0
                    }
            
            # Run transformation and capture detailed results
            print("🔄 Running transformation...")
            
            try:
                processed, transformed_tx = self.transform_manager.process_transaction(transaction)
                
                # Capture transformation results
                analysis["transformation"] = {
                    "processed": processed,
                    "transfers": {},
                    "events": {},
                    "errors": {}
                }
                
                # Analyze transfers
                if transformed_tx.transfers:
                    for transfer_id, transfer in transformed_tx.transfers.items():
                        analysis["transformation"]["transfers"][transfer_id] = {
                            "type": type(transfer).__name__,
                            "token": transfer.token,
                            "amount": transfer.amount,
                            "from_address": transfer.from_address,
                            "to_address": transfer.to_address,
                            "transfer_type": getattr(transfer, 'transfer_type', 'transfer'),
                            "log_index": transfer.log_index
                        }
                
                # Analyze events
                if transformed_tx.events:
                    for event_id, event in transformed_tx.events.items():
                        analysis["transformation"]["events"][event_id] = {
                            "type": type(event).__name__,
                            "timestamp": event.timestamp,
                            "log_index": getattr(event, 'log_index', None)
                        }
                
                # Analyze errors
                if transformed_tx.errors:
                    for error_id, error in transformed_tx.errors.items():
                        analysis["transformation"]["errors"][error_id] = {
                            "error_type": error.error_type,
                            "message": error.message,
                            "stage": error.stage,
                            "context": error.context
                        }
                
            except Exception as e:
                analysis["transformation"]["exception"] = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }
                print(f"❌ Transformation exception: {e}")
            
            # Generate summary
            analysis["summary"] = {
                "total_logs": len(transaction.logs),
                "decoded_logs": len([log for log in transaction.logs.values() if hasattr(log, 'name')]),
                "transfers_created": len(analysis["transformation"].get("transfers", {})),
                "events_created": len(analysis["transformation"].get("events", {})),
                "transformation_errors": len(analysis["transformation"].get("errors", {})),
                "has_exception": "exception" in analysis["transformation"],
                "contracts_involved": list(set(log.contract for log in transaction.logs.values() if hasattr(log, 'contract'))),
                "transformers_used": list(set(
                    analysis["transaction"]["logs"][str(idx)]["transformer"]["name"]
                    for idx, log_data in analysis["transaction"]["logs"].items()
                    if log_data["type"] == "decoded" and log_data["transformer"]["exists"]
                ))
            }
            
        except Exception as e:
            analysis["errors"].append({
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc()
            })
        
        # Save analysis to file
        self._save_analysis(output_file, analysis)
        
        # Print summary
        print(f"\n📊 ANALYSIS SUMMARY:")
        print(f"   Total logs: {analysis['summary'].get('total_logs', 0)}")
        print(f"   Decoded logs: {analysis['summary'].get('decoded_logs', 0)}")
        print(f"   Transfers created: {analysis['summary'].get('transfers_created', 0)}")
        print(f"   Events created: {analysis['summary'].get('events_created', 0)}")
        print(f"   Transformation errors: {analysis['summary'].get('transformation_errors', 0)}")
        
        if analysis['summary'].get('has_exception'):
            print(f"   ❌ Has transformation exception")
        
        print(f"\n📄 Full analysis saved to: {output_file}")
        
        return str(output_file)
    
    def _save_analysis(self, output_file: Path, analysis: dict):
        """Save analysis to JSON file with pretty formatting"""
        try:
            with open(output_file, 'w') as f:
                json.dump(analysis, f, indent=2, default=str)
            print(f"✅ Analysis saved to {output_file}")
        except Exception as e:
            print(f"❌ Failed to save analysis: {e}")
    
    def quick_block_analysis_to_file(self, block_number: int) -> str:
        """Quick block analysis with file output"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / f"block_analysis_{block_number}_{timestamp}.json"
        
        print(f"🔍 Quick analysis for block {block_number}")
        print(f"📄 Output file: {output_file}")
        print("-" * 40)
        
        analysis = {
            "metadata": {
                "block_number": block_number,
                "analysis_time": datetime.now().isoformat()
            },
            "block": {},
            "transactions": {},
            "summary": {},
            "errors": []
        }
        
        try:
            # Get raw block
            raw_block = self.storage_handler.get_rpc_block(block_number)
            if not raw_block:
                analysis["errors"].append(f"Block {block_number} not found")
                self._save_analysis(output_file, analysis)
                return str(output_file)
            
            analysis["block"] = {
                "number": block_number,
                "transactions": len(raw_block.transactions),
                "receipts": len(raw_block.receipts)
            }
            
            # Decode block
            decoded_block = self.block_decoder.decode_block(raw_block)
            
            if decoded_block.transactions:
                for tx_hash, transaction in decoded_block.transactions.items():
                    total_logs = len(transaction.logs)
                    decoded_logs = sum(1 for log in transaction.logs.values() if hasattr(log, 'name'))
                    
                    analysis["transactions"][tx_hash] = {
                        "success": transaction.tx_success,
                        "from": transaction.origin_from,
                        "to": transaction.origin_to,
                        "total_logs": total_logs,
                        "decoded_logs": decoded_logs,
                        "contracts": list(set(log.contract for log in transaction.logs.values() if hasattr(log, 'contract')))
                    }
            
            analysis["summary"] = {
                "total_transactions": len(decoded_block.transactions) if decoded_block.transactions else 0,
                "successful_transactions": sum(1 for tx in decoded_block.transactions.values() if tx.tx_success) if decoded_block.transactions else 0,
                "total_logs": sum(len(tx.logs) for tx in decoded_block.transactions.values()) if decoded_block.transactions else 0,
                "decoded_logs": sum(
                    sum(1 for log in tx.logs.values() if hasattr(log, 'name'))
                    for tx in decoded_block.transactions.values()
                ) if decoded_block.transactions else 0
            }
            
        except Exception as e:
            analysis["errors"].append({
                "type": type(e).__name__,
                "message": str(e),
                "traceback": traceback.format_exc()
            })
        
        self._save_analysis(output_file, analysis)
        
        print(f"📊 Block Summary:")
        print(f"   Transactions: {analysis['summary'].get('total_transactions', 0)}")
        print(f"   Total logs: {analysis['summary'].get('total_logs', 0)}")
        print(f"   Decoded logs: {analysis['summary'].get('decoded_logs', 0)}")
        print(f"\n📄 Full analysis saved to: {output_file}")
        
        return str(output_file)
    
    def transformer_performance_report(self) -> str:
        """Generate transformer performance report"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / f"transformer_report_{timestamp}.json"
        
        print(f"⚡ Generating transformer performance report")
        print(f"📄 Output file: {output_file}")
        print("-" * 50)
        
        report = {
            "metadata": {
                "report_time": datetime.now().isoformat(),
                "indexer": {
                    "name": self.testing_env.config.name,
                    "version": self.testing_env.config.version
                }
            },
            "transformers": {},
            "summary": {}
        }
        
        all_transformers = self.transformer_registry.get_all_contracts()
        
        for address, transformer_config in all_transformers.items():
            transformer_name = type(transformer_config.instance).__name__
            
            report["transformers"][address] = {
                "name": transformer_name,
                "active": transformer_config.active,
                "transfer_events": dict(transformer_config.transfer_priorities),
                "log_events": dict(transformer_config.log_priorities),
                "methods": {
                    "process_transfers": hasattr(transformer_config.instance, 'process_transfers'),
                    "process_logs": hasattr(transformer_config.instance, 'process_logs')
                }
            }
        
        report["summary"] = {
            "total_contracts": len(all_transformers),
            "active_transformers": sum(1 for t in all_transformers.values() if t.active),
            "transformer_types": list(set(type(t.instance).__name__ for t in all_transformers.values()))
        }
        
        self._save_analysis(output_file, report)
        
        print(f"📊 Transformer Summary:")
        print(f"   Total contracts: {report['summary']['total_contracts']}")
        print(f"   Active transformers: {report['summary']['active_transformers']}")
        print(f"   Transformer types: {', '.join(report['summary']['transformer_types'])}")
        print(f"\n📄 Full report saved to: {output_file}")
        
        return str(output_file)


def main():
    """Main debug session with file output"""
    print("Starting enhanced debug session with file output...")
    
    try:
        debug_session = EnhancedDebugSession()
        
        if len(sys.argv) > 1:
            # Command line mode
            command = sys.argv[1]
            
            if command == "analyze" and len(sys.argv) > 3:
                tx_hash = sys.argv[2]
                block_number = int(sys.argv[3])
                output_file = debug_session.analyze_transaction_to_file(tx_hash, block_number)
                print(f"\n🎯 Use this file to share the analysis: {output_file}")
                
            elif command == "block" and len(sys.argv) > 2:
                block_number = int(sys.argv[2])
                output_file = debug_session.quick_block_analysis_to_file(block_number)
                print(f"\n🎯 Use this file to share the analysis: {output_file}")
                
            elif command == "transformers":
                output_file = debug_session.transformer_performance_report()
                print(f"\n🎯 Use this file to share the report: {output_file}")
                
            else:
                print("Usage:")
                print("  python testing/scripts/debug_session.py analyze <tx_hash> <block_number>")
                print("  python testing/scripts/debug_session.py block <block_number>")
                print("  python testing/scripts/debug_session.py transformers")
                print("\nAll commands generate JSON files in debug_output/ directory")
        else:
            print("Enhanced debug session - generates analysis files")
            print("Run with 'analyze', 'block', or 'transformers' commands")
            
    except Exception as e:
        print(f"❌ Debug session failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()