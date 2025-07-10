# testing/diagnostics/signal_troubleshooter.py
"""
Signal Generation Troubleshooter
Diagnoses why signals/events are not being generated for a specific transaction
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager
from indexer.transform.registry import TransformRegistry
from indexer.contracts.registry import ContractRegistry


class SignalTroubleshooter:
    """Deep diagnostic tool for signal generation issues"""
    
    def __init__(self, config_path: str = None):
        self.testing_env = get_testing_environment(config_path, log_level="DEBUG")
        
        # Get services
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformManager)
        self.transformer_registry = self.testing_env.get_service(TransformRegistry)
        self.contract_registry = self.testing_env.get_service(ContractRegistry)
    
    def diagnose_transaction(self, tx_hash: str, block_number: int) -> Dict[str, Any]:
        """Comprehensive diagnosis of signal generation"""
        
        print(f"🔬 Deep Signal Analysis: {tx_hash[:10]}...")
        print("=" * 60)
        
        diagnosis = {
            "transaction_hash": tx_hash,
            "block_number": block_number,
            "stages": {},
            "issues_found": [],
            "recommendations": []
        }
        
        try:
            # Stage 1: Block and Transaction Retrieval
            raw_block, transaction = self._diagnose_retrieval(block_number, tx_hash, diagnosis)
            if not transaction:
                return diagnosis
            
            # Stage 2: Log Analysis
            self._diagnose_logs(transaction, diagnosis)
            
            # Stage 3: Contract and Transformer Mapping
            self._diagnose_contract_mapping(transaction, diagnosis)
            
            # Stage 4: Signal Generation Step-by-Step
            self._diagnose_signal_generation(transaction, diagnosis)
            
            # Stage 5: Event Generation
            self._diagnose_event_generation(transaction, diagnosis)
            
            # Stage 6: Generate Recommendations
            self._generate_recommendations(diagnosis)
            
            return diagnosis
            
        except Exception as e:
            diagnosis["fatal_error"] = {
                "error": str(e),
                "stage": "diagnosis_setup"
            }
            return diagnosis
    
    def _diagnose_retrieval(self, block_number: int, tx_hash: str, diagnosis: Dict) -> tuple:
        """Diagnose block and transaction retrieval"""
        
        stage = "retrieval"
        diagnosis["stages"][stage] = {}
        
        try:
            # Get raw block
            raw_block = self.storage_handler.get_rpc_block(block_number)
            if not raw_block:
                diagnosis["stages"][stage]["error"] = f"Block {block_number} not found in storage"
                diagnosis["issues_found"].append("Block not found in GCS storage")
                return None, None
            
            diagnosis["stages"][stage]["raw_block_found"] = True
            diagnosis["stages"][stage]["raw_transactions"] = len(raw_block.transactions)
            
            # Decode block
            decoded_block = self.block_decoder.decode_block(raw_block)
            diagnosis["stages"][stage]["decoded_transactions"] = len(decoded_block.transactions) if decoded_block.transactions else 0
            
            # Find specific transaction
            if not decoded_block.transactions or tx_hash not in decoded_block.transactions:
                diagnosis["stages"][stage]["error"] = f"Transaction {tx_hash} not found in decoded block"
                diagnosis["issues_found"].append("Transaction not found in decoded block")
                
                # List available transactions for debugging
                if decoded_block.transactions:
                    available_txs = list(decoded_block.transactions.keys())[:5]
                    diagnosis["stages"][stage]["available_transactions_sample"] = available_txs
                
                return raw_block, None
            
            transaction = decoded_block.transactions[tx_hash]
            diagnosis["stages"][stage]["transaction_found"] = True
            diagnosis["stages"][stage]["transaction_success"] = transaction.tx_success
            diagnosis["stages"][stage]["total_logs"] = len(transaction.logs)
            
            print(f"✅ Block/Transaction: {len(transaction.logs)} logs found")
            
            return raw_block, transaction
            
        except Exception as e:
            diagnosis["stages"][stage]["exception"] = str(e)
            diagnosis["issues_found"].append(f"Retrieval exception: {e}")
            return None, None
    
    def _diagnose_logs(self, transaction, diagnosis: Dict):
        """Analyze transaction logs in detail"""
        
        stage = "logs"
        diagnosis["stages"][stage] = {
            "total_logs": len(transaction.logs),
            "decoded_logs": 0,
            "encoded_logs": 0,
            "contracts_involved": set(),
            "log_details": []
        }
        
        for log_idx, log in transaction.logs.items():
            log_info = {
                "index": log_idx,
                "contract": log.contract,
                "is_decoded": hasattr(log, 'name')
            }
            
            diagnosis["stages"][stage]["contracts_involved"].add(log.contract)
            
            if hasattr(log, 'name'):
                # Decoded log
                diagnosis["stages"][stage]["decoded_logs"] += 1
                log_info.update({
                    "name": log.name,
                    "attributes": list(log.attributes.keys()),
                    "attribute_count": len(log.attributes)
                })
            else:
                # Encoded log  
                diagnosis["stages"][stage]["encoded_logs"] += 1
                log_info.update({
                    "signature": log.signature,
                    "topics_count": len(log.topics) if log.topics else 0
                })
            
            diagnosis["stages"][stage]["log_details"].append(log_info)
        
        # Convert set to list for JSON serialization
        diagnosis["stages"][stage]["contracts_involved"] = list(diagnosis["stages"][stage]["contracts_involved"])
        
        decoded_count = diagnosis["stages"][stage]["decoded_logs"]
        total_count = diagnosis["stages"][stage]["total_logs"]
        
        print(f"✅ Logs: {decoded_count}/{total_count} decoded, {len(diagnosis['stages'][stage]['contracts_involved'])} contracts")
        
        if decoded_count == 0:
            diagnosis["issues_found"].append("No logs were decoded - check ABI files and contract registry")
        
        return diagnosis["stages"][stage]
    
    def _diagnose_contract_mapping(self, transaction, diagnosis: Dict):
        """Diagnose contract-to-transformer mapping"""
        
        stage = "contract_mapping"
        diagnosis["stages"][stage] = {
            "mappings": {},
            "missing_transformers": [],
            "missing_handlers": [],
            "valid_mappings": 0
        }
        
        for log_idx, log in transaction.logs.items():
            if not hasattr(log, 'name'):
                continue  # Skip encoded logs
            
            contract = log.contract
            log_name = log.name
            
            # Check contract registry
            contract_config = self.contract_registry.get_contract(contract)
            has_contract_config = contract_config is not None
            
            # Check transformer registry
            transformer = self.transformer_registry.get_transformer(contract)
            has_transformer = transformer is not None
            
            # Check handler
            has_handler = False
            available_handlers = []
            if transformer and hasattr(transformer, 'handler_map'):
                available_handlers = list(transformer.handler_map.keys())
                has_handler = log_name in transformer.handler_map
            
            mapping_info = {
                "log_index": log_idx,
                "contract": contract,
                "log_name": log_name,
                "has_contract_config": has_contract_config,
                "has_transformer": has_transformer,
                "transformer_name": type(transformer).__name__ if transformer else None,
                "has_handler": has_handler,
                "available_handlers": available_handlers
            }
            
            diagnosis["stages"][stage]["mappings"][str(log_idx)] = mapping_info
            
            # Track issues
            if not has_transformer:
                diagnosis["stages"][stage]["missing_transformers"].append({
                    "contract": contract,
                    "log_name": log_name,
                    "log_index": log_idx
                })
            elif not has_handler:
                diagnosis["stages"][stage]["missing_handlers"].append({
                    "contract": contract,
                    "log_name": log_name,
                    "transformer": type(transformer).__name__,
                    "log_index": log_idx,
                    "available_handlers": available_handlers
                })
            else:
                diagnosis["stages"][stage]["valid_mappings"] += 1
        
        valid_count = diagnosis["stages"][stage]["valid_mappings"]
        missing_transformers = len(diagnosis["stages"][stage]["missing_transformers"])
        missing_handlers = len(diagnosis["stages"][stage]["missing_handlers"])
        
        print(f"✅ Mapping: {valid_count} valid, {missing_transformers} missing transformers, {missing_handlers} missing handlers")
        
        if missing_transformers > 0:
            diagnosis["issues_found"].append(f"{missing_transformers} contracts have no transformers")
        
        if missing_handlers > 0:
            diagnosis["issues_found"].append(f"{missing_handlers} logs have no handlers")
    
    def _diagnose_signal_generation(self, transaction, diagnosis: Dict):
        """Diagnose signal generation step by step"""
        
        stage = "signal_generation"
        diagnosis["stages"][stage] = {
            "signals_generated": 0,
            "signal_types": {},
            "transformer_results": {},
            "processing_success": False
        }
        
        try:
            # Test signal generation
            success, processed_tx = self.transform_manager.process_transaction(transaction)
            
            diagnosis["stages"][stage]["processing_success"] = success
            
            if processed_tx.signals:
                diagnosis["stages"][stage]["signals_generated"] = len(processed_tx.signals)
                
                for signal_idx, signal in processed_tx.signals.items():
                    signal_type = type(signal).__name__
                    diagnosis["stages"][stage]["signal_types"][signal_type] = diagnosis["stages"][stage]["signal_types"].get(signal_type, 0) + 1
            
            if processed_tx.errors:
                diagnosis["stages"][stage]["errors"] = {}
                for error_id, error in processed_tx.errors.items():
                    diagnosis["stages"][stage]["errors"][error_id] = {
                        "error_type": error.error_type,
                        "message": error.message,
                        "stage": error.stage,
                        "context": error.context
                    }
            
            signal_count = diagnosis["stages"][stage]["signals_generated"]
            error_count = len(processed_tx.errors) if processed_tx.errors else 0
            
            print(f"✅ Signals: {signal_count} generated, {error_count} errors")
            
            if signal_count == 0 and error_count == 0:
                diagnosis["issues_found"].append("No signals generated and no errors - check transformer logic")
            
        except Exception as e:
            diagnosis["stages"][stage]["exception"] = str(e)
            diagnosis["issues_found"].append(f"Signal generation failed: {e}")
            print(f"❌ Signal generation exception: {e}")
    
    def _diagnose_event_generation(self, transaction, diagnosis: Dict):
        """Diagnose event generation from signals"""
        
        stage = "event_generation"
        diagnosis["stages"][stage] = {
            "events_generated": 0,
            "event_types": {},
            "pattern_processing": {}
        }
        
        try:
            # This is covered in the signal generation step
            success, processed_tx = self.transform_manager.process_transaction(transaction)
            
            if processed_tx.events:
                diagnosis["stages"][stage]["events_generated"] = len(processed_tx.events)
                
                for event_id, event in processed_tx.events.items():
                    event_type = type(event).__name__
                    diagnosis["stages"][stage]["event_types"][event_type] = diagnosis["stages"][stage]["event_types"].get(event_type, 0) + 1
            
            event_count = diagnosis["stages"][stage]["events_generated"]
            print(f"✅ Events: {event_count} generated")
            
            if event_count == 0:
                diagnosis["issues_found"].append("No events generated from signals")
                
        except Exception as e:
            diagnosis["stages"][stage]["exception"] = str(e)
            print(f"❌ Event generation exception: {e}")
    
    def _generate_recommendations(self, diagnosis: Dict):
        """Generate specific recommendations based on diagnosis"""
        
        recommendations = []
        
        # Check for missing transformers
        if "contract_mapping" in diagnosis["stages"]:
            missing_transformers = diagnosis["stages"]["contract_mapping"]["missing_transformers"]
            if missing_transformers:
                for missing in missing_transformers:
                    recommendations.append({
                        "priority": "HIGH",
                        "issue": "Missing Transformer",
                        "description": f"Contract {missing['contract']} has no transformer for event '{missing['log_name']}'",
                        "action": f"Add transformer configuration for contract {missing['contract']} in config.json",
                        "example": {
                            "transform": {
                                "name": "AppropriateTransformerName",
                                "instantiate": {
                                    "contract": missing['contract']
                                }
                            }
                        }
                    })
            
            # Check for missing handlers
            missing_handlers = diagnosis["stages"]["contract_mapping"]["missing_handlers"]
            if missing_handlers:
                for missing in missing_handlers:
                    recommendations.append({
                        "priority": "HIGH", 
                        "issue": "Missing Handler",
                        "description": f"Transformer {missing['transformer']} has no handler for event '{missing['log_name']}'",
                        "action": f"Add handler for '{missing['log_name']}' to {missing['transformer']}",
                        "available_handlers": missing.get('available_handlers', [])
                    })
        
        # Check for signal generation issues
        if "signal_generation" in diagnosis["stages"]:
            signal_count = diagnosis["stages"]["signal_generation"]["signals_generated"]
            if signal_count == 0:
                recommendations.append({
                    "priority": "MEDIUM",
                    "issue": "No Signals Generated",
                    "description": "Transformers processed logs but generated no signals",
                    "action": "Check transformer logic and validation methods",
                    "debug_steps": [
                        "Enable DEBUG logging to see transformer processing details",
                        "Check if validation methods are rejecting valid data",
                        "Verify attribute extraction from log events"
                    ]
                })
        
        diagnosis["recommendations"] = recommendations
    
    def print_summary(self, diagnosis: Dict):
        """Print a clean summary of the diagnosis"""
        
        print("\n" + "=" * 60)
        print("🔍 DIAGNOSIS SUMMARY")
        print("=" * 60)
        
        # Transaction info
        print(f"📋 Transaction: {diagnosis['transaction_hash'][:10]}...")
        print(f"📦 Block: {diagnosis['block_number']}")
        
        # Issues found
        if diagnosis["issues_found"]:
            print(f"\n❌ Issues Found ({len(diagnosis['issues_found'])}):")
            for i, issue in enumerate(diagnosis["issues_found"], 1):
                print(f"   {i}. {issue}")
        else:
            print(f"\n✅ No critical issues found")
        
        # Quick stats
        stages = diagnosis["stages"]
        if "logs" in stages:
            decoded = stages["logs"]["decoded_logs"]
            total = stages["logs"]["total_logs"]
            print(f"\n📊 Quick Stats:")
            print(f"   Decoded Logs: {decoded}/{total}")
            
            if "contract_mapping" in stages:
                valid_mappings = stages["contract_mapping"]["valid_mappings"]
                print(f"   Valid Mappings: {valid_mappings}")
            
            if "signal_generation" in stages:
                signals = stages["signal_generation"]["signals_generated"]
                print(f"   Signals Generated: {signals}")
            
            if "event_generation" in stages:
                events = stages["event_generation"]["events_generated"]
                print(f"   Events Generated: {events}")
        
        # Recommendations
        if diagnosis["recommendations"]:
            print(f"\n💡 Recommendations ({len(diagnosis['recommendations'])}):")
            for i, rec in enumerate(diagnosis["recommendations"], 1):
                priority_icon = "🔴" if rec["priority"] == "HIGH" else "🟡"
                print(f"   {i}. {priority_icon} {rec['issue']}: {rec['description']}")


def main():
    """Main troubleshooting function"""
    
    if len(sys.argv) != 3:
        print("Usage: python signal_troubleshooter.py <tx_hash> <block_number>")
        print("Example: python signal_troubleshooter.py 0xab6908d3303c7b77c6a6b945c42235fd972de6180d738c779d91e86d55e8c19b 63269916")
        sys.exit(1)
    
    tx_hash = sys.argv[1]
    try:
        block_number = int(sys.argv[2])
    except ValueError:
        print("❌ Invalid block number")
        sys.exit(1)
    
    try:
        troubleshooter = SignalTroubleshooter()
        diagnosis = troubleshooter.diagnose_transaction(tx_hash, block_number)
        troubleshooter.print_summary(diagnosis)
        
        # Save detailed diagnosis
        timestamp = f"{block_number}_{tx_hash[:10]}"
        output_file = PROJECT_ROOT / "debug_output" / f"signal_diagnosis_{timestamp}.json"
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(diagnosis, f, indent=2, default=str)
        
        print(f"\n💾 Detailed diagnosis saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Troubleshooting failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()