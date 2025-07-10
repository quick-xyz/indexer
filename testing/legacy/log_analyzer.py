# testing/diagnostics/log_analyzer.py
#!/usr/bin/env python3
"""
Enhanced Log Analysis Tool with File Output

Analyzes structured logs and generates comprehensive reports in files.
"""

import json
import sys
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict, Counter
from datetime import datetime

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.core.logging_config import IndexerLogger


class EnhancedLogAnalyzer:
    """
    Enhanced log analyzer with file output capabilities
    """
    
    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.logs: List[Dict[str, Any]] = []
        self.logger = IndexerLogger.get_logger('testing.analyzer')
        
        # Create output directory
        self.output_dir = PROJECT_ROOT / "debug_output"
        self.output_dir.mkdir(exist_ok=True)
        
        self.logger.info(f"Initializing enhanced log analyzer for: {self.log_file}")
        self._parse_logs()
    
    def _parse_logs(self):
        """Parse structured JSON logs from the indexer"""
        if not self.log_file.exists():
            print(f"❌ Log file not found: {self.log_file}")
            return
        
        self.logger.debug(f"Parsing log file: {self.log_file}")
        
        json_logs = 0
        text_logs = 0
        
        with open(self.log_file, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                # Try to parse as structured JSON log first
                try:
                    log_entry = json.loads(line)
                    if isinstance(log_entry, dict) and 'timestamp' in log_entry:
                        self.logs.append(log_entry)
                        json_logs += 1
                        continue
                except json.JSONDecodeError:
                    pass
                
                # Handle plain text logs
                if any(keyword in line for keyword in ['===', '✅', '❌', '⚠️', 'DEBUG', 'INFO']):
                    self.logs.append({
                        'level': self._extract_level_from_text(line),
                        'message': line,
                        'logger': 'text_log',
                        'line_number': line_num,
                        'timestamp': None
                    })
                    text_logs += 1
        
        self.logger.info(f"Parsed {len(self.logs)} total log entries ({json_logs} JSON, {text_logs} text)")
    
    def _extract_level_from_text(self, line: str) -> str:
        """Extract log level from text line"""
        if '❌' in line or 'ERROR' in line or 'Failed' in line:
            return 'ERROR'
        elif '⚠️' in line or 'WARNING' in line or 'Warning' in line:
            return 'WARNING'
        elif '✅' in line or 'INFO' in line or 'completed' in line:
            return 'INFO'
        elif 'DEBUG' in line:
            return 'DEBUG'
        return 'INFO'
    
    def generate_comprehensive_report(self) -> str:
        """Generate comprehensive analysis report to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = self.output_dir / f"log_analysis_report_{timestamp}.json"
        
        print(f"📊 Generating comprehensive log analysis report")
        print(f"📄 Output file: {output_file}")
        print("=" * 70)
        
        report = {
            "metadata": {
                "log_file": str(self.log_file),
                "analysis_time": datetime.now().isoformat(),
                "total_logs": len(self.logs)
            },
            "summary": {},
            "transformation_pipeline": {},
            "contract_activity": {},
            "error_analysis": {},
            "transformer_performance": {},
            "recommendations": []
        }
        
        # Generate all analysis sections
        report["summary"] = self._generate_summary()
        report["transformation_pipeline"] = self._analyze_transformation_pipeline_detailed()
        report["contract_activity"] = self._analyze_contract_activity_detailed()
        report["error_analysis"] = self._analyze_errors_detailed()
        report["transformer_performance"] = self._analyze_transformer_performance_detailed()
        report["recommendations"] = self._generate_recommendations()
        
        # Save to file
        try:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            print(f"✅ Comprehensive report saved to {output_file}")
        except Exception as e:
            print(f"❌ Failed to save report: {e}")
        
        # Print summary to console
        self._print_report_summary(report)
        
        return str(output_file)
    
    def _generate_summary(self) -> Dict:
        """Generate overall summary"""
        levels = Counter(log.get('level', 'UNKNOWN') for log in self.logs)
        loggers = Counter(log.get('logger', 'unknown') for log in self.logs)
        
        return {
            "total_entries": len(self.logs),
            "by_level": dict(levels),
            "by_logger": dict(loggers.most_common(10)),
            "error_rate": (levels.get('ERROR', 0) / len(self.logs)) * 100 if self.logs else 0,
            "warning_rate": (levels.get('WARNING', 0) / len(self.logs)) * 100 if self.logs else 0
        }
    
    def _analyze_transformation_pipeline_detailed(self) -> Dict:
        """Detailed transformation pipeline analysis"""
        transform_logs = [
            log for log in self.logs 
            if 'transform' in log.get('logger', '').lower() or
               'transformer' in log.get('message', '').lower()
        ]
        
        # Analyze transfer pipeline
        transfer_analysis = self._analyze_transfer_pipeline_detailed(transform_logs)
        
        # Analyze event creation
        event_analysis = self._analyze_event_creation_detailed(transform_logs)
        
        # Analyze by transaction
        tx_analysis = self._analyze_by_transaction(transform_logs)
        
        return {
            "total_logs": len(transform_logs),
            "transfers": transfer_analysis,
            "events": event_analysis,
            "by_transaction": tx_analysis
        }
    
    def _analyze_transfer_pipeline_detailed(self, logs: List[Dict]) -> Dict:
        """Detailed transfer pipeline analysis"""
        transfer_logs = [log for log in logs if 'transfer' in log.get('message', '').lower()]
        
        # Categorize operations
        operations = {
            'created': [log for log in transfer_logs if 'created' in log.get('message', '').lower()],
            'matched': [log for log in transfer_logs if 'matched' in log.get('message', '').lower()],
            'validated': [log for log in transfer_logs if 'validation' in log.get('message', '').lower() and 'passed' in log.get('message', '').lower()],
            'failed_validation': [log for log in transfer_logs if 'validation' in log.get('message', '').lower() and 'failed' in log.get('message', '').lower()]
        }
        
        # Find problematic transactions
        problematic_txs = self._find_problematic_transactions(transfer_logs)
        
        return {
            "total_transfer_logs": len(transfer_logs),
            "operations": {k: len(v) for k, v in operations.items()},
            "problematic_transactions": problematic_txs,
            "success_rate": (len(operations['matched']) / len(operations['created'])) * 100 if operations['created'] else 0
        }
    
    def _analyze_event_creation_detailed(self, logs: List[Dict]) -> Dict:
        """Detailed event creation analysis"""
        event_logs = [
            log for log in logs 
            if any(keyword in log.get('message', '').lower() 
                  for keyword in ['event', 'swap', 'liquidity', 'poolswap'])
        ]
        
        event_types = Counter()
        failures = []
        
        for log in event_logs:
            message = log.get('message', '').lower()
            
            # Extract event type
            for event_type in ['swap', 'poolswap', 'liquidity', 'position', 'reward']:
                if event_type in message:
                    event_types[event_type] += 1
                    break
            
            # Track failures
            if log.get('level') == 'ERROR' or 'failed' in message or 'exception' in message:
                failures.append({
                    "level": log.get('level'),
                    "message": log.get('message'),
                    "context": log.get('context', {}),
                    "timestamp": log.get('timestamp')
                })
        
        return {
            "total_event_logs": len(event_logs),
            "event_types": dict(event_types),
            "failures": failures,
            "failure_rate": (len(failures) / len(event_logs)) * 100 if event_logs else 0
        }
    
    def _analyze_by_transaction(self, logs: List[Dict]) -> Dict:
        """Analyze logs grouped by transaction"""
        tx_logs = defaultdict(list)
        
        for log in logs:
            context = log.get('context', {})
            tx_hash = context.get('tx_hash')
            if tx_hash:
                tx_logs[tx_hash].append(log)
        
        tx_analysis = {}
        for tx_hash, tx_log_list in tx_logs.items():
            errors = [log for log in tx_log_list if log.get('level') == 'ERROR']
            transfers = [log for log in tx_log_list if 'transfer' in log.get('message', '').lower()]
            events = [log for log in tx_log_list if 'event' in log.get('message', '').lower()]
            
            tx_analysis[tx_hash] = {
                "total_logs": len(tx_log_list),
                "errors": len(errors),
                "transfer_logs": len(transfers),
                "event_logs": len(events),
                "has_errors": len(errors) > 0,
                "error_details": [{"message": log.get('message'), "context": log.get('context')} for log in errors]
            }
        
        return tx_analysis
    
    def _analyze_contract_activity_detailed(self) -> Dict:
        """Detailed contract activity analysis"""
        contract_logs = [
            log for log in self.logs 
            if 'contract' in log.get('message', '').lower() or
               'contract_address' in log.get('context', {})
        ]
        
        contract_activity = defaultdict(lambda: {
            'total': 0, 'errors': 0, 'warnings': 0, 'events': [], 'transformers': set()
        })
        
        for log in contract_logs:
            context = log.get('context', {})
            message = log.get('message', '')
            level = log.get('level', '')
            
            # Extract contract address
            contract_address = context.get('contract_address')
            if not contract_address:
                addresses = re.findall(r'0x[a-fA-F0-9]{40}', message)
                if addresses:
                    contract_address = addresses[0]
            
            if contract_address:
                activity = contract_activity[contract_address]
                activity['total'] += 1
                
                if level == 'ERROR':
                    activity['errors'] += 1
                elif level == 'WARNING':
                    activity['warnings'] += 1
                
                # Track event types
                for event_type in ['transfer', 'swap', 'mint', 'burn', 'liquidity']:
                    if event_type in message.lower():
                        activity['events'].append(event_type)
                
                # Track transformers
                transformer_name = context.get('transformer_name')
                if transformer_name:
                    activity['transformers'].add(transformer_name)
        
        # Convert sets to lists for JSON serialization
        result = {}
        for contract, activity in contract_activity.items():
            result[contract] = {
                'total': activity['total'],
                'errors': activity['errors'],
                'warnings': activity['warnings'],
                'error_rate': (activity['errors'] / activity['total']) * 100,
                'events': dict(Counter(activity['events']))
            }