# testing/diagnostics/log_analyzer.py
"""
Log Analysis Tool for Blockchain Indexer

Analyzes structured logs from the indexer's logging system to identify
transformation pipeline issues and debugging insights.
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


class LogAnalyzer:
    """
    Analyzer for indexer's structured JSON logs and transformation debugging
    """
    
    def __init__(self, log_file: str):
        self.log_file = Path(log_file)
        self.logs: List[Dict[str, Any]] = []
        self.logger = IndexerLogger.get_logger('testing.analyzer')
        
        self.logger.info(f"Initializing log analyzer for: {self.log_file}")
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
                
                # Handle plain text logs (from print statements, etc.)
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
    
    def analyze_transformation_pipeline(self):
        """Deep analysis of transformation pipeline"""
        print("🔍 TRANSFORMATION PIPELINE ANALYSIS")
        print("=" * 60)
        
        # Filter transformation-related logs
        transform_logs = self._filter_logs_by_component(['transform', 'transformer'])
        
        print(f"📊 Found {len(transform_logs)} transformation-related logs")
        
        # Analyze by component
        self._analyze_by_component(transform_logs)
        
        # Specific analysis
        self._analyze_transfer_pipeline(transform_logs)
        self._analyze_event_creation(transform_logs)
        self._analyze_validation_failures(transform_logs)
        self._analyze_transformer_performance(transform_logs)
    
    def _filter_logs_by_component(self, components: List[str]) -> List[Dict]:
        """Filter logs by component names"""
        filtered = []
        for log in self.logs:
            logger_name = log.get('logger', '').lower()
            message = log.get('message', '').lower()
            
            if any(comp in logger_name or comp in message for comp in components):
                filtered.append(log)
        
        return filtered
    
    def _analyze_by_component(self, logs: List[Dict]):
        """Analyze logs by component/logger"""
        print(f"\n📈 LOGS BY COMPONENT")
        print("-" * 30)
        
        component_stats = Counter()
        level_stats = defaultdict(Counter)
        
        for log in logs:
            logger = log.get('logger', 'unknown')
            level = log.get('level', 'UNKNOWN')
            
            component_stats[logger] += 1
            level_stats[logger][level] += 1
        
        for component, count in component_stats.most_common():
            levels = level_stats[component]
            level_summary = ', '.join(f"{level}: {count}" for level, count in levels.items())
            print(f"   📄 {component}: {count} entries ({level_summary})")
    
    def _analyze_transfer_pipeline(self, logs: List[Dict]):
        """Analyze transfer creation and matching pipeline"""
        print(f"\n🔄 TRANSFER PIPELINE ANALYSIS")
        print("-" * 30)
        
        transfer_logs = [log for log in logs if 'transfer' in log.get('message', '').lower()]
        print(f"Found {len(transfer_logs)} transfer-related logs")
        
        # Categorize transfer operations
        categories = {
            'created': [],
            'matched': [],
            'unmatched': [],
            'validated': [],
            'failed_validation': [],
            'converted': []
        }
        
        for log in transfer_logs:
            message = log.get('message', '').lower()
            context = log.get('context', {})
            
            if 'transfer created' in message or 'created from log' in message:
                categories['created'].append(log)
            elif 'matched' in message and 'transfer' in message:
                categories['matched'].append(log)
            elif 'unmatched' in message:
                categories['unmatched'].append(log)
            elif 'validation' in message and ('passed' in message or 'success' in message):
                categories['validated'].append(log)
            elif 'validation' in message and ('failed' in message or 'error' in message):
                categories['failed_validation'].append(log)
            elif 'convert' in message:
                categories['converted'].append(log)
        
        # Report statistics
        for category, logs_list in categories.items():
            if logs_list:
                print(f"   📊 {category.replace('_', ' ').title()}: {len(logs_list)}")
                
                # Show sample details for key categories
                if category in ['failed_validation', 'unmatched'] and logs_list:
                    for log in logs_list[:3]:  # Show first 3
                        msg = log.get('message', '')
                        context = log.get('context', {})
                        print(f"      - {msg}")
                        if context:
                            relevant_context = {k: v for k, v in context.items() 
                                              if k in ['tx_hash', 'transfer_id', 'token', 'amount']}
                            if relevant_context:
                                print(f"        Context: {relevant_context}")
        
        # Analyze transfer flow issues
        self._analyze_transfer_flow_issues(transfer_logs)
    
    def _analyze_transfer_flow_issues(self, transfer_logs: List[Dict]):
        """Analyze specific transfer flow issues"""
        print(f"\n   🔍 TRANSFER FLOW ISSUES")
        
        # Group by transaction hash
        tx_transfers = defaultdict(list)
        for log in transfer_logs:
            context = log.get('context', {})
            tx_hash = context.get('tx_hash')
            if tx_hash:
                tx_transfers[tx_hash].append(log)
        
        problematic_txs = []
        for tx_hash, tx_logs in tx_transfers.items():
            # Look for transactions with transfers created but not matched
            created = [log for log in tx_logs if 'created' in log.get('message', '').lower()]
            matched = [log for log in tx_logs if 'matched' in log.get('message', '').lower()]
            
            if created and not matched:
                problematic_txs.append((tx_hash, len(created), len(matched)))
        
        if problematic_txs:
            print(f"      ⚠️  Found {len(problematic_txs)} transactions with unmatched transfers:")
            for tx_hash, created_count, matched_count in problematic_txs[:5]:
                print(f"         TX {tx_hash[:10]}...: {created_count} created, {matched_count} matched")
    
    def _analyze_event_creation(self, logs: List[Dict]):
        """Analyze domain event creation"""
        print(f"\n🎯 EVENT CREATION ANALYSIS")
        print("-" * 30)
        
        event_logs = [log for log in logs if any(keyword in log.get('message', '').lower() 
                     for keyword in ['event', 'swap', 'liquidity', 'poolswap', 'domain'])]
        
        print(f"Found {len(event_logs)} event-related logs")
        
        # Categorize events
        event_types = Counter()
        event_outcomes = {'created': 0, 'failed': 0}
        
        for log in event_logs:
            message = log.get('message', '').lower()
            context = log.get('context', {})
            
            # Extract event type
            for event_type in ['swap', 'poolswap', 'liquidity', 'position', 'reward']:
                if event_type in message:
                    event_types[event_type] += 1
                    break
            
            # Track outcomes
            if 'created' in message or 'completed successfully' in message:
                event_outcomes['created'] += 1
            elif 'failed' in message or 'error' in message:
                event_outcomes['failed'] += 1
        
        print(f"   📊 Event types: {dict(event_types)}")
        print(f"   📊 Outcomes: Created: {event_outcomes['created']}, Failed: {event_outcomes['failed']}")
        
        # Show failures
        failed_events = [log for log in event_logs 
                        if 'failed' in log.get('message', '').lower() or 
                           log.get('level') == 'ERROR']
        
        if failed_events:
            print(f"\n   ❌ Event Creation Failures ({len(failed_events)}):")
            for log in failed_events[:5]:
                msg = log.get('message', '')
                context = log.get('context', {})
                print(f"      - {msg}")
                if context.get('tx_hash'):
                    print(f"        TX: {context['tx_hash'][:10]}...")
    
    def _analyze_validation_failures(self, logs: List[Dict]):
        """Analyze validation failures in detail"""
        print(f"\n❌ VALIDATION FAILURE ANALYSIS")
        print("-" * 30)
        
        error_logs = [log for log in logs 
                     if log.get('level') in ['ERROR', 'WARNING'] or
                        'fail' in log.get('message', '').lower() or
                        'error' in log.get('message', '').lower()]
        
        print(f"Found {len(error_logs)} error/warning logs")
        
        # Categorize errors
        error_categories = defaultdict(list)
        for log in error_logs:
            message = log.get('message', '').lower()
            
            if 'validation' in message:
                error_categories['validation'].append(log)
            elif 'transfer' in message:
                error_categories['transfer'].append(log)
            elif 'attribute' in message:
                error_categories['attribute'].append(log)
            elif 'exception' in message:
                error_categories['exception'].append(log)
            else:
                error_categories['other'].append(log)
        
        for category, logs_list in error_categories.items():
            if logs_list:
                print(f"\n   📊 {category.title()} Errors: {len(logs_list)}")
                for log in logs_list[:3]:  # Show first 3
                    msg = log.get('message', '')
                    context = log.get('context', {})
                    logger = log.get('logger', '')
                    print(f"      [{logger}] {msg}")
                    if context:
                        print(f"        Context: {context}")
    
    def _analyze_transformer_performance(self, logs: List[Dict]):
        """Analyze individual transformer performance"""
        print(f"\n⚡ TRANSFORMER PERFORMANCE")
        print("-" * 30)
        
        transformer_logs = [log for log in logs if 'transformer' in log.get('logger', '').lower()]
        
        # Group by transformer type
        transformer_stats = defaultdict(lambda: {'total': 0, 'errors': 0, 'success': 0})
        
        for log in transformer_logs:
            logger = log.get('logger', '')
            level = log.get('level', '')
            context = log.get('context', {})
            
            transformer_name = context.get('transformer_name', 
                              logger.split('.')[-1] if '.' in logger else logger)
            
            transformer_stats[transformer_name]['total'] += 1
            
            if level == 'ERROR':
                transformer_stats[transformer_name]['errors'] += 1
            elif 'success' in log.get('message', '').lower():
                transformer_stats[transformer_name]['success'] += 1
        
        for transformer, stats in transformer_stats.items():
            if stats['total'] > 0:
                error_rate = (stats['errors'] / stats['total']) * 100
                success_rate = (stats['success'] / stats['total']) * 100
                print(f"   🔧 {transformer}:")
                print(f"      Total: {stats['total']}, Errors: {stats['errors']} ({error_rate:.1f}%), Success: {stats['success']} ({success_rate:.1f}%)")
    
    def analyze_contract_activity(self):
        """Analyze contract-specific activity"""
        print(f"\n🏗️  CONTRACT ACTIVITY ANALYSIS")
        print("=" * 60)
        
        contract_logs = [log for log in self.logs if 'contract' in log.get('message', '').lower() or
                        'contract_address' in log.get('context', {})]
        
        print(f"Found {len(contract_logs)} contract-related logs")
        
        # Extract contract addresses and their activity
        contract_activity = defaultdict(lambda: {'total': 0, 'errors': 0, 'events': []})
        
        for log in contract_logs:
            context = log.get('context', {})
            message = log.get('message', '')
            level = log.get('level', '')
            
            contract_address = context.get('contract_address')
            if not contract_address:
                # Try to extract from message
                import re
                addresses = re.findall(r'0x[a-fA-F0-9]{40}', message)
                if addresses:
                    contract_address = addresses[0]
            
            if contract_address:
                contract_activity[contract_address]['total'] += 1
                
                if level == 'ERROR':
                    contract_activity[contract_address]['errors'] += 1
                
                # Track event types
                for event_type in ['transfer', 'swap', 'mint', 'burn', 'liquidity']:
                    if event_type in message.lower():
                        contract_activity[contract_address]['events'].append(event_type)
        
        # Display contract activity
        for contract, activity in sorted(contract_activity.items(), 
                                       key=lambda x: x[1]['total'], reverse=True):
            if activity['total'] > 0:
                event_summary = Counter(activity['events'])
                event_str = ', '.join(f"{event}: {count}" for event, count in event_summary.items())
                error_rate = (activity['errors'] / activity['total']) * 100
                
                print(f"   📄 {contract}:")
                print(f"      Activity: {activity['total']} logs, Errors: {activity['errors']} ({error_rate:.1f}%)")
                if event_str:
                    print(f"      Events: {event_str}")
    
    def generate_debugging_recommendations(self):
        """Generate specific debugging recommendations"""
        print(f"\n🎯 DEBUGGING RECOMMENDATIONS")
        print("=" * 60)
        
        # Analyze error patterns
        error_logs = [log for log in self.logs if log.get('level') == 'ERROR']
        warning_logs = [log for log in self.logs if log.get('level') == 'WARNING']
        
        print(f"📊 Summary: {len(error_logs)} errors, {len(warning_logs)} warnings out of {len(self.logs)} total logs")
        
        # Top error patterns
        error_patterns = Counter()
        for log in error_logs:
            message = log.get('message', '')
            # Extract key patterns
            if 'validation' in message.lower():
                error_patterns['validation_failures'] += 1
            elif 'transfer' in message.lower():
                error_patterns['transfer_issues'] += 1
            elif 'attribute' in message.lower():
                error_patterns['missing_attributes'] += 1
            elif 'exception' in message.lower():
                error_patterns['exceptions'] += 1
        
        print(f"\n🔍 Top Error Patterns:")
        for pattern, count in error_patterns.most_common(5):
            print(f"   {pattern.replace('_', ' ').title()}: {count}")
        
        # Specific recommendations
        print(f"\n💡 Specific Recommendations:")
        
        if error_patterns['validation_failures'] > 0:
            print("   1. 🔍 VALIDATION FAILURES:")
            print("      - Check transfer count validation logic")
            print("      - Verify amount matching in compare_amounts()")
            print("      - Review transformer event handler mapping")
        
        if error_patterns['transfer_issues'] > 0:
            print("   2. 🔄 TRANSFER ISSUES:")
            print("      - Debug UnmatchedTransfer → MatchedTransfer conversion")
            print("      - Check transfer filtering logic in transformers")
            print("      - Verify token address matching (case sensitivity)")
        
        if error_patterns['missing_attributes'] > 0:
            print("   3. 📋 MISSING ATTRIBUTES:")
            print("      - Check ABI decoding for log attributes")
            print("      - Verify contract ABIs are loaded correctly")
            print("      - Check log signature matching")
        
        print(f"\n🎯 Next Steps:")
        print("   1. Focus on the highest error count pattern first")
        print("   2. Use tx_hash from error context to debug specific transactions")
        print("   3. Run with a simpler transaction (single token transfer)")
        print("   4. Check transformer configuration in config.json")


def main():
    """Main analysis function"""
    if len(sys.argv) < 2:
        print("Usage: python testing/diagnostics/log_analyzer.py <log_file>")
        print("Example: python testing/diagnostics/log_analyzer.py logs/indexer.log")
        sys.exit(1)
    
    log_file = sys.argv[1]
    
    print("🔍 BLOCKCHAIN INDEXER LOG ANALYSIS")
    print("=" * 70)
    print(f"📄 Analyzing: {log_file}")
    print(f"📅 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        analyzer = LogAnalyzer(log_file)
        
        if not analyzer.logs:
            print("❌ No logs found to analyze")
            sys.exit(1)
        
        # Run all analyses
        analyzer.analyze_transformation_pipeline()
        analyzer.analyze_contract_activity()
        analyzer.generate_debugging_recommendations()
        
        print(f"\n✅ Analysis complete!")
        
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()