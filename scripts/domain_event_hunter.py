#!/usr/bin/env python3
"""
Domain Event Hunter & Database Comparator

Finds specific domain events in GCS blocks and compares exactly what 
made it to the database for those transactions. Saves detailed JSON 
reports for analysis.
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.storage.gcs_handler import GCSHandler
from indexer.database.connection import ModelDatabaseManager
from sqlalchemy import text


class DomainEventHunter:
    """Hunt for specific domain events and compare GCS vs database"""
    
    def __init__(self, model_name: str = None):
        """Initialize with DI container"""
        self.model_name = model_name
        
        # Initialize DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services
        self.gcs = self.container.get(GCSHandler)
        self.model_db = self.container.get(ModelDatabaseManager)
        
        # Create output directory
        self.output_dir = Path("db_exporter/block_compare")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"✅ Initialized hunter for model: {self.config.model_name}")
        print(f"📁 Output directory: {self.output_dir}")
    
    def hunt_domain_events(self, event_type: str, sample_count: int = 10, save_details: bool = True) -> Dict[str, Any]:
        """
        Hunt for blocks containing specific domain events
        
        Args:
            event_type: Type of event to hunt for (e.g., 'Trade', 'PoolSwap', 'Liquidity')
            sample_count: Number of blocks to find with this event type
            save_details: Whether to save detailed comparison files
        """
        print(f"🔍 Hunting for {sample_count} blocks with '{event_type}' events...")
        print("=" * 70)
        
        # Get all complete blocks
        complete_blocks = self.gcs.list_complete_blocks()
        print(f"📊 Scanning {len(complete_blocks):,} complete blocks in GCS...")
        
        found_blocks = []
        scanned_count = 0
        
        # Search through blocks (most recent first)
        for block_num in sorted(complete_blocks, reverse=True):
            scanned_count += 1
            
            if scanned_count % 100 == 0:
                print(f"   📋 Scanned {scanned_count:,} blocks, found {len(found_blocks)} with {event_type}")
            
            # Get block data
            block_data = self.gcs.get_complete_block(block_num)
            if not block_data or not block_data.transactions:
                continue
            
            # Check for target event type
            block_has_target_event = False
            target_events_count = 0
            target_transactions = []
            
            for tx_hash, tx in block_data.transactions.items():
                if hasattr(tx, 'events') and tx.events:
                    tx_target_events = []
                    for event_id, event in tx.events.items():
                        if type(event).__name__ == event_type:
                            tx_target_events.append({
                                'event_id': event_id,
                                'event_type': type(event).__name__,
                                'event_data': self._serialize_event(event)
                            })
                            target_events_count += 1
                            block_has_target_event = True
                    
                    if tx_target_events:
                        target_transactions.append({
                            'tx_hash': tx_hash,
                            'target_events': tx_target_events,
                            'total_events': len(tx.events),
                            'positions_count': len(tx.positions) if hasattr(tx, 'positions') and tx.positions else 0,
                            'tx_success': getattr(tx, 'tx_success', None)
                        })
            
            if block_has_target_event:
                found_blocks.append({
                    'block_number': block_num,
                    'timestamp': getattr(block_data, 'timestamp', None),
                    'target_events_count': target_events_count,
                    'target_transactions': target_transactions,
                    'total_transactions': len(block_data.transactions)
                })
                
                print(f"   ✅ Block {block_num}: {target_events_count} {event_type} events in {len(target_transactions)} transactions")
                
                if len(found_blocks) >= sample_count:
                    break
        
        print(f"\n📊 Hunt Results:")
        print(f"   Blocks scanned: {scanned_count:,}")
        print(f"   Blocks found with {event_type}: {len(found_blocks)}")
        print(f"   Total {event_type} events found: {sum(b['target_events_count'] for b in found_blocks)}")
        
        if not found_blocks:
            print(f"❌ No blocks found containing {event_type} events")
            return {
                'event_type': event_type,
                'blocks_found': [],
                'summary': {
                    'blocks_scanned': scanned_count,
                    'blocks_with_events': 0,
                    'total_events': 0
                }
            }
        
        # Now compare with database
        comparison_results = []
        
        print(f"\n🔍 Comparing {len(found_blocks)} blocks with database...")
        
        for block_info in found_blocks:
            block_num = block_info['block_number']
            print(f"   📋 Comparing block {block_num}...")
            
            comparison = self._compare_block_with_database(block_info)
            comparison_results.append(comparison)
        
        # Create final report
        report = {
            'hunt_metadata': {
                'event_type': event_type,
                'sample_count': sample_count,
                'hunt_timestamp': datetime.now().isoformat(),
                'model_name': self.config.model_name,
                'blocks_scanned': scanned_count,
                'blocks_found': len(found_blocks)
            },
            'gcs_blocks': found_blocks,
            'database_comparisons': comparison_results,
            'summary': self._generate_comparison_summary(found_blocks, comparison_results)
        }
        
        if save_details:
            self._save_hunt_results(report, event_type)
        
        return report
    
    def _serialize_event(self, event: Any) -> Dict[str, Any]:
        """Serialize an event object to JSON-safe dict"""
        try:
            if hasattr(event, 'to_dict'):
                return event.to_dict()
            elif hasattr(event, '__dict__'):
                # Convert object attributes to dict
                data = {}
                for key, value in event.__dict__.items():
                    if not key.startswith('_'):
                        # Convert to string for JSON serialization
                        data[key] = str(value) if value is not None else None
                return data
            else:
                return {'raw_data': str(event), 'type': type(event).__name__}
        except Exception as e:
            return {'serialization_error': str(e), 'type': type(event).__name__}
    
    def _compare_block_with_database(self, block_info: Dict[str, Any]) -> Dict[str, Any]:
        """Compare a specific block's transactions with database records"""
        block_num = block_info['block_number']
        target_tx_hashes = [tx['tx_hash'] for tx in block_info['target_transactions']]
        
        comparison = {
            'block_number': block_num,
            'gcs_summary': {
                'total_transactions': block_info['total_transactions'],
                'target_transactions': len(target_tx_hashes),
                'target_events_count': block_info['target_events_count']
            },
            'database_records': {},
            'transaction_details': [],
            'discrepancies': []
        }
        
        with self.model_db.get_session() as session:
            # Get transaction processing records
            if target_tx_hashes:
                placeholders = ','.join([f"'{tx_hash}'" for tx_hash in target_tx_hashes])
                
                result = session.execute(text(f"""
                    SELECT tx_hash, status, events_generated, tx_success, block_number
                    FROM transaction_processing 
                    WHERE tx_hash IN ({placeholders})
                """))
                
                tx_processing_records = [dict(row._mapping) for row in result]
            else:
                tx_processing_records = []
            
            comparison['database_records']['transaction_processing'] = tx_processing_records
            
            # Get all domain events for this block
            domain_tables = ['trades', 'pool_swaps', 'transfers', 'liquidity', 'rewards', 'positions']
            domain_events = {}
            
            for table in domain_tables:
                try:
                    result = session.execute(text(f"""
                        SELECT * FROM {table} 
                        WHERE block_number = :block_num
                    """), {'block_num': block_num})
                    
                    records = [dict(row._mapping) for row in result]
                    domain_events[table] = records
                except Exception as e:
                    domain_events[table] = {'error': str(e)}
            
            comparison['database_records']['domain_events'] = domain_events
            
            # Get domain events for specific transactions
            if target_tx_hashes:
                for table in domain_tables:
                    try:
                        result = session.execute(text(f"""
                            SELECT * FROM {table} 
                            WHERE tx_hash IN ({placeholders})
                        """))
                        
                        tx_specific_records = [dict(row._mapping) for row in result]
                        if tx_specific_records:
                            if 'transaction_specific_events' not in comparison['database_records']:
                                comparison['database_records']['transaction_specific_events'] = {}
                            comparison['database_records']['transaction_specific_events'][table] = tx_specific_records
                    except Exception as e:
                        # Not all tables have tx_hash column
                        pass
        
        # Analyze each target transaction
        for gcs_tx in block_info['target_transactions']:
            tx_hash = gcs_tx['tx_hash']
            
            # Find corresponding database record
            db_tx_record = next(
                (record for record in tx_processing_records if record['tx_hash'] == tx_hash),
                None
            )
            
            tx_detail = {
                'tx_hash': tx_hash,
                'gcs_data': gcs_tx,
                'database_record': db_tx_record,
                'in_database': db_tx_record is not None
            }
            
            # Check for discrepancies
            if not db_tx_record:
                comparison['discrepancies'].append(f"Transaction {tx_hash[:10]}... not found in database")
            else:
                gcs_events = gcs_tx['target_events']
                db_events_generated = db_tx_record.get('events_generated', 0)
                
                if len(gcs_events) != db_events_generated:
                    comparison['discrepancies'].append(
                        f"Transaction {tx_hash[:10]}... event count mismatch: "
                        f"GCS={len(gcs_events)}, DB={db_events_generated}"
                    )
            
            comparison['transaction_details'].append(tx_detail)
        
        return comparison
    
    def _generate_comparison_summary(self, gcs_blocks: List[Dict], comparisons: List[Dict]) -> Dict[str, Any]:
        """Generate summary statistics of the comparison"""
        summary = {
            'total_blocks_analyzed': len(gcs_blocks),
            'total_gcs_transactions': sum(len(block['target_transactions']) for block in gcs_blocks),
            'total_gcs_events': sum(block['target_events_count'] for block in gcs_blocks),
            'transactions_found_in_db': 0,
            'transactions_missing_from_db': 0,
            'blocks_with_discrepancies': 0,
            'common_discrepancies': []
        }
        
        all_discrepancies = []
        
        for comparison in comparisons:
            summary['transactions_found_in_db'] += sum(
                1 for tx in comparison['transaction_details'] if tx['in_database']
            )
            summary['transactions_missing_from_db'] += sum(
                1 for tx in comparison['transaction_details'] if not tx['in_database']
            )
            
            if comparison['discrepancies']:
                summary['blocks_with_discrepancies'] += 1
                all_discrepancies.extend(comparison['discrepancies'])
        
        # Find common discrepancy patterns
        discrepancy_patterns = {}
        for discrepancy in all_discrepancies:
            if 'not found in database' in discrepancy:
                pattern = 'transaction_not_found'
            elif 'event count mismatch' in discrepancy:
                pattern = 'event_count_mismatch'
            else:
                pattern = 'other'
            
            discrepancy_patterns[pattern] = discrepancy_patterns.get(pattern, 0) + 1
        
        summary['discrepancy_patterns'] = discrepancy_patterns
        
        return summary
    
    def _save_hunt_results(self, report: Dict[str, Any], event_type: str):
        """Save hunt results to JSON files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Main report file
        main_report_file = self.output_dir / f"hunt_{event_type.lower()}_{timestamp}.json"
        
        with open(main_report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"📄 Main report saved: {main_report_file}")
        
        # Summary file
        summary_file = self.output_dir / f"summary_{event_type.lower()}_{timestamp}.json"
        summary_data = {
            'event_type': event_type,
            'hunt_metadata': report['hunt_metadata'],
            'summary': report['summary'],
            'key_findings': self._extract_key_findings(report)
        }
        
        with open(summary_file, 'w') as f:
            json.dump(summary_data, f, indent=2, default=str)
        
        print(f"📄 Summary saved: {summary_file}")
        
        # Individual block files for detailed analysis
        blocks_dir = self.output_dir / f"blocks_{event_type.lower()}_{timestamp}"
        blocks_dir.mkdir(exist_ok=True)
        
        for i, comparison in enumerate(report['database_comparisons']):
            block_file = blocks_dir / f"block_{comparison['block_number']}.json"
            
            with open(block_file, 'w') as f:
                json.dump(comparison, f, indent=2, default=str)
        
        print(f"📁 Individual block files saved in: {blocks_dir}")
        print(f"   {len(report['database_comparisons'])} block detail files created")
    
    def _extract_key_findings(self, report: Dict[str, Any]) -> List[str]:
        """Extract key findings from the report"""
        findings = []
        summary = report['summary']
        
        # Transaction findings
        total_tx = summary['total_gcs_transactions']
        found_tx = summary['transactions_found_in_db']
        missing_tx = summary['transactions_missing_from_db']
        
        if missing_tx > 0:
            findings.append(f"❌ {missing_tx}/{total_tx} transactions not found in database ({missing_tx/total_tx*100:.1f}%)")
        else:
            findings.append(f"✅ All {total_tx} transactions found in database")
        
        # Event findings
        if summary['blocks_with_discrepancies'] > 0:
            findings.append(f"⚠️ {summary['blocks_with_discrepancies']} blocks have event count discrepancies")
        
        # Pattern findings
        patterns = summary.get('discrepancy_patterns', {})
        for pattern, count in patterns.items():
            if pattern == 'transaction_not_found':
                findings.append(f"🔍 Pattern: {count} transactions completely missing from database")
            elif pattern == 'event_count_mismatch':
                findings.append(f"🔍 Pattern: {count} transactions have event count mismatches")
        
        return findings
    
    def analyze_specific_blocks(self, block_numbers: List[int], event_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Analyze specific blocks for all or specific event types
        
        Args:
            block_numbers: List of specific block numbers to analyze
            event_types: Optional list of event types to focus on (analyzes all if None)
        """
        print(f"🎯 Analyzing Specific Blocks: {block_numbers}")
        if event_types:
            print(f"🔍 Focusing on event types: {', '.join(event_types)}")
        else:
            print(f"🔍 Analyzing all event types")
        print("=" * 70)
        
        analyzed_blocks = []
        
        for block_num in block_numbers:
            print(f"\n📋 Analyzing block {block_num}...")
            
            # Get block data from GCS
            block_data = self.gcs.get_complete_block(block_num)
            if not block_data:
                print(f"   ❌ Block {block_num} not found in GCS complete storage")
                analyzed_blocks.append({
                    'block_number': block_num,
                    'error': 'Block not found in GCS'
                })
                continue
            
            # Analyze block content
            block_analysis = self._analyze_specific_block(block_data, event_types)
            analyzed_blocks.append(block_analysis)
            
            # Print brief summary
            print(f"   📊 Transactions: {block_analysis['total_transactions']}")
            print(f"   📊 Total events: {block_analysis['total_events']}")
            if event_types:
                filtered_events = sum(
                    len(events) for events in block_analysis['events_by_type'].values()
                    if any(event['event_type'] in event_types for event in events)
                )
                print(f"   📊 Target events: {filtered_events}")
            print(f"   📊 Event types found: {list(block_analysis['events_by_type'].keys())}")
        
        # Create comprehensive report
        report = {
            'analysis_metadata': {
                'block_numbers': block_numbers,
                'target_event_types': event_types,
                'analysis_timestamp': datetime.now().isoformat(),
                'model_name': self.config.model_name,
                'blocks_analyzed': len([b for b in analyzed_blocks if 'error' not in b])
            },
            'block_analyses': analyzed_blocks,
            'database_comparisons': [],
            'summary': {}
        }
        
        # Compare each valid block with database
        print(f"\n🔍 Comparing with database records...")
        
        valid_blocks = [b for b in analyzed_blocks if 'error' not in b]
        for block_analysis in valid_blocks:
            comparison = self._compare_specific_block_with_database(block_analysis, event_types)
            report['database_comparisons'].append(comparison)
        
        # Generate summary
        report['summary'] = self._generate_specific_blocks_summary(valid_blocks, report['database_comparisons'])
        
        # Save report
        self._save_specific_blocks_report(report, block_numbers, event_types)
        
        return report
    
    def _analyze_specific_block(self, block_data: Any, target_event_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Analyze a specific block's content with full GCS data"""
        analysis = {
            'block_number': getattr(block_data, 'block_number', None),
            'timestamp': getattr(block_data, 'timestamp', None),
            'indexing_status': getattr(block_data, 'indexing_status', None),
            'total_transactions': 0,
            'transactions_with_events': 0,
            'transactions_with_positions': 0,
            'total_events': 0,
            'total_positions': 0,
            'events_by_type': {},
            'transactions_details': [],
            'full_gcs_block_data': self._serialize_full_block(block_data)
        }
        
        if not block_data.transactions:
            return analysis
        
        analysis['total_transactions'] = len(block_data.transactions)
        
        # Analyze each transaction with full details
        for tx_hash, tx in block_data.transactions.items():
            tx_detail = {
                'tx_hash': tx_hash,
                'tx_success': getattr(tx, 'tx_success', None),
                'block': getattr(tx, 'block', None),
                'timestamp': getattr(tx, 'timestamp', None),
                'events': [],
                'events_raw': {},
                'positions': [],
                'positions_raw': {},
                'signals': [],
                'signals_raw': {},
                'errors': [],
                'errors_raw': {}
            }
            
            # Full events data
            if hasattr(tx, 'events') and tx.events:
                analysis['transactions_with_events'] += 1
                
                for event_id, event in tx.events.items():
                    event_type = type(event).__name__
                    
                    # Always capture raw event data
                    tx_detail['events_raw'][event_id] = {
                        'event_type': event_type,
                        'event_data': self._serialize_event(event)
                    }
                    
                    # Only include if no filter or if matches filter
                    if not target_event_types or event_type in target_event_types:
                        event_detail = {
                            'event_id': event_id,
                            'event_type': event_type,
                            'event_data': self._serialize_event(event)
                        }
                        
                        tx_detail['events'].append(event_detail)
                        
                        # Group by type
                        if event_type not in analysis['events_by_type']:
                            analysis['events_by_type'][event_type] = []
                        analysis['events_by_type'][event_type].append(event_detail)
                        
                        analysis['total_events'] += 1
            
            # Full positions data
            if hasattr(tx, 'positions') and tx.positions:
                analysis['transactions_with_positions'] += 1
                analysis['total_positions'] += len(tx.positions)
                
                for pos_id, position in tx.positions.items():
                    pos_data = self._serialize_event(position)
                    tx_detail['positions_raw'][pos_id] = pos_data
                    tx_detail['positions'].append({
                        'position_id': pos_id,
                        'position_data': pos_data
                    })
            
            # Full signals data
            if hasattr(tx, 'signals') and tx.signals:
                for signal_id, signal in tx.signals.items():
                    signal_data = self._serialize_event(signal)
                    tx_detail['signals_raw'][signal_id] = signal_data
                    tx_detail['signals'].append({
                        'signal_id': signal_id,
                        'signal_type': type(signal).__name__,
                        'signal_data': signal_data
                    })
            
            # Full errors data
            if hasattr(tx, 'errors') and tx.errors:
                for error_id, error in tx.errors.items():
                    error_data = self._serialize_event(error)
                    tx_detail['errors_raw'][error_id] = error_data
                    tx_detail['errors'].append({
                        'error_id': error_id,
                        'error_type': type(error).__name__,
                        'error_data': error_data
                    })
            
            # Include all transactions (not just those with events)
            analysis['transactions_details'].append(tx_detail)
        
        return analysis
    
    def _compare_specific_block_with_database(self, block_analysis: Dict[str, Any], target_event_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Compare specific block analysis with database records"""
        block_num = block_analysis['block_number']
        
        comparison = {
            'block_number': block_num,
            'gcs_summary': {
                'total_transactions': block_analysis['total_transactions'],
                'transactions_with_events': block_analysis['transactions_with_events'],
                'total_events': block_analysis['total_events'],
                'events_by_type': {k: len(v) for k, v in block_analysis['events_by_type'].items()}
            },
            'database_records': {},
            'transaction_comparisons': [],
            'discrepancies': []
        }
        
        with self.model_db.get_session() as session:
            # Get all transaction processing records for this block
            result = session.execute(text("""
                SELECT tx_hash, status, events_generated, tx_success, block_number
                FROM transaction_processing 
                WHERE block_number = :block_num
            """), {'block_num': block_num})
            
            tx_processing_records = [dict(row._mapping) for row in result]
            comparison['database_records']['transaction_processing'] = tx_processing_records
            
            # Get domain events for this block
            domain_tables = ['trades', 'pool_swaps', 'transfers', 'liquidity', 'rewards', 'positions']
            domain_events = {}
            
            for table in domain_tables:
                try:
                    result = session.execute(text(f"""
                        SELECT * FROM {table} 
                        WHERE block_number = :block_num
                    """), {'block_num': block_num})
                    
                    records = [dict(row._mapping) for row in result]
                    domain_events[table] = records
                except Exception as e:
                    domain_events[table] = {'error': str(e)}
            
            comparison['database_records']['domain_events'] = domain_events
        
        # Compare each transaction
        tx_hash_to_processing = {rec['tx_hash']: rec for rec in tx_processing_records}
        
        for tx_detail in block_analysis['transactions_details']:
            tx_hash = tx_detail['tx_hash']
            
            db_record = tx_hash_to_processing.get(tx_hash)
            
            # Use the new structure - events and positions are lists now
            gcs_events_count = len(tx_detail.get('events', []))
            gcs_positions_count = len(tx_detail.get('positions', []))
            
            tx_comparison = {
                'tx_hash': tx_hash,
                'gcs_events': gcs_events_count,
                'gcs_positions': gcs_positions_count,
                'gcs_events_raw_count': len(tx_detail.get('events_raw', {})),
                'gcs_positions_raw_count': len(tx_detail.get('positions_raw', {})),
                'gcs_signals_count': len(tx_detail.get('signals', [])),
                'gcs_errors_count': len(tx_detail.get('errors', [])),
                'db_record': db_record,
                'in_database': db_record is not None
            }
            
            if db_record:
                db_events = db_record.get('events_generated', 0)
                if tx_comparison['gcs_events_raw_count'] != db_events:
                    comparison['discrepancies'].append(
                        f"TX {tx_hash[:10]}... event mismatch: GCS_raw={tx_comparison['gcs_events_raw_count']}, DB={db_events}"
                    )
            else:
                comparison['discrepancies'].append(f"TX {tx_hash[:10]}... not found in database")
            
            comparison['transaction_comparisons'].append(tx_comparison)
        
        return comparison
    
    def _generate_specific_blocks_summary(self, block_analyses: List[Dict], comparisons: List[Dict]) -> Dict[str, Any]:
        """Generate summary for specific blocks analysis"""
        summary = {
            'blocks_analyzed': len(block_analyses),
            'total_transactions': sum(b['total_transactions'] for b in block_analyses),
            'total_events': sum(b['total_events'] for b in block_analyses),
            'total_positions': sum(b['total_positions'] for b in block_analyses),
            'events_by_type_totals': {},
            'database_analysis': {
                'transactions_in_db': 0,
                'transactions_missing': 0,
                'event_count_matches': 0,
                'event_count_mismatches': 0
            },
            'discrepancy_summary': []
        }
        
        # Aggregate event types
        for block in block_analyses:
            for event_type, events in block['events_by_type'].items():
                if event_type not in summary['events_by_type_totals']:
                    summary['events_by_type_totals'][event_type] = 0
                summary['events_by_type_totals'][event_type] += len(events)
        
        # Analyze database comparisons
        all_discrepancies = []
        for comparison in comparisons:
            for tx_comp in comparison['transaction_comparisons']:
                if tx_comp['in_database']:
                    summary['database_analysis']['transactions_in_db'] += 1
                    if tx_comp['gcs_events'] == tx_comp['db_record'].get('events_generated', 0):
                        summary['database_analysis']['event_count_matches'] += 1
                    else:
                        summary['database_analysis']['event_count_mismatches'] += 1
                else:
                    summary['database_analysis']['transactions_missing'] += 1
            
            all_discrepancies.extend(comparison['discrepancies'])
        
        summary['discrepancy_summary'] = all_discrepancies
        
        return summary
    
    def _serialize_full_block(self, block_data: Any) -> Dict[str, Any]:
        """Serialize the complete block data for debugging"""
        try:
            if hasattr(block_data, 'to_dict'):
                return block_data.to_dict()
            elif hasattr(block_data, '__dict__'):
                serialized = {}
                for key, value in block_data.__dict__.items():
                    if not key.startswith('_'):
                        try:
                            if hasattr(value, 'to_dict'):
                                serialized[key] = value.to_dict()
                            elif hasattr(value, '__dict__') and not callable(value):
                                # Handle nested objects
                                if isinstance(value, dict):
                                    nested_dict = {}
                                    for nested_key, nested_value in value.items():
                                        if hasattr(nested_value, 'to_dict'):
                                            nested_dict[str(nested_key)] = nested_value.to_dict()
                                        elif hasattr(nested_value, '__dict__') and not callable(nested_value):
                                            nested_dict[str(nested_key)] = self._serialize_event(nested_value)
                                        else:
                                            nested_dict[str(nested_key)] = str(nested_value) if nested_value is not None else None
                                    serialized[key] = nested_dict
                                else:
                                    serialized[key] = self._serialize_event(value)
                            else:
                                serialized[key] = str(value) if value is not None else None
                        except Exception as e:
                            serialized[key] = f"Serialization error: {str(e)}"
                return serialized
            else:
                return {'raw_data': str(block_data), 'type': type(block_data).__name__}
        except Exception as e:
            return {'serialization_error': str(e), 'type': type(block_data).__name__}

    def _save_specific_blocks_report(self, report: Dict[str, Any], block_numbers: List[int], event_types: Optional[List[str]]):
        """Save single comprehensive report file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create simple filename
        if len(block_numbers) == 1:
            blocks_str = str(block_numbers[0])
        else:
            blocks_str = f"{block_numbers[0]}_plus{len(block_numbers)-1}more"
        
        events_str = "_".join(event_types) if event_types else "all_events"
        
        # Single comprehensive report file
        report_file = self.output_dir / f"complete_analysis_{blocks_str}_{events_str}_{timestamp}.json"
        
        # Create comprehensive report with everything
        comprehensive_report = {
            'metadata': {
                'analysis_type': 'complete_block_analysis',
                'timestamp': timestamp,
                'model_name': self.config.model_name,
                'block_numbers': block_numbers,
                'target_event_types': event_types,
                'blocks_analyzed': len([b for b in report['block_analyses'] if 'error' not in b])
            },
            'gcs_block_data': {
                'blocks': report['block_analyses'],
                'raw_transactions_sample': self._extract_raw_transactions_sample(report['block_analyses'])
            },
            'database_comparison': {
                'comparisons': report['database_comparisons'],
                'missing_transactions': self._extract_missing_transactions(report['database_comparisons']),
                'database_table_counts': self._get_database_summary(block_numbers)
            },
            'analysis_summary': report['summary'],
            'diagnostic_findings': {
                'key_issues': self._extract_blocks_key_findings(report),
                'event_type_breakdown': self._get_event_type_breakdown(report['block_analyses']),
                'database_discrepancies': self._get_database_discrepancies(report['database_comparisons'])
            }
        }
        
        with open(report_file, 'w') as f:
            json.dump(comprehensive_report, f, indent=2, default=str)
        
        print(f"\n📄 Complete analysis saved: {report_file}")
        print(f"   📊 Includes: GCS block data + Database comparison + Analysis")
        
        # Print key findings to console
        self._print_diagnostic_summary(comprehensive_report)

    def _extract_raw_transactions_sample(self, block_analyses: List[Dict]) -> Dict[str, Any]:
        """Extract sample of raw transaction data for debugging"""
        sample = {}
        
        for block in block_analyses:
            if 'error' in block:
                continue
                
            block_num = block['block_number']
            sample[f"block_{block_num}"] = {
                'transaction_count': len(block.get('transactions_details', [])),
                'sample_transactions': []
            }
            
            # Include first transaction with full data
            for tx in block.get('transactions_details', [])[:1]:
                sample[f"block_{block_num}"]['sample_transactions'].append({
                    'tx_hash': tx['tx_hash'],
                    'events_raw_count': len(tx.get('events_raw', {})),
                    'positions_raw_count': len(tx.get('positions_raw', {})),
                    'signals_raw_count': len(tx.get('signals_raw', {})),
                    'events_raw_sample': dict(list(tx.get('events_raw', {}).items())[:2]),
                    'positions_raw_sample': dict(list(tx.get('positions_raw', {}).items())[:2])
                })
        
        return sample

    def _extract_missing_transactions(self, comparisons: List[Dict]) -> List[Dict]:
        """Extract details about transactions missing from database"""
        missing = []
        
        for comparison in comparisons:
            for tx_comp in comparison.get('transaction_comparisons', []):
                if not tx_comp['in_database']:
                    missing.append({
                        'block_number': comparison['block_number'],
                        'tx_hash': tx_comp['tx_hash'],
                        'gcs_events': tx_comp['gcs_events'],
                        'gcs_positions': tx_comp['gcs_positions']
                    })
        
        return missing

    def _get_database_summary(self, block_numbers: List[int]) -> Dict[str, Any]:
        """Get summary of database state for these blocks"""
        summary = {
            'tables_checked': [],
            'total_records_found': 0,
            'records_by_table': {}
        }
        
        try:
            with self.model_db.get_session() as session:
                domain_tables = ['transaction_processing', 'trades', 'pool_swaps', 'transfers', 'liquidity', 'rewards', 'positions']
                
                for table in domain_tables:
                    try:
                        if table == 'transaction_processing':
                            # Check by block numbers
                            placeholders = ','.join(str(b) for b in block_numbers)
                            result = session.execute(text(f"""
                                SELECT COUNT(*) FROM {table} 
                                WHERE block_number IN ({placeholders})
                            """))
                        else:
                            # Check by block numbers
                            placeholders = ','.join(str(b) for b in block_numbers)
                            result = session.execute(text(f"""
                                SELECT COUNT(*) FROM {table} 
                                WHERE block_number IN ({placeholders})
                            """))
                        
                        count = result.scalar()
                        summary['records_by_table'][table] = count
                        summary['total_records_found'] += count
                        summary['tables_checked'].append(table)
                        
                    except Exception as e:
                        summary['records_by_table'][table] = f"Error: {str(e)}"
                        
        except Exception as e:
            summary['database_error'] = str(e)
        
        return summary

    def _get_event_type_breakdown(self, block_analyses: List[Dict]) -> Dict[str, Any]:
        """Get detailed breakdown of event types found"""
        breakdown = {
            'total_unique_event_types': 0,
            'event_types_found': {},
            'event_type_details': {}
        }
        
        all_event_types = set()
        
        for block in block_analyses:
            if 'error' in block:
                continue
                
            for tx in block.get('transactions_details', []):
                for event_id, event_data in tx.get('events_raw', {}).items():
                    event_type = event_data.get('event_type', 'Unknown')
                    all_event_types.add(event_type)
                    
                    if event_type not in breakdown['event_types_found']:
                        breakdown['event_types_found'][event_type] = 0
                    breakdown['event_types_found'][event_type] += 1
                    
                    # Store sample event data
                    if event_type not in breakdown['event_type_details']:
                        breakdown['event_type_details'][event_type] = {
                            'sample_event_data': event_data.get('event_data', {}),
                            'count': 0
                        }
                    breakdown['event_type_details'][event_type]['count'] += 1
        
        breakdown['total_unique_event_types'] = len(all_event_types)
        
        return breakdown

    def _get_database_discrepancies(self, comparisons: List[Dict]) -> Dict[str, Any]:
        """Analyze database discrepancies in detail"""
        discrepancies = {
            'total_discrepancies': 0,
            'discrepancy_types': {},
            'affected_transactions': [],
            'patterns': []
        }
        
        for comparison in comparisons:
            for discrepancy in comparison.get('discrepancies', []):
                discrepancies['total_discrepancies'] += 1
                
                if 'not found in database' in discrepancy:
                    disc_type = 'transaction_not_found'
                elif 'event count mismatch' in discrepancy:
                    disc_type = 'event_count_mismatch'
                else:
                    disc_type = 'other'
                
                if disc_type not in discrepancies['discrepancy_types']:
                    discrepancies['discrepancy_types'][disc_type] = 0
                discrepancies['discrepancy_types'][disc_type] += 1
            
            # Track affected transactions
            for tx_comp in comparison.get('transaction_comparisons', []):
                if not tx_comp['in_database'] or tx_comp['gcs_events'] != tx_comp.get('db_record', {}).get('events_generated', 0):
                    discrepancies['affected_transactions'].append({
                        'block': comparison['block_number'],
                        'tx_hash': tx_comp['tx_hash'],
                        'issue': 'missing' if not tx_comp['in_database'] else 'event_mismatch'
                    })
        
        return discrepancies

    def _print_diagnostic_summary(self, report: Dict[str, Any]):
        """Print diagnostic summary to console"""
        print(f"\n🔍 DIAGNOSTIC SUMMARY")
        print("=" * 50)
        
        metadata = report['metadata']
        print(f"Blocks analyzed: {metadata['blocks_analyzed']}")
        print(f"Model: {metadata['model_name']}")
        
        # GCS data summary
        gcs_data = report['gcs_block_data']
        print(f"\n📊 GCS Data:")
        raw_sample = gcs_data.get('raw_transactions_sample', {})
        for block_key, block_data in raw_sample.items():
            print(f"  {block_key}: {block_data['transaction_count']} transactions")
        
        # Database summary
        db_summary = report['database_comparison']['database_table_counts']
        print(f"\n🗄️ Database Records Found: {db_summary.get('total_records_found', 0)}")
        for table, count in db_summary.get('records_by_table', {}).items():
            if isinstance(count, int) and count > 0:
                print(f"  {table}: {count}")
        
        # Key issues
        findings = report['diagnostic_findings']
        print(f"\n⚠️ Key Issues:")
        for issue in findings.get('key_issues', []):
            print(f"  • {issue}")
        
        # Event types found
        event_breakdown = findings.get('event_type_breakdown', {})
        if event_breakdown.get('event_types_found'):
            print(f"\n📋 Event Types Found:")
            for event_type, count in event_breakdown['event_types_found'].items():
                print(f"  • {event_type}: {count}")
        
        # Database issues
        db_discrepancies = findings.get('database_discrepancies', {})
        if db_discrepancies.get('total_discrepancies', 0) > 0:
            print(f"\n❌ Database Issues:")
            for disc_type, count in db_discrepancies.get('discrepancy_types', {}).items():
                print(f"  • {disc_type}: {count}")

    def _extract_blocks_key_findings(self, report: Dict[str, Any]) -> List[str]:
        """Extract key findings from blocks analysis"""
        findings = []
        summary = report['summary']
        
        # Event findings
        total_events = summary['total_events']
        if total_events > 0:
            findings.append(f"✅ Found {total_events} events across {summary['blocks_analyzed']} blocks")
            
            # Event type breakdown
            event_types = summary['events_by_type_totals']
            if event_types:
                top_events = sorted(event_types.items(), key=lambda x: x[1], reverse=True)[:3]
                findings.append(f"📊 Top event types: {', '.join(f'{t}({c})' for t, c in top_events)}")
        else:
            findings.append(f"❌ No events found in {summary['blocks_analyzed']} blocks")
        
        # Database findings
        db_analysis = summary['database_analysis']
        total_tx = summary['total_transactions']
        missing_tx = db_analysis['transactions_missing']
        
        if missing_tx > 0:
            findings.append(f"❌ {missing_tx}/{total_tx} transactions missing from database")
        
        mismatches = db_analysis['event_count_mismatches']
        if mismatches > 0:
            findings.append(f"⚠️ {mismatches} transactions have event count mismatches")
        
        # Discrepancy patterns
        discrepancies = summary['discrepancy_summary']
        if discrepancies:
            findings.append(f"🔍 {len(discrepancies)} total discrepancies found")
        
        return findings

    def hunt_multiple_event_types(self, event_types: List[str], sample_count: int = 10):
        """Hunt for multiple event types and create comparative report"""
        print(f"🔍 Multi-Event Hunt: {', '.join(event_types)}")
        print("=" * 70)
        
        all_reports = {}
        
        for event_type in event_types:
            print(f"\n📋 Hunting for {event_type}...")
            report = self.hunt_domain_events(event_type, sample_count, save_details=False)
            all_reports[event_type] = report
        
        # Create comparative summary
        comparative_report = {
            'hunt_metadata': {
                'event_types': event_types,
                'sample_count': sample_count,
                'hunt_timestamp': datetime.now().isoformat(),
                'model_name': self.config.model_name
            },
            'individual_reports': all_reports,
            'comparative_summary': self._create_comparative_summary(all_reports)
        }
        
        # Save comparative report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        comp_file = self.output_dir / f"comparative_hunt_{timestamp}.json"
        
        with open(comp_file, 'w') as f:
            json.dump(comparative_report, f, indent=2, default=str)
        
        print(f"\n📄 Comparative report saved: {comp_file}")
        
        # Print summary
        self._print_comparative_summary(comparative_report['comparative_summary'])
    
    def _create_comparative_summary(self, all_reports: Dict[str, Dict]) -> Dict[str, Any]:
        """Create comparative summary across multiple event types"""
        summary = {
            'event_availability': {},
            'database_persistence_rates': {},
            'common_issues': []
        }
        
        for event_type, report in all_reports.items():
            blocks_found = len(report.get('gcs_blocks', []))
            total_events = sum(b['target_events_count'] for b in report.get('gcs_blocks', []))
            
            summary['event_availability'][event_type] = {
                'blocks_found': blocks_found,
                'total_events': total_events,
                'events_per_block_avg': total_events / blocks_found if blocks_found > 0 else 0
            }
            
            if 'summary' in report:
                tx_total = report['summary']['total_gcs_transactions']
                tx_found = report['summary']['transactions_found_in_db']
                persistence_rate = (tx_found / tx_total * 100) if tx_total > 0 else 0
                
                summary['database_persistence_rates'][event_type] = {
                    'rate_percent': persistence_rate,
                    'transactions_total': tx_total,
                    'transactions_persisted': tx_found
                }
        
        return summary
    
    def _print_comparative_summary(self, comp_summary: Dict[str, Any]):
        """Print comparative summary in readable format"""
        print(f"\n📊 COMPARATIVE SUMMARY")
        print("=" * 70)
        
        print(f"Event Availability in GCS:")
        for event_type, data in comp_summary['event_availability'].items():
            print(f"  • {event_type}: {data['blocks_found']} blocks, {data['total_events']} events")
        
        print(f"\nDatabase Persistence Rates:")
        for event_type, data in comp_summary['database_persistence_rates'].items():
            rate = data['rate_percent']
            status = "✅" if rate >= 90 else "⚠️" if rate >= 50 else "❌"
            print(f"  {status} {event_type}: {rate:.1f}% ({data['transactions_persisted']}/{data['transactions_total']})")


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python domain_event_hunter.py <command> [args...]")
        print("Commands:")
        print("  hunt <event_type> [count] [model] - Hunt for specific event type")
        print("  multi <event_type1,event_type2,...> [count] [model] - Hunt for multiple event types")
        print("  blocks <block1,block2,...> [event_types] [model] - Analyze specific blocks")
        print("  block <block_number> [event_types] [model] - Analyze single block")
        print("\nEvent types: Trade, PoolSwap, Transfer, Liquidity, Reward, Position")
        print("\nExamples:")
        print("  python domain_event_hunter.py hunt Trade 10")
        print("  python domain_event_hunter.py hunt PoolSwap 5 blub_test")
        print("  python domain_event_hunter.py multi Trade,PoolSwap 10")
        print("  python domain_event_hunter.py blocks 58277747,58277748,58277749")
        print("  python domain_event_hunter.py blocks 58277747,58277748 Trade,PoolSwap")
        print("  python domain_event_hunter.py block 58277747")
        print("  python domain_event_hunter.py block 58277747 Trade")
        return 1
    
    command = sys.argv[1]
    
    # Parse arguments based on command
    model_name = None
    
    try:
        if command == "hunt":
            if len(sys.argv) < 3:
                print("Error: hunt command requires event_type")
                return 1
            
            event_input = sys.argv[2]
            count = 10
            
            # Parse optional arguments
            if len(sys.argv) > 3:
                try:
                    count = int(sys.argv[3])
                except ValueError:
                    model_name = sys.argv[3]
            
            if len(sys.argv) > 4:
                model_name = sys.argv[4]
            
            hunter = DomainEventHunter(model_name=model_name)
            hunter.hunt_domain_events(event_input, count)
            
        elif command == "multi":
            if len(sys.argv) < 3:
                print("Error: multi command requires event_types")
                return 1
            
            event_input = sys.argv[2]
            count = 10
            
            # Parse optional arguments
            if len(sys.argv) > 3:
                try:
                    count = int(sys.argv[3])
                except ValueError:
                    model_name = sys.argv[3]
            
            if len(sys.argv) > 4:
                model_name = sys.argv[4]
            
            event_types = event_input.split(',')
            hunter = DomainEventHunter(model_name=model_name)
            hunter.hunt_multiple_event_types(event_types, count)
            
        elif command in ["blocks", "block"]:
            if len(sys.argv) < 3:
                print(f"Error: {command} command requires block number(s)")
                return 1
            
            # Parse block numbers
            block_input = sys.argv[2]
            if command == "blocks":
                block_numbers = [int(b.strip()) for b in block_input.split(',')]
            else:  # single block
                block_numbers = [int(block_input)]
            
            # Parse optional event types and model
            event_types = None
            arg_index = 3
            
            if len(sys.argv) > arg_index:
                # Check if it's event types (contains letters) or model name
                potential_events = sys.argv[arg_index]
                if ',' in potential_events or potential_events in ['Trade', 'PoolSwap', 'Transfer', 'Liquidity', 'Reward', 'Position']:
                    event_types = potential_events.split(',')
                    arg_index += 1
                else:
                    # Assume it's a model name if it doesn't look like event types
                    model_name = potential_events
                    arg_index += 1
            
            if len(sys.argv) > arg_index:
                model_name = sys.argv[arg_index]
            
            hunter = DomainEventHunter(model_name=model_name)
            hunter.analyze_specific_blocks(block_numbers, event_types)
            
        else:
            print(f"Unknown command: {command}")
            return 1
        
        return 0
        
    except ValueError as e:
        print(f"❌ Invalid argument: {e}")
        return 1
    except Exception as e:
        print(f"❌ Hunt failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())