#!/usr/bin/env python3
"""
GCS Block Sampling & Analysis Script

Samples completed blocks from GCS storage to analyze what data was 
generated during indexing and compare it to what made it into the database.
"""

import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from collections import defaultdict, Counter

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.storage.gcs_handler import GCSHandler
from indexer.database.connection import ModelDatabaseManager
from sqlalchemy import text


class GCSBlockSampler:
    """Sample and analyze GCS blocks to understand data flow issues"""
    
    def __init__(self, model_name: str = None):
        """Initialize with DI container"""
        self.model_name = model_name
        
        # Initialize DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services
        self.gcs = self.container.get(GCSHandler)
        self.model_db = self.container.get(ModelDatabaseManager)
        
        print(f"✅ Initialized GCS sampler for model: {self.config.model_name}")
    
    def get_sampling_strategy(self, total_blocks: int, sample_size: int = 20) -> List[int]:
        """Get a good sampling strategy across the block range"""
        complete_blocks = self.gcs.list_complete_blocks()
        
        if not complete_blocks:
            return []
        
        if len(complete_blocks) <= sample_size:
            return complete_blocks
        
        # Sample strategy: recent + distributed + oldest
        recent_count = min(5, sample_size // 3)
        distributed_count = sample_size - recent_count - 2
        
        sample_blocks = []
        
        # Recent blocks
        sample_blocks.extend(sorted(complete_blocks, reverse=True)[:recent_count])
        
        # Distributed sampling
        if distributed_count > 0:
            step = len(complete_blocks) // distributed_count
            for i in range(step, len(complete_blocks) - step, step):
                if len(sample_blocks) < sample_size - 2:
                    sample_blocks.append(complete_blocks[i])
        
        # Oldest blocks
        sample_blocks.extend(sorted(complete_blocks)[:2])
        
        return sorted(list(set(sample_blocks)))
    
    def analyze_block_content(self, block_number: int) -> Dict[str, Any]:
        """Analyze the content of a specific block"""
        block_data = self.gcs.get_complete_block(block_number)
        
        if not block_data:
            return {'error': 'Block not found in GCS'}
        
        analysis = {
            'block_number': block_number,
            'timestamp': getattr(block_data, 'timestamp', None),
            'indexing_status': getattr(block_data, 'indexing_status', None),
            'transaction_count': 0,
            'transactions_with_events': 0,
            'transactions_with_positions': 0,
            'transactions_with_errors': 0,
            'total_events': 0,
            'total_positions': 0,
            'total_errors': 0,
            'event_types': {},
            'position_types': {},
            'sample_transactions': []
        }
        
        if not block_data.transactions:
            return analysis
        
        analysis['transaction_count'] = len(block_data.transactions)
        
        # Analyze each transaction
        for tx_hash, tx in block_data.transactions.items():
            tx_analysis = {
                'tx_hash': tx_hash,
                'tx_success': getattr(tx, 'tx_success', None),
                'events_count': 0,
                'positions_count': 0,
                'errors_count': 0,
                'event_types': [],
                'signals_count': 0
            }
            
            # Count events
            if hasattr(tx, 'events') and tx.events:
                tx_analysis['events_count'] = len(tx.events)
                analysis['total_events'] += len(tx.events)
                analysis['transactions_with_events'] += 1
                
                for event_id, event in tx.events.items():
                    event_type = type(event).__name__
                    tx_analysis['event_types'].append(event_type)
                    analysis['event_types'][event_type] = analysis['event_types'].get(event_type, 0) + 1
            
            # Count positions
            if hasattr(tx, 'positions') and tx.positions:
                tx_analysis['positions_count'] = len(tx.positions)
                analysis['total_positions'] += len(tx.positions)
                analysis['transactions_with_positions'] += 1
            
            # Count errors
            if hasattr(tx, 'errors') and tx.errors:
                tx_analysis['errors_count'] = len(tx.errors)
                analysis['total_errors'] += len(tx.errors)
                analysis['transactions_with_errors'] += 1
            
            # Count signals (if available)
            if hasattr(tx, 'signals') and tx.signals:
                tx_analysis['signals_count'] = len(tx.signals)
            
            # Keep sample of interesting transactions
            if (tx_analysis['events_count'] > 0 or 
                tx_analysis['positions_count'] > 0 or 
                tx_analysis['errors_count'] > 0):
                analysis['sample_transactions'].append(tx_analysis)
        
        return analysis
    
    def compare_block_to_database(self, block_number: int) -> Dict[str, Any]:
        """Compare GCS block data to what's in the database"""
        gcs_analysis = self.analyze_block_content(block_number)
        
        comparison = {
            'block_number': block_number,
            'gcs_data': gcs_analysis,
            'database_data': {},
            'discrepancies': []
        }
        
        # Get database data
        with self.model_db.get_session() as session:
            # Transaction processing records
            result = session.execute(text("""
                SELECT tx_hash, status, events_generated, tx_success
                FROM transaction_processing 
                WHERE block_number = :block_num
            """), {'block_num': block_number})
            
            tx_processing = [dict(row._mapping) for row in result]
            comparison['database_data']['transaction_processing'] = tx_processing
            
            # Domain event counts
            domain_tables = ['trades', 'pool_swaps', 'transfers', 'liquidity', 'rewards', 'positions']
            domain_counts = {}
            
            for table in domain_tables:
                try:
                    result = session.execute(text(f"""
                        SELECT COUNT(*) FROM {table} WHERE block_number = :block_num
                    """), {'block_num': block_number})
                    domain_counts[table] = result.scalar()
                except Exception as e:
                    domain_counts[table] = f"Error: {e}"
            
            comparison['database_data']['domain_events'] = domain_counts
        
        # Analyze discrepancies
        gcs_tx_count = gcs_analysis.get('transaction_count', 0)
        db_tx_count = len(tx_processing)
        
        if gcs_tx_count != db_tx_count:
            comparison['discrepancies'].append(
                f"Transaction count mismatch: GCS={gcs_tx_count}, DB={db_tx_count}"
            )
        
        # Check event counts
        gcs_events = gcs_analysis.get('total_events', 0)
        db_events_total = sum(
            count for count in domain_counts.values() 
            if isinstance(count, int)
        )
        
        if gcs_events != db_events_total:
            comparison['discrepancies'].append(
                f"Event count mismatch: GCS={gcs_events}, DB={db_events_total}"
            )
        
        return comparison
    
    def run_comprehensive_sampling(self, sample_size: int = 10):
        """Run comprehensive sampling across GCS blocks"""
        print(f"🔍 Running Comprehensive GCS Block Sampling")
        print("=" * 70)
        
        # Get sampling strategy
        complete_blocks = self.gcs.list_complete_blocks()
        sample_blocks = self.get_sampling_strategy(len(complete_blocks), sample_size)
        
        print(f"📊 Total complete blocks in GCS: {len(complete_blocks):,}")
        print(f"📊 Sampling {len(sample_blocks)} blocks")
        print(f"📊 Block range: {min(complete_blocks)} to {max(complete_blocks)}")
        
        # Analyze each sample block
        all_analyses = []
        summary_stats = {
            'blocks_with_events': 0,
            'blocks_with_positions': 0,
            'blocks_with_errors': 0,
            'total_events': 0,
            'total_positions': 0,
            'total_errors': 0,
            'event_type_counts': Counter(),
            'blocks_in_database': 0,
            'database_mismatches': 0
        }
        
        for i, block_num in enumerate(sample_blocks):
            print(f"\n🔍 Analyzing block {block_num} ({i+1}/{len(sample_blocks)})...")
            
            analysis = self.analyze_block_content(block_num)
            comparison = self.compare_block_to_database(block_num)
            
            all_analyses.append({
                'analysis': analysis,
                'comparison': comparison
            })
            
            # Update summary stats
            if analysis.get('total_events', 0) > 0:
                summary_stats['blocks_with_events'] += 1
                summary_stats['total_events'] += analysis['total_events']
                
                for event_type, count in analysis.get('event_types', {}).items():
                    summary_stats['event_type_counts'][event_type] += count
            
            if analysis.get('total_positions', 0) > 0:
                summary_stats['blocks_with_positions'] += 1
                summary_stats['total_positions'] += analysis['total_positions']
            
            if analysis.get('total_errors', 0) > 0:
                summary_stats['blocks_with_errors'] += 1
                summary_stats['total_errors'] += analysis['total_errors']
            
            # Check database presence
            db_tx_count = len(comparison['database_data'].get('transaction_processing', []))
            if db_tx_count > 0:
                summary_stats['blocks_in_database'] += 1
            
            if comparison.get('discrepancies'):
                summary_stats['database_mismatches'] += 1
            
            # Print brief summary for this block
            print(f"   📊 Transactions: {analysis.get('transaction_count', 0)}")
            print(f"   📊 Events: {analysis.get('total_events', 0)} ({len(analysis.get('event_types', {}))} types)")
            print(f"   📊 Positions: {analysis.get('total_positions', 0)}")
            print(f"   🗄️ In database: {'Yes' if db_tx_count > 0 else 'No'}")
            
            if comparison.get('discrepancies'):
                print(f"   ⚠️ Discrepancies: {len(comparison['discrepancies'])}")
        
        # Print comprehensive summary
        self._print_sampling_summary(summary_stats, len(sample_blocks))
        
        # Save detailed analysis to file
        self._save_detailed_analysis(all_analyses, sample_blocks)
    
    def _print_sampling_summary(self, stats: Dict, sample_count: int):
        """Print comprehensive sampling summary"""
        print(f"\n📊 COMPREHENSIVE SAMPLING SUMMARY")
        print("=" * 70)
        
        print(f"Sample size: {sample_count} blocks")
        print(f"Blocks with events: {stats['blocks_with_events']} ({stats['blocks_with_events']/sample_count*100:.1f}%)")
        print(f"Blocks with positions: {stats['blocks_with_positions']} ({stats['blocks_with_positions']/sample_count*100:.1f}%)")
        print(f"Blocks with errors: {stats['blocks_with_errors']} ({stats['blocks_with_errors']/sample_count*100:.1f}%)")
        
        print(f"\nTotal events found: {stats['total_events']:,}")
        print(f"Total positions found: {stats['total_positions']:,}")
        print(f"Total errors found: {stats['total_errors']:,}")
        
        print(f"\nDatabase integration:")
        print(f"Blocks found in database: {stats['blocks_in_database']} ({stats['blocks_in_database']/sample_count*100:.1f}%)")
        print(f"Blocks with data mismatches: {stats['database_mismatches']} ({stats['database_mismatches']/sample_count*100:.1f}%)")
        
        if stats['event_type_counts']:
            print(f"\nTop event types found:")
            for event_type, count in stats['event_type_counts'].most_common(10):
                print(f"  • {event_type}: {count:,}")
    
    def _save_detailed_analysis(self, analyses: List[Dict], sample_blocks: List[int]):
        """Save detailed analysis to JSON file"""
        output_file = Path(f"gcs_sampling_analysis_{self.config.model_name}.json")
        
        output_data = {
            'model_name': self.config.model_name,
            'sample_blocks': sample_blocks,
            'timestamp': str(Path(__file__).stat().st_mtime),
            'analyses': analyses
        }
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        print(f"\n📄 Detailed analysis saved to: {output_file}")
    
    def analyze_specific_discrepancy(self, block_number: int):
        """Deep dive into a specific block's discrepancy"""
        print(f"🔬 Deep Discrepancy Analysis: Block {block_number}")
        print("=" * 70)
        
        comparison = self.compare_block_to_database(block_number)
        
        gcs_data = comparison['gcs_data']
        db_data = comparison['database_data']
        
        print(f"📊 GCS Data Summary:")
        print(f"   Transactions: {gcs_data.get('transaction_count', 0)}")
        print(f"   Events: {gcs_data.get('total_events', 0)}")
        print(f"   Positions: {gcs_data.get('total_positions', 0)}")
        print(f"   Event types: {list(gcs_data.get('event_types', {}).keys())}")
        
        print(f"\n🗄️ Database Data Summary:")
        print(f"   Transaction records: {len(db_data.get('transaction_processing', []))}")
        print(f"   Domain events: {db_data.get('domain_events', {})}")
        
        if comparison.get('discrepancies'):
            print(f"\n⚠️ Discrepancies Found:")
            for discrepancy in comparison['discrepancies']:
                print(f"   • {discrepancy}")
        
        # Show sample transactions for debugging
        sample_txs = gcs_data.get('sample_transactions', [])[:3]
        if sample_txs:
            print(f"\n🔍 Sample Transactions from GCS:")
            for tx in sample_txs:
                print(f"   📋 {tx['tx_hash'][:10]}...:")
                print(f"      Success: {tx['tx_success']}")
                print(f"      Events: {tx['events_count']} ({tx['event_types']})")
                print(f"      Positions: {tx['positions_count']}")
                print(f"      Signals: {tx['signals_count']}")


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python gcs_sampling_script.py <command> [model_name] [args]")
        print("Commands:")
        print("  sample [model] [count] - Sample GCS blocks (default: 10)")
        print("  compare [model] <block_number> - Compare specific block GCS vs DB")
        print("  overview [model] - Quick overview of GCS vs database")
        return 1
    
    command = sys.argv[1]
    
    # Parse arguments
    model_name = None
    if len(sys.argv) > 2 and not sys.argv[2].isdigit():
        model_name = sys.argv[2]
        args_start = 3
    else:
        args_start = 2
    
    try:
        sampler = GCSBlockSampler(model_name=model_name)
        
        if command == "sample":
            count = int(sys.argv[args_start]) if len(sys.argv) > args_start else 10
            sampler.run_comprehensive_sampling(count)
            
        elif command == "compare":
            if len(sys.argv) <= args_start:
                print("Error: compare command requires block_number")
                return 1
            block_number = int(sys.argv[args_start])
            sampler.analyze_specific_discrepancy(block_number)
            
        elif command == "overview":
            # Quick overview
            complete_blocks = sampler.gcs.list_complete_blocks()
            print(f"📊 GCS complete blocks: {len(complete_blocks):,}")
            if complete_blocks:
                print(f"📊 Block range: {min(complete_blocks)} to {max(complete_blocks)}")
            
            sampler.run_comprehensive_sampling(5)  # Quick sample
            
        else:
            print(f"Unknown command: {command}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Sampling failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())