#!/usr/bin/env python3
"""
Block Processing Diagnostic

Troubleshoots the discrepancy between GCS complete blocks and database records.
Checks if blocks are being processed but not persisted to database properly.
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import json

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.storage.gcs_handler import GCSHandler
from indexer.database.repository import RepositoryManager
from indexer.database.connection import ModelDatabaseManager
from sqlalchemy import text


class BlockProcessingDiagnostic:
    """Diagnose block processing pipeline issues"""
    
    def __init__(self, model_name: str = None):
        """Initialize with DI container"""
        self.model_name = model_name
        
        # Initialize DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services
        self.gcs = self.container.get(GCSHandler)
        self.repository_manager = self.container.get(RepositoryManager)
        self.model_db = self.container.get(ModelDatabaseManager)
        
        print(f"✅ Initialized for model: {self.config.model_name}")
    
    def check_gcs_vs_database_discrepancy(self, limit: int = 50):
        """Check discrepancy between GCS complete blocks and database records"""
        print("🔍 Checking GCS vs Database Discrepancy")
        print("=" * 60)
        
        # Get complete blocks from GCS
        complete_blocks = self.gcs.list_complete_blocks()
        print(f"📊 GCS complete blocks: {len(complete_blocks):,}")
        
        if not complete_blocks:
            print("❌ No complete blocks found in GCS")
            return
        
        # Get database processing records
        with self.model_db.get_session() as session:
            result = session.execute(text("""
                SELECT block_number, status, events_generated, tx_success
                FROM transaction_processing 
                ORDER BY block_number DESC
                LIMIT 100
            """))
            db_processing = [dict(row._mapping) for row in result]
        
        print(f"📊 Database processing records: {len(db_processing):,}")
        
        # Check recent complete blocks
        recent_complete = sorted(complete_blocks, reverse=True)[:limit]
        
        print(f"\n🔍 Analyzing {len(recent_complete)} most recent complete blocks...")
        
        blocks_with_data = 0
        blocks_in_db = 0
        
        for i, block_num in enumerate(recent_complete):
            if i >= 10:  # Limit detailed output
                break
                
            print(f"\n📋 Block {block_num}:")
            
            # Get block from GCS
            block_data = self.gcs.get_complete_block(block_num)
            if not block_data:
                print(f"   ⚠️  Could not retrieve block data from GCS")
                continue
            
            # Analyze block content
            tx_count = len(block_data.transactions) if block_data.transactions else 0
            
            total_events = 0
            total_positions = 0
            event_types = {}
            
            if block_data.transactions:
                for tx_hash, tx in block_data.transactions.items():
                    if hasattr(tx, 'events') and tx.events:
                        for event_id, event in tx.events.items():
                            total_events += 1
                            event_type = type(event).__name__
                            event_types[event_type] = event_types.get(event_type, 0) + 1
                    
                    if hasattr(tx, 'positions') and tx.positions:
                        total_positions += len(tx.positions)
            
            print(f"   📊 Transactions: {tx_count}")
            print(f"   📊 Events: {total_events}")
            print(f"   📊 Positions: {total_positions}")
            
            if event_types:
                print(f"   📊 Event types: {dict(list(event_types.items())[:3])}")
                blocks_with_data += 1
            
            # Check if block is in database
            db_records = [r for r in db_processing if r['block_number'] == block_num]
            if db_records:
                blocks_in_db += 1
                record = db_records[0]
                print(f"   🗄️ Database status: {record['status']}")
                print(f"   🗄️ Database events: {record['events_generated']}")
            else:
                print(f"   ❌ Not found in database processing table")
        
        print(f"\n📊 Summary of recent {len(recent_complete)} blocks:")
        print(f"   📋 Blocks with event data in GCS: {blocks_with_data}")
        print(f"   📋 Blocks found in database: {blocks_in_db}")
        print(f"   📋 Missing from database: {len(recent_complete) - blocks_in_db}")
        
        return {
            'gcs_complete_count': len(complete_blocks),
            'db_processing_count': len(db_processing),
            'recent_blocks_with_data': blocks_with_data,
            'recent_blocks_in_db': blocks_in_db
        }
    
    def analyze_database_tables(self):
        """Analyze what's actually in the database tables"""
        print("\n🗄️ Database Table Analysis")
        print("=" * 60)
        
        tables = [
            'transaction_processing',
            'block_processing', 
            'trades',
            'pool_swaps',
            'transfers',
            'liquidity',
            'rewards',
            'positions'
        ]
        
        with self.model_db.get_session() as session:
            for table in tables:
                try:
                    # Get row count
                    result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.scalar()
                    
                    # Get date range if timestamp exists
                    date_info = ""
                    try:
                        result = session.execute(text(f"""
                            SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts 
                            FROM {table} WHERE timestamp IS NOT NULL
                        """))
                        row = result.fetchone()
                        if row and row[0]:
                            date_info = f" (range: {row[0]} to {row[1]})"
                    except:
                        pass
                    
                    print(f"   📊 {table}: {count:,} rows{date_info}")
                    
                except Exception as e:
                    print(f"   ❌ {table}: Error - {e}")
    
    def sample_gcs_blocks(self, count: int = 5):
        """Sample GCS blocks to see what data should be in database"""
        print(f"\n📋 Sampling {count} GCS Complete Blocks")
        print("=" * 60)
        
        complete_blocks = self.gcs.list_complete_blocks()
        if not complete_blocks:
            print("❌ No complete blocks in GCS")
            return
        
        # Sample from different parts of the range
        sample_blocks = []
        if len(complete_blocks) >= count:
            step = len(complete_blocks) // count
            for i in range(0, len(complete_blocks), step):
                if len(sample_blocks) < count:
                    sample_blocks.append(complete_blocks[i])
        else:
            sample_blocks = complete_blocks
        
        for block_num in sample_blocks:
            print(f"\n🔍 Block {block_num}:")
            
            block_data = self.gcs.get_complete_block(block_num)
            if not block_data:
                print(f"   ❌ Could not retrieve block")
                continue
            
            print(f"   📊 Status: {getattr(block_data, 'indexing_status', 'unknown')}")
            print(f"   📊 Timestamp: {getattr(block_data, 'timestamp', 'unknown')}")
            
            if not block_data.transactions:
                print(f"   📊 No transactions")
                continue
            
            # Analyze transaction data
            tx_with_events = 0
            tx_with_positions = 0
            all_event_types = {}
            total_events = 0
            total_positions = 0
            
            for tx_hash, tx in block_data.transactions.items():
                has_events = hasattr(tx, 'events') and tx.events and len(tx.events) > 0
                has_positions = hasattr(tx, 'positions') and tx.positions and len(tx.positions) > 0
                
                if has_events:
                    tx_with_events += 1
                    for event_id, event in tx.events.items():
                        total_events += 1
                        event_type = type(event).__name__
                        all_event_types[event_type] = all_event_types.get(event_type, 0) + 1
                
                if has_positions:
                    tx_with_positions += 1
                    total_positions += len(tx.positions)
            
            print(f"   📊 Transactions: {len(block_data.transactions)}")
            print(f"   📊 Transactions with events: {tx_with_events}")
            print(f"   📊 Transactions with positions: {tx_with_positions}")
            print(f"   📊 Total events: {total_events}")
            print(f"   📊 Total positions: {total_positions}")
            
            if all_event_types:
                print(f"   📊 Event types: {dict(list(all_event_types.items())[:5])}")
            
            # Show sample transaction
            if block_data.transactions:
                sample_tx_hash = list(block_data.transactions.keys())[0]
                sample_tx = block_data.transactions[sample_tx_hash]
                
                print(f"   🔍 Sample transaction {sample_tx_hash[:10]}...:")
                print(f"      Events: {len(sample_tx.events) if hasattr(sample_tx, 'events') and sample_tx.events else 0}")
                print(f"      Positions: {len(sample_tx.positions) if hasattr(sample_tx, 'positions') and sample_tx.positions else 0}")
                print(f"      Success: {getattr(sample_tx, 'tx_success', 'unknown')}")
    
    def check_specific_block(self, block_number: int):
        """Deep dive into a specific block"""
        print(f"\n🔬 Deep Analysis: Block {block_number}")
        print("=" * 60)
        
        # Check GCS
        block_data = self.gcs.get_complete_block(block_number)
        if not block_data:
            print(f"❌ Block {block_number} not found in GCS complete storage")
            return
        
        print(f"✅ Found in GCS complete storage")
        
        # Analyze GCS data
        if block_data.transactions:
            for tx_hash, tx in list(block_data.transactions.items())[:3]:  # First 3 transactions
                print(f"\n📋 Transaction {tx_hash[:10]}...:")
                
                if hasattr(tx, 'events') and tx.events:
                    print(f"   📊 Events: {len(tx.events)}")
                    for event_id, event in list(tx.events.items())[:3]:
                        print(f"      {type(event).__name__}: {event_id[:10]}...")
                else:
                    print(f"   📊 Events: 0")
                
                if hasattr(tx, 'positions') and tx.positions:
                    print(f"   📊 Positions: {len(tx.positions)}")
                    for pos_id, pos in list(tx.positions.items())[:3]:
                        print(f"      Position: {pos_id[:10]}...")
                else:
                    print(f"   📊 Positions: 0")
        
        # Check database
        with self.model_db.get_session() as session:
            # Check transaction processing
            result = session.execute(text("""
                SELECT * FROM transaction_processing 
                WHERE block_number = :block_num
            """), {'block_num': block_number})
            
            processing_records = [dict(row._mapping) for row in result]
            
            print(f"\n🗄️ Database transaction_processing records: {len(processing_records)}")
            for record in processing_records[:3]:
                print(f"   📋 TX {record['tx_hash'][:10]}... - Status: {record['status']} - Events: {record['events_generated']}")
            
            # Check domain events
            domain_tables = ['trades', 'pool_swaps', 'transfers', 'liquidity', 'rewards', 'positions']
            
            print(f"\n🗄️ Database domain events:")
            for table in domain_tables:
                result = session.execute(text(f"""
                    SELECT COUNT(*) FROM {table} WHERE block_number = :block_num
                """), {'block_num': block_number})
                count = result.scalar()
                print(f"   📊 {table}: {count}")


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python block_processing_diagnostic.py <command> [model_name] [args]")
        print("Commands:")
        print("  overview [model] - Check GCS vs database discrepancy")
        print("  analyze [model] - Analyze database table contents")
        print("  sample [model] [count] - Sample GCS blocks (default: 5)")
        print("  block [model] <block_number> - Deep dive into specific block")
        return 1
    
    command = sys.argv[1]
    model_name = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].isdigit() else None
    
    # Adjust argument parsing based on whether model_name was provided
    if model_name and not model_name.isdigit():
        # Model name provided
        if command == "sample":
            count = int(sys.argv[3]) if len(sys.argv) > 3 else 5
        elif command == "block":
            if len(sys.argv) < 4:
                print("Error: block command requires block_number")
                return 1
            block_number = int(sys.argv[3])
    else:
        # No model name, shift arguments
        model_name = None
        if command == "sample":
            count = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        elif command == "block":
            if len(sys.argv) < 3:
                print("Error: block command requires block_number")
                return 1
            block_number = int(sys.argv[2])
    
    try:
        diagnostic = BlockProcessingDiagnostic(model_name=model_name)
        
        if command == "overview":
            diagnostic.check_gcs_vs_database_discrepancy()
            diagnostic.analyze_database_tables()
            
        elif command == "analyze":
            diagnostic.analyze_database_tables()
            
        elif command == "sample":
            diagnostic.sample_gcs_blocks(count)
            
        elif command == "block":
            diagnostic.check_specific_block(block_number)
            
        else:
            print(f"Unknown command: {command}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())