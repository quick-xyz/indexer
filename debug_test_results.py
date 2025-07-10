#!/usr/bin/env python3
"""
Debug script to fetch and inspect test results from GCS and database
"""

import sys
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.storage.gcs_handler import GCSHandler
from indexer.database.repository import RepositoryManager

class TestResultsDebugger:
    """Debug and inspect test results from GCS and database"""
    
    def __init__(self, model_name: str = None):
        self.container = create_indexer(model_name=model_name)
        self.gcs = self.container.get(GCSHandler)
        self.repository_manager = self.container.get(RepositoryManager)
        
        # Create debug output directory
        self.debug_dir = Path("debug_output")
        self.debug_dir.mkdir(exist_ok=True)
        
        print(f"🔍 Debug output will be saved to: {self.debug_dir.absolute()}")
    
    def fetch_gcs_data(self, block_number: int) -> Dict[str, Any]:
        """Fetch block data from GCS"""
        print(f"\n📥 Fetching GCS data for block {block_number}...")
        
        gcs_data = {}
        
        # Try to fetch complete block data
        try:
            complete_blob_path = self.gcs.get_blob_string("complete", block_number)
            print(f"   Fetching: {complete_blob_path}")
            
            if self.gcs.blob_exists(complete_blob_path):
                data_bytes = self.gcs.download_blob_as_bytes(complete_blob_path)
                complete_data = json.loads(data_bytes.decode('utf-8'))
                gcs_data["complete"] = complete_data
                print(f"   ✅ Complete block data: {len(str(complete_data))} chars")
            else:
                print(f"   ❌ Complete block blob does not exist")
                gcs_data["complete"] = None
            
        except Exception as e:
            print(f"   ❌ Failed to fetch complete data: {e}")
            gcs_data["complete"] = None
        
        # Try to fetch processing block data (might not exist if moved to complete)
        try:
            processing_blob_path = self.gcs.get_blob_string("processing", block_number)
            print(f"   Fetching: {processing_blob_path}")
            
            if self.gcs.blob_exists(processing_blob_path):
                data_bytes = self.gcs.download_blob_as_bytes(processing_blob_path)
                processing_data = json.loads(data_bytes.decode('utf-8'))
                gcs_data["processing"] = processing_data
                print(f"   ✅ Processing block data: {len(str(processing_data))} chars")
            else:
                print(f"   ⏭️  Processing block blob does not exist (expected if moved to complete)")
                gcs_data["processing"] = None
            
        except Exception as e:
            print(f"   ⏭️  Processing data not found (expected if moved to complete): {e}")
            gcs_data["processing"] = None
        
        return gcs_data
    
    def fetch_database_records(self, block_number: int) -> Dict[str, List[Dict]]:
        """Fetch all database records for the block"""
        print(f"\n💾 Fetching database records for block {block_number}...")
        
        db_data = {}
        
        with self.repository_manager.get_session() as session:
            # Transaction Processing
            try:
                from indexer.database.indexer.tables.processing import TransactionProcessing
                tx_records = session.query(TransactionProcessing).filter(
                    TransactionProcessing.block_number == block_number
                ).all()
                
                db_data["transaction_processing"] = [
                    {
                        "tx_hash": record.tx_hash,
                        "status": record.status.value if hasattr(record.status, 'value') else str(record.status),
                        "block_number": record.block_number,
                        "timestamp": record.timestamp,
                        "tx_success": record.tx_success,
                        "events_generated": record.events_generated,
                        "logs_processed": record.logs_processed,
                        "created_at": record.created_at.isoformat() if record.created_at else None,
                    }
                    for record in tx_records
                ]
                print(f"   ✅ Transaction processing: {len(tx_records)} records")
                
            except Exception as e:
                print(f"   ❌ Failed to fetch transaction processing: {e}")
                db_data["transaction_processing"] = []
            
            # Liquidity Events - Handle enum case issue
            try:
                from indexer.database.indexer.tables.events.liquidity import Liquidity
                from sqlalchemy import text
                
                # Try to fetch with raw SQL to avoid enum validation errors
                raw_liquidity = session.execute(
                    text("SELECT content_id, tx_hash, pool, provider, action, base_token, base_amount, quote_token, quote_amount, block_number, timestamp, created_at FROM liquidity WHERE block_number = :block_number"),
                    {"block_number": block_number}
                ).fetchall()
                
                db_data["liquidity_events"] = [
                    {
                        "content_id": record[0],
                        "tx_hash": record[1],
                        "pool": record[2],
                        "provider": record[3],
                        "action": record[4],  # Raw string value from database
                        "base_token": record[5],
                        "base_amount": str(record[6]),
                        "quote_token": record[7],
                        "quote_amount": str(record[8]),
                        "block_number": record[9],
                        "timestamp": record[10],
                        "created_at": record[11].isoformat() if record[11] else None,
                    }
                    for record in raw_liquidity
                ]
                print(f"   ✅ Liquidity events (raw): {len(raw_liquidity)} records")
                
            except Exception as e:
                print(f"   ❌ Failed to fetch liquidity events: {e}")
                db_data["liquidity_events"] = []
            
            # Trade Events  
            try:
                from indexer.database.indexer.tables.events.trade import Trade, PoolSwap
                
                trade_records = session.query(Trade).filter(
                    Trade.block_number == block_number
                ).all()
                
                db_data["trade_events"] = [
                    {
                        "content_id": record.content_id,
                        "tx_hash": record.tx_hash,
                        "taker": record.taker,
                        "direction": record.direction.value if hasattr(record.direction, 'value') else str(record.direction),
                        "base_token": record.base_token,
                        "base_amount": str(record.base_amount),
                        "trade_type": record.trade_type.value if hasattr(record.trade_type, 'value') else str(record.trade_type),
                        "router": record.router,
                        "swap_count": record.swap_count,
                        "block_number": record.block_number,
                        "timestamp": record.timestamp,
                        "created_at": record.created_at.isoformat() if record.created_at else None,
                    }
                    for record in trade_records
                ]
                
                # Pool Swaps
                pool_swap_records = session.query(PoolSwap).filter(
                    PoolSwap.block_number == block_number
                ).all()
                
                db_data["pool_swap_events"] = [
                    {
                        "content_id": record.content_id,
                        "tx_hash": record.tx_hash,
                        "pool": record.pool,
                        "taker": record.taker,
                        "direction": record.direction.value if hasattr(record.direction, 'value') else str(record.direction),
                        "base_token": record.base_token,
                        "base_amount": str(record.base_amount),
                        "quote_token": record.quote_token,
                        "quote_amount": str(record.quote_amount),
                        "trade_id": record.trade_id,
                        "block_number": record.block_number,
                        "timestamp": record.timestamp,
                        "created_at": record.created_at.isoformat() if record.created_at else None,
                    }
                    for record in pool_swap_records
                ]
                
                print(f"   ✅ Trade events: {len(trade_records)} records")
                print(f"   ✅ Pool swap events: {len(pool_swap_records)} records")
                
            except Exception as e:
                print(f"   ❌ Failed to fetch trade/pool swap events: {e}")
                db_data["trade_events"] = []
                db_data["pool_swap_events"] = []
            
            # Positions - Handle schema differences
            try:
                from indexer.database.indexer.tables.events.position import Position
                from sqlalchemy import text
                
                # Try raw SQL first to see what columns exist
                raw_positions = session.execute(
                    text("SELECT content_id, tx_hash, \"user\", token, amount, block_number, timestamp, created_at FROM positions WHERE block_number = :block_number"),
                    {"block_number": block_number}
                ).fetchall()
                
                db_data["positions"] = [
                    {
                        "content_id": record[0],
                        "tx_hash": record[1],
                        "user": record[2],
                        "token": record[3],
                        "amount": str(record[4]),
                        "block_number": record[5],
                        "timestamp": record[6],
                        "created_at": record[7].isoformat() if record[7] else None,
                    }
                    for record in raw_positions
                ]
                print(f"   ✅ Positions (raw): {len(raw_positions)} records")
                
            except Exception as e:
                print(f"   ❌ Failed to fetch positions: {e}")
                db_data["positions"] = []
        
        return db_data
    
    def save_results(self, block_number: int, gcs_data: Dict, db_data: Dict):
        """Save results to debug output files"""
        print(f"\n💾 Saving debug results...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save GCS data
        gcs_file = self.debug_dir / f"block_{block_number}_gcs_{timestamp}.json"
        with open(gcs_file, 'w') as f:
            json.dump(gcs_data, f, indent=2, default=str)
        print(f"   ✅ GCS data saved: {gcs_file}")
        
        # Save database data
        db_file = self.debug_dir / f"block_{block_number}_database_{timestamp}.json"
        with open(db_file, 'w') as f:
            json.dump(db_data, f, indent=2, default=str)
        print(f"   ✅ Database data saved: {db_file}")
        
        # Save summary
        summary = {
            "block_number": block_number,
            "timestamp": timestamp,
            "summary": {
                "gcs": {
                    "complete_data_exists": gcs_data.get("complete") is not None,
                    "processing_data_exists": gcs_data.get("processing") is not None,
                },
                "database": {
                    "transaction_processing_count": len(db_data.get("transaction_processing", [])),
                    "liquidity_events_count": len(db_data.get("liquidity_events", [])),
                    "trade_events_count": len(db_data.get("trade_events", [])),
                    "pool_swap_events_count": len(db_data.get("pool_swap_events", [])),
                    "positions_count": len(db_data.get("positions", [])),
                }
            }
        }
        
        summary_file = self.debug_dir / f"block_{block_number}_summary_{timestamp}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"   ✅ Summary saved: {summary_file}")
        
        return gcs_file, db_file, summary_file
    
    def debug_block(self, block_number: int):
        """Debug a specific block's test results"""
        print(f"🔍 Debugging test results for block {block_number}")
        print("=" * 60)
        
        # First, check database schema issues
        self.check_database_schema()
        
        # Fetch GCS data
        gcs_data = self.fetch_gcs_data(block_number)
        
        # Fetch database records
        db_data = self.fetch_database_records(block_number)
        
        # Save results
        gcs_file, db_file, summary_file = self.save_results(block_number, gcs_data, db_data)
        
        # Print summary
        print(f"\n📊 Summary for block {block_number}:")
        print(f"   GCS Complete: {'✅' if gcs_data.get('complete') else '❌'}")
        print(f"   GCS Processing: {'✅' if gcs_data.get('processing') else '❌'}")
        print(f"   DB Transactions: {len(db_data.get('transaction_processing', []))}")
        print(f"   DB Events: {len(db_data.get('liquidity_events', [])) + len(db_data.get('trade_events', [])) + len(db_data.get('pool_swap_events', []))}")
        print(f"   DB Positions: {len(db_data.get('positions', []))}")
        
        print(f"\n✅ Debug files saved to {self.debug_dir.absolute()}")
        return gcs_file, db_file, summary_file
    
    def check_database_schema(self):
        """Check database schema for enum issues"""
        print(f"\n🔍 Checking database schema...")
        
        with self.repository_manager.get_session() as session:
            try:
                from sqlalchemy import text
                
                # Check enum values in database
                enum_check = session.execute(
                    text("SELECT enumlabel FROM pg_enum WHERE enumtypid = (SELECT oid FROM pg_type WHERE typname = 'liquidityaction') ORDER BY enumsortorder")
                ).fetchall()
                
                enum_values = [row[0] for row in enum_check]
                print(f"   📋 liquidityaction enum values: {enum_values}")
                
                if 'remove' in enum_values:
                    print(f"   ✅ Enum has lowercase 'remove' value")
                elif 'REMOVE' in enum_values:
                    print(f"   ⚠️  Enum has uppercase 'REMOVE' value - database needs recreation")
                else:
                    print(f"   ❌ Enum missing 'remove'/'REMOVE' value")
                    
            except Exception as e:
                print(f"   ❌ Failed to check enum schema: {e}")
            
            try:
                from sqlalchemy import text
                
                # Check positions table schema
                columns_check = session.execute(
                    text("SELECT column_name FROM information_schema.columns WHERE table_name = 'positions' ORDER BY ordinal_position")
                ).fetchall()
                
                column_names = [row[0] for row in columns_check]
                print(f"   📋 positions table columns: {column_names}")
                
            except Exception as e:
                print(f"   ❌ Failed to check positions schema: {e}")
                
            try:
                from sqlalchemy import text
                
                # Check if liquidity table has data
                liquidity_count = session.execute(
                    text("SELECT COUNT(*) FROM liquidity WHERE block_number = :block_number"),
                    {"block_number": 58277747}
                ).scalar()
                
                print(f"   📊 Liquidity records for block 58277747: {liquidity_count}")
                
            except Exception as e:
                print(f"   ❌ Failed to check liquidity data: {e}")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Debug test results from GCS and database')
    parser.add_argument('block_number', type=int, help='Block number to debug')
    parser.add_argument('--model', help='Model name (defaults to env var)')
    args = parser.parse_args()
    
    try:
        debugger = TestResultsDebugger(model_name=args.model)
        debugger.debug_block(args.block_number)
        
    except KeyboardInterrupt:
        print(f"\n⏹️ Debug interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Debug failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()