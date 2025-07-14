#!/usr/bin/env python3
"""
End-to-End Test Results Analyzer

Comprehensive analysis tool that follows end-to-end tests by fetching 
both GCS and database data for a block, comparing them for consistency,
and generating a unified JSON report for review.

Usage:
    python testing/analyze_test_results.py 65297576
    python testing/analyze_test_results.py 65297576 --detailed
    python testing/analyze_test_results.py 65297576 --output results.json
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import msgspec

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer import create_indexer
from indexer.storage.gcs_handler import GCSHandler
from indexer.database.repository import RepositoryManager
from sqlalchemy import text


@dataclass
class ConsistencyResult:
    """Result of consistency check between GCS and database"""
    is_consistent: bool
    discrepancies: List[str]
    gcs_summary: Dict[str, Any]
    db_summary: Dict[str, Any]


class EndToEndResultsAnalyzer:
    """Analyze end-to-end test results by comparing GCS and database data"""
    
    def __init__(self, model_name: str = None):
        """Initialize with DI container"""
        # Initialize DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services
        self.gcs = self.container.get(GCSHandler)
        self.repository_manager = self.container.get(RepositoryManager)
        
        print(f"✅ Initialized analyzer for model: {self.config.model_name}")
    
    def analyze_block(self, block_number: int, include_detailed: bool = False) -> Dict[str, Any]:
        """
        Comprehensive analysis of a single block
        
        Args:
            block_number: Block number to analyze
            include_detailed: Whether to include detailed transaction/event data
            
        Returns:
            Complete analysis results
        """
        print(f"🔍 Analyzing Block {block_number}")
        print("=" * 60)
        
        # Create output directory for this specific block
        output_dir = Path("testing/output") / str(block_number)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Fetch GCS data
        print("📥 Fetching GCS data...")
        gcs_data = self._fetch_gcs_data(block_number)
        
        # 2. Fetch database data
        print("🗄️ Fetching database data...")
        db_data = self._fetch_database_data(block_number)
        
        # 3. Perform consistency check
        print("🔍 Performing consistency analysis...")
        consistency = self._check_consistency(gcs_data, db_data, include_detailed)
        
        # 4. Build comprehensive report
        report = {
            "metadata": {
                "block_number": block_number,
                "model_name": self.config.model_name,
                "analysis_timestamp": datetime.now().isoformat(),
                "analyzer_version": "1.0"
            },
            "gcs_data": gcs_data,
            "database_data": db_data,
            "consistency_check": {
                "is_consistent": consistency.is_consistent,
                "discrepancies": consistency.discrepancies,
                "gcs_summary": consistency.gcs_summary,
                "database_summary": consistency.db_summary
            }
        }
        
        # 5. Display summary
        self._display_summary(report)
        
        # 6. Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"analysis_{timestamp}.json"
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n💾 Analysis saved to: {output_file}")
        print(f"\n🎉 Analysis complete!")
        print(f"📊 Consistent: {'YES' if consistency.is_consistent else 'NO'}")
        print(f"📁 Report saved: {output_file}")
        
        return report
    
    def _fetch_gcs_data(self, block_number: int) -> Dict[str, Any]:
        """Fetch block data from GCS storage"""
        gcs_result = {
            "block_exists": False,
            "processing_exists": False,
            "complete_exists": False,
            "block_data": None,
            "processing_data": None,
            "error": None
        }
        
        try:
            # Check for complete block first (most likely location)
            complete_blob_path = self.gcs.get_blob_string("complete", block_number)
            if self.gcs.blob_exists(complete_blob_path):
                print(f"   ✅ Found complete block data ({complete_blob_path})")
                gcs_result["complete_exists"] = True
                gcs_result["block_exists"] = True
                
                # Load the complete block data and convert to JSON
                complete_data = self.gcs.get_complete_block(block_number)
                gcs_result["block_data"] = self._convert_block_to_json(complete_data)
                print(f"   ✅ Loaded complete block data")
            
            # Check for processing block (less likely but possible)
            processing_blob_path = self.gcs.get_blob_string("processing", block_number)
            if self.gcs.blob_exists(processing_blob_path):
                print(f"   ✅ Found processing block data ({processing_blob_path})")
                gcs_result["processing_exists"] = True
                if not gcs_result["block_exists"]:
                    gcs_result["block_exists"] = True
                    processing_data = self.gcs.get_processing_block(block_number)
                    gcs_result["processing_data"] = self._convert_block_to_json(processing_data)
                    print(f"   ✅ Loaded processing block data")
            
            if not gcs_result["block_exists"]:
                print(f"   ❌ No block data found in GCS")
                
        except Exception as e:
            gcs_result["error"] = str(e)
            print(f"   ❌ Error fetching GCS data: {e}")
        
        return gcs_result
    
    def _convert_block_to_json(self, block_obj) -> Dict[str, Any]:
        """Convert msgspec Block object to JSON-serializable dictionary"""
        if not block_obj:
            return None
            
        try:
            # Use msgspec to convert to JSON then back to dict for proper serialization
            json_bytes = msgspec.json.encode(block_obj)
            return msgspec.json.decode(json_bytes)
        except Exception as e:
            print(f"   ⚠️ Warning: Could not convert block to JSON: {e}")
            return None
    
    def _fetch_database_data(self, block_number: int) -> Dict[str, Any]:
        """Fetch block data from database"""
        db_result = {
            "transaction_processing": [],
            "domain_events": {
                "trades": [],
                "pool_swaps": [],
                "transfers": [],
                "liquidity": [],
                "rewards": []
            },
            "positions": [],
            "error": None,
            "tables_checked": []
        }
        
        try:
            with self.repository_manager.get_session() as session:
                # Get transaction processing records
                result = session.execute(text("""
                    SELECT tx_hash, block_number, status, events_generated, 
                           tx_success, timestamp, logs_processed
                    FROM transaction_processing 
                    WHERE block_number = :block_number
                    ORDER BY timestamp
                """), {"block_number": block_number})
                
                db_result["transaction_processing"] = [dict(row._mapping) for row in result]
                print(f"   📋 Found {len(db_result['transaction_processing'])} transaction records")
                
                # Get domain events by type
                event_tables = ["trades", "pool_swaps", "transfers", "liquidity", "rewards"]
                for table_name in event_tables:
                    db_result["tables_checked"].append(table_name)
                    
                    # Basic fields that should exist in all event tables
                    base_fields = "content_id, tx_hash, block_number, created_at, updated_at, timestamp"
                    
                    # Table-specific fields - CORRECTED TO MATCH ACTUAL DATABASE SCHEMAS
                    if table_name == "trades":
                        fields = f"{base_fields}, taker, direction, base_token, base_amount, trade_type, router, swap_count"
                    elif table_name == "pool_swaps":
                        fields = f"{base_fields}, pool, taker, direction, base_token, base_amount, quote_token, quote_amount, trade_id"
                    elif table_name == "transfers":
                        fields = f"{base_fields}, token, from_address, to_address, amount, parent_id, parent_type, classification"
                    elif table_name == "liquidity":
                        fields = f"{base_fields}, pool, provider, action, base_token, base_amount, quote_token, quote_amount"
                    elif table_name == "rewards":
                        fields = f"{base_fields}, contract, recipient, token, amount, reward_type"
                    
                    result = session.execute(text(f"""
                        SELECT {fields}
                        FROM {table_name} 
                        WHERE block_number = :block_number
                        ORDER BY timestamp
                    """), {"block_number": block_number})
                    
                    records = [dict(row._mapping) for row in result]
                    db_result["domain_events"][table_name] = records
                    print(f"   📊 Found {len(records)} {table_name} records")
                
                # Get positions
                result = session.execute(text("""
                    SELECT "user", token, amount, token_id, custodian, 
                           parent_id, parent_type, content_id, tx_hash, block_number
                    FROM positions 
                    WHERE block_number = :block_number
                    ORDER BY timestamp
                """), {"block_number": block_number})
                
                db_result["positions"] = [dict(row._mapping) for row in result]
                print(f"   🏦 Found {len(db_result['positions'])} position records")
                
        except Exception as e:
            db_result["error"] = str(e)
            print(f"   ❌ Error fetching database data: {e}")
        
        return db_result
    
    def _check_consistency(self, gcs_data: Dict, db_data: Dict, detailed: bool = False) -> ConsistencyResult:
        """Check consistency between GCS and database data"""
        discrepancies = []
        
        # Get the block JSON data
        block_json = gcs_data.get("block_data")
        
        if not block_json:
            discrepancies.append("No GCS block data available for comparison")
            return ConsistencyResult(
                is_consistent=False,
                discrepancies=discrepancies,
                gcs_summary={},
                db_summary={}
            )
        
        # Extract GCS data using direct JSON access
        gcs_tx_count = len(block_json.get("transactions", {}))
        gcs_events = self._count_gcs_events(block_json)
        gcs_positions = self._count_gcs_positions(block_json)
        
        gcs_summary = {
            "transaction_count": gcs_tx_count,
            "total_events": sum(gcs_events.values()),
            "events_by_type": gcs_events,
            "total_positions": gcs_positions,
            "block_timestamp": block_json.get("timestamp"),
            "block_hash": block_json.get("hash")
        }
        
        # Database Summary
        db_tx_count = len(db_data.get("transaction_processing", []))
        db_events = {k: len(v) for k, v in db_data.get("domain_events", {}).items()}
        db_positions_count = len(db_data.get("positions", []))
        
        db_summary = {
            "transaction_count": db_tx_count,
            "total_events": sum(db_events.values()),
            "events_by_type": db_events,
            "total_positions": db_positions_count,
            "tables_available": db_data.get("tables_checked", [])
        }
        
        # Consistency Checks
        
        # 1. Transaction count
        if gcs_tx_count != db_tx_count:
            discrepancies.append(
                f"Transaction count mismatch: GCS={gcs_tx_count}, DB={db_tx_count}"
            )
        
        # 2. Position count
        if gcs_positions != db_positions_count:
            discrepancies.append(
                f"Position count mismatch: GCS={gcs_positions}, DB={db_positions_count}"
            )
        
        # 3. Event type comparison
        for event_type, gcs_count in gcs_events.items():
            db_count = db_events.get(event_type, 0)
            if gcs_count != db_count:
                discrepancies.append(
                    f"{event_type} count mismatch: GCS={gcs_count}, DB={db_count}"
                )
        
        is_consistent = len(discrepancies) == 0
        
        return ConsistencyResult(
            is_consistent=is_consistent,
            discrepancies=discrepancies,
            gcs_summary=gcs_summary,
            db_summary=db_summary
        )
    
    def _count_gcs_events(self, block_json: Dict) -> Dict[str, int]:
        """Count events by type from GCS block JSON data"""
        event_counts = {
            'trades': 0,
            'pool_swaps': 0,
            'transfers': 0,
            'liquidity': 0,
            'rewards': 0
        }
        
        if not block_json or not block_json.get("transactions"):
            return event_counts
        
        # Access transactions from JSON
        for tx_hash, transaction in block_json["transactions"].items():
            if not transaction.get("events"):
                continue
                
            # Access events from transaction JSON
            for event_id, event in transaction["events"].items():
                # Get event type from the class name or type field
                event_type_name = event.get("type", "").lower()
                
                # Map event class names to our categories
                if event_type_name == 'trade':
                    event_counts['trades'] += 1
                elif event_type_name == 'poolswap':
                    event_counts['pool_swaps'] += 1
                elif event_type_name in ['transfer', 'unknowntransfer']:
                    event_counts['transfers'] += 1
                elif event_type_name == 'liquidity':
                    event_counts['liquidity'] += 1
                elif event_type_name == 'reward':
                    event_counts['rewards'] += 1
        
        return event_counts
    
    def _count_gcs_positions(self, block_json: Dict) -> int:
        """Count total positions from GCS block JSON data"""
        total_positions = 0
        
        if not block_json or not block_json.get("transactions"):
            return total_positions
        
        # Access transactions from JSON
        for tx_hash, transaction in block_json["transactions"].items():
            if transaction.get("positions"):
                total_positions += len(transaction["positions"])
        
        return total_positions
    
    def _display_summary(self, report: Dict) -> None:
        """Display a summary of the analysis results"""
        consistency = report["consistency_check"]
        
        print(f"\n📊 Analysis Summary")
        print("-" * 30)
        print(f"Block: {report['metadata']['block_number']}")
        print(f"Model: {report['metadata']['model_name']}")
        print(f"Consistent: {'✅ YES' if consistency['is_consistent'] else '❌ NO'}")
        
        # GCS Summary
        gcs_summary = consistency["gcs_summary"]
        print(f"\n📥 GCS Data:")
        print(f"   Transactions: {gcs_summary.get('transaction_count', 0)}")
        print(f"   Total Events: {gcs_summary.get('total_events', 0)}")
        print(f"   Total Positions: {gcs_summary.get('total_positions', 0)}")
        
        # Database Summary
        db_summary = consistency["database_summary"]
        print(f"\n🗄️ Database Data:")
        print(f"   Transactions: {db_summary.get('transaction_count', 0)}")
        print(f"   Total Events: {db_summary.get('total_events', 0)}")
        print(f"   Total Positions: {db_summary.get('total_positions', 0)}")
        
        # Event breakdown
        gcs_events = gcs_summary.get("events_by_type", {})
        db_events = db_summary.get("events_by_type", {})
        
        if gcs_events or db_events:
            print(f"\n📋 Event Breakdown:")
            all_event_types = set(gcs_events.keys()) | set(db_events.keys())
            for event_type in sorted(all_event_types):
                gcs_count = gcs_events.get(event_type, 0)
                db_count = db_events.get(event_type, 0)
                status = "✅" if gcs_count == db_count else "❌"
                print(f"   {status} {event_type}: GCS={gcs_count}, DB={db_count}")
        
        # Discrepancies
        if consistency["discrepancies"]:
            print(f"\n⚠️ Discrepancies Found:")
            for discrepancy in consistency["discrepancies"]:
                print(f"   • {discrepancy}")
        else:
            print(f"\n✅ No discrepancies found - data is consistent!")


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Analyze end-to-end test results")
    parser.add_argument("block_number", type=int, help="Block number to analyze")
    parser.add_argument("--detailed", action="store_true", help="Include detailed transaction analysis")
    parser.add_argument("--output", help="Output file name (optional)")
    parser.add_argument("--model", help="Model name (optional, uses env var if not specified)")
    
    args = parser.parse_args()
    
    try:
        # Initialize analyzer
        analyzer = EndToEndResultsAnalyzer(model_name=args.model)
        
        # Perform analysis
        report = analyzer.analyze_block(
            block_number=args.block_number,
            include_detailed=args.detailed
        )
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())