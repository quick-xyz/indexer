#!/usr/bin/env python3
"""
Block Processing Validator

Validates the complete processing pipeline by checking:
1. Are blocks being processed and saved to GCS?
2. Are processing records being created in database?
3. Are domain events being written to database?
4. Where is the data loss occurring?
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
from indexer.database.connection import ModelDatabaseManager
from sqlalchemy import text


class BlockProcessingValidator:
    """Validate the complete block processing pipeline"""
    
    def __init__(self, model_name: str = None):
        """Initialize with DI container"""
        self.model_name = model_name
        
        # Initialize DI container
        self.container = create_indexer(model_name=model_name)
        self.config = self.container._config
        
        # Get services
        self.gcs = self.container.get(GCSHandler)
        self.model_db = self.container.get(ModelDatabaseManager)
        
        print(f"✅ Initialized validator for model: {self.config.model_name}")
    
    def validate_pipeline_health(self):
        """Comprehensive pipeline health check"""
        print("🏥 Block Processing Pipeline Health Check")
        print("=" * 70)
        
        # Step 1: Check GCS storage
        print("\n1️⃣ Checking GCS Storage...")
        gcs_health = self._check_gcs_health()
        
        # Step 2: Check database tables
        print("\n2️⃣ Checking Database Tables...")
        db_health = self._check_database_health()
        
        # Step 3: Check data flow
        print("\n3️⃣ Checking Data Flow...")
        flow_health = self._check_data_flow()
        
        # Step 4: Overall assessment
        print("\n4️⃣ Overall Assessment...")
        self._print_health_summary(gcs_health, db_health, flow_health)
    
    def _check_gcs_health(self) -> Dict[str, Any]:
        """Check GCS storage health"""
        health = {
            'processing_blocks': 0,
            'complete_blocks': 0,
            'recent_complete_blocks': [],
            'storage_accessible': False,
            'issues': []
        }
        
        try:
            # Check if GCS is accessible
            processing_blocks = self.gcs.list_processing_blocks()
            complete_blocks = self.gcs.list_complete_blocks()
            
            health['storage_accessible'] = True
            health['processing_blocks'] = len(processing_blocks)
            health['complete_blocks'] = len(complete_blocks)
            health['recent_complete_blocks'] = sorted(complete_blocks, reverse=True)[:5]
            
            print(f"   ✅ GCS accessible")
            print(f"   📊 Processing blocks: {health['processing_blocks']:,}")
            print(f"   📊 Complete blocks: {health['complete_blocks']:,}")
            
            if health['complete_blocks'] == 0:
                health['issues'].append("No complete blocks found in GCS")
                print(f"   ⚠️ No complete blocks found!")
            else:
                print(f"   📊 Recent complete: {health['recent_complete_blocks']}")
            
        except Exception as e:
            health['issues'].append(f"GCS access failed: {e}")
            print(f"   ❌ GCS access failed: {e}")
        
        return health
    
    def _check_database_health(self) -> Dict[str, Any]:
        """Check database table health"""
        health = {
            'tables_accessible': False,
            'table_counts': {},
            'processing_records': 0,
            'domain_events': 0,
            'recent_processing': [],
            'issues': []
        }
        
        try:
            with self.model_db.get_session() as session:
                health['tables_accessible'] = True
                
                # Check processing tables
                result = session.execute(text("SELECT COUNT(*) FROM transaction_processing"))
                health['processing_records'] = result.scalar()
                
                result = session.execute(text("SELECT COUNT(*) FROM block_processing"))
                health['table_counts']['block_processing'] = result.scalar()
                
                # Check domain event tables
                domain_tables = ['trades', 'pool_swaps', 'transfers', 'liquidity', 'rewards', 'positions']
                total_domain_events = 0
                
                for table in domain_tables:
                    try:
                        result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        count = result.scalar()
                        health['table_counts'][table] = count
                        total_domain_events += count
                    except Exception as e:
                        health['table_counts'][table] = f"Error: {e}"
                        health['issues'].append(f"Table {table} error: {e}")
                
                health['domain_events'] = total_domain_events
                
                # Get recent processing records
                result = session.execute(text("""
                    SELECT block_number, status, events_generated 
                    FROM transaction_processing 
                    ORDER BY block_number DESC 
                    LIMIT 5
                """))
                health['recent_processing'] = [dict(row._mapping) for row in result]
                
                print(f"   ✅ Database accessible")
                print(f"   📊 Transaction processing records: {health['processing_records']:,}")
                print(f"   📊 Block processing records: {health['table_counts'].get('block_processing', 0):,}")
                print(f"   📊 Total domain events: {health['domain_events']:,}")
                
                if health['processing_records'] == 0:
                    health['issues'].append("No transaction processing records found")
                    print(f"   ⚠️ No processing records found!")
                
                if health['domain_events'] == 0:
                    health['issues'].append("No domain events found in any table")
                    print(f"   ⚠️ No domain events found!")
                
        except Exception as e:
            health['issues'].append(f"Database access failed: {e}")
            print(f"   ❌ Database access failed: {e}")
        
        return health
    
    def _check_data_flow(self) -> Dict[str, Any]:
        """Check data flow between GCS and database"""
        flow = {
            'gcs_to_db_match': False,
            'blocks_in_both': 0,
            'blocks_only_gcs': 0,
            'blocks_only_db': 0,
            'data_consistency': False,
            'sample_comparisons': [],
            'issues': []
        }
        
        try:
            # Get recent complete blocks from GCS
            complete_blocks = self.gcs.list_complete_blocks()
            recent_complete = sorted(complete_blocks, reverse=True)[:10]
            
            # Get recent processing records from database
            with self.model_db.get_session() as session:
                result = session.execute(text("""
                    SELECT DISTINCT block_number 
                    FROM transaction_processing 
                    ORDER BY block_number DESC 
                    LIMIT 20
                """))
                db_blocks = [row[0] for row in result]
            
            # Compare
            gcs_set = set(recent_complete)
            db_set = set(db_blocks)
            
            flow['blocks_in_both'] = len(gcs_set & db_set)
            flow['blocks_only_gcs'] = len(gcs_set - db_set)
            flow['blocks_only_db'] = len(db_set - gcs_set)
            flow['gcs_to_db_match'] = flow['blocks_only_gcs'] == 0
            
            print(f"   📊 Blocks in both GCS and DB: {flow['blocks_in_both']}")
            print(f"   📊 Blocks only in GCS: {flow['blocks_only_gcs']}")
            print(f"   📊 Blocks only in DB: {flow['blocks_only_db']}")
            
            # Sample detailed comparisons
            for block_num in list(gcs_set & db_set)[:3]:
                comparison = self._compare_block_details(block_num)
                flow['sample_comparisons'].append(comparison)
                
                gcs_events = comparison['gcs_events']
                db_events = comparison['db_events']
                
                print(f"   🔍 Block {block_num}: GCS={gcs_events} events, DB={db_events} events")
                
                if gcs_events != db_events:
                    flow['issues'].append(f"Block {block_num} event count mismatch: GCS={gcs_events}, DB={db_events}")
            
            if flow['blocks_only_gcs'] > 0:
                flow['issues'].append(f"{flow['blocks_only_gcs']} blocks exist in GCS but not in database")
                print(f"   ⚠️ {flow['blocks_only_gcs']} blocks missing from database")
            
        except Exception as e:
            flow['issues'].append(f"Data flow check failed: {e}")
            print(f"   ❌ Data flow check failed: {e}")
        
        return flow
    
    def _compare_block_details(self, block_number: int) -> Dict[str, Any]:
        """Compare detailed block data between GCS and database"""
        comparison = {
            'block_number': block_number,
            'gcs_events': 0,
            'db_events': 0,
            'gcs_positions': 0,
            'db_positions': 0,
            'gcs_transactions': 0,
            'db_transactions': 0
        }
        
        # Get GCS data
        block_data = self.gcs.get_complete_block(block_number)
        if block_data and block_data.transactions:
            comparison['gcs_transactions'] = len(block_data.transactions)
            
            for tx in block_data.transactions.values():
                if hasattr(tx, 'events') and tx.events:
                    comparison['gcs_events'] += len(tx.events)
                if hasattr(tx, 'positions') and tx.positions:
                    comparison['gcs_positions'] += len(tx.positions)
        
        # Get database data
        with self.model_db.get_session() as session:
            # Transaction count
            result = session.execute(text("""
                SELECT COUNT(*) FROM transaction_processing 
                WHERE block_number = :block_num
            """), {'block_num': block_number})
            comparison['db_transactions'] = result.scalar()
            
            # Domain events count
            domain_tables = ['trades', 'pool_swaps', 'transfers', 'liquidity', 'rewards']
            for table in domain_tables:
                try:
                    result = session.execute(text(f"""
                        SELECT COUNT(*) FROM {table} WHERE block_number = :block_num
                    """), {'block_num': block_number})
                    comparison['db_events'] += result.scalar()
                except:
                    pass
            
            # Positions count
            try:
                result = session.execute(text("""
                    SELECT COUNT(*) FROM positions WHERE block_number = :block_num
                """), {'block_num': block_number})
                comparison['db_positions'] = result.scalar()
            except:
                pass
        
        return comparison
    
    def _print_health_summary(self, gcs_health: Dict, db_health: Dict, flow_health: Dict):
        """Print overall health summary"""
        print("🏥 PIPELINE HEALTH SUMMARY")
        print("=" * 70)
        
        # Determine overall health
        all_issues = gcs_health['issues'] + db_health['issues'] + flow_health['issues']
        
        if not all_issues:
            print("✅ PIPELINE HEALTHY - No issues detected")
        else:
            print(f"⚠️ PIPELINE ISSUES DETECTED - {len(all_issues)} issues found")
        
        print(f"\n📊 Key Metrics:")
        print(f"   GCS complete blocks: {gcs_health['complete_blocks']:,}")
        print(f"   Database processing records: {db_health['processing_records']:,}")
        print(f"   Database domain events: {db_health['domain_events']:,}")
        print(f"   Blocks in both systems: {flow_health['blocks_in_both']}")
        
        if all_issues:
            print(f"\n⚠️ Issues Found:")
            for i, issue in enumerate(all_issues, 1):
                print(f"   {i}. {issue}")
            
            print(f"\n💡 Recommended Actions:")
            
            if gcs_health['complete_blocks'] == 0:
                print("   1. Check if indexing pipeline is running")
                print("   2. Verify GCS bucket access and configuration")
            
            if db_health['processing_records'] == 0:
                print("   3. Check if database persistence is working")
                print("   4. Verify database connection and permissions")
            
            if flow_health['blocks_only_gcs'] > 0:
                print("   5. Check domain event writer for errors")
                print("   6. Verify transaction processing logic")
            
            if db_health['domain_events'] == 0 and gcs_health['complete_blocks'] > 0:
                print("   7. Check if events are being generated during transformation")
                print("   8. Verify domain event writer is being called")
    
    def diagnose_specific_issue(self, issue_type: str):
        """Diagnose specific pipeline issues"""
        if issue_type == "no_blocks":
            self._diagnose_no_blocks()
        elif issue_type == "no_events":
            self._diagnose_no_events()
        elif issue_type == "missing_trades":
            self._diagnose_missing_trades()
        else:
            print(f"Unknown issue type: {issue_type}")
    
    def _diagnose_no_blocks(self):
        """Diagnose why no blocks are being processed"""
        print("🔍 Diagnosing: No Blocks Being Processed")
        print("=" * 70)
        
        # Check if any blocks exist in any storage
        processing_blocks = self.gcs.list_processing_blocks()
        complete_blocks = self.gcs.list_complete_blocks()
        
        print(f"Processing blocks in GCS: {len(processing_blocks)}")
        print(f"Complete blocks in GCS: {len(complete_blocks)}")
        
        if len(processing_blocks) == 0 and len(complete_blocks) == 0:
            print("❌ No blocks found in any GCS storage")
            print("💡 Check:")
            print("   1. Is the indexing pipeline running?")
            print("   2. Are there jobs in the processing queue?")
            print("   3. Is GCS bucket configured correctly?")
        
        # Check processing jobs
        with self.model_db.get_session() as session:
            result = session.execute(text("""
                SELECT status, COUNT(*) as count 
                FROM processing_jobs 
                GROUP BY status
            """))
            job_counts = dict(result.fetchall())
            
            print(f"\nProcessing job status:")
            for status, count in job_counts.items():
                print(f"   {status}: {count}")
    
    def _diagnose_no_events(self):
        """Diagnose why no events are being generated"""
        print("🔍 Diagnosing: No Events Being Generated")
        print("=" * 70)
        
        # Sample recent complete blocks to see if they should have events
        complete_blocks = self.gcs.list_complete_blocks()
        if not complete_blocks:
            print("❌ No complete blocks to analyze")
            return
        
        sample_blocks = sorted(complete_blocks, reverse=True)[:5]
        print(f"Analyzing {len(sample_blocks)} recent complete blocks...")
        
        for block_num in sample_blocks:
            print(f"\n📋 Block {block_num}:")
            
            block_data = self.gcs.get_complete_block(block_num)
            if not block_data:
                print("   ❌ Could not retrieve block data")
                continue
            
            if not block_data.transactions:
                print("   📊 No transactions in block")
                continue
            
            tx_count = len(block_data.transactions)
            events_count = 0
            signals_count = 0
            errors_count = 0
            
            for tx in block_data.transactions.values():
                if hasattr(tx, 'events') and tx.events:
                    events_count += len(tx.events)
                if hasattr(tx, 'signals') and tx.signals:
                    signals_count += len(tx.signals)
                if hasattr(tx, 'errors') and tx.errors:
                    errors_count += len(tx.errors)
            
            print(f"   📊 Transactions: {tx_count}")
            print(f"   📊 Signals: {signals_count}")
            print(f"   📊 Events: {events_count}")
            print(f"   📊 Errors: {errors_count}")
            
            if signals_count > 0 and events_count == 0:
                print("   ⚠️ Signals generated but no events - transformation issue?")
            elif tx_count > 0 and signals_count == 0:
                print("   ⚠️ Transactions exist but no signals - decoding issue?")
    
    def _diagnose_missing_trades(self):
        """Diagnose why trades/pool_swaps are missing but transfers/positions exist"""
        print("🔍 Diagnosing: Missing Trades/Pool Swaps")
        print("=" * 70)
        
        with self.model_db.get_session() as session:
            # Get table counts
            result = session.execute(text("SELECT COUNT(*) FROM transfers"))
            transfer_count = result.scalar()
            
            result = session.execute(text("SELECT COUNT(*) FROM positions"))
            position_count = result.scalar()
            
            result = session.execute(text("SELECT COUNT(*) FROM trades"))
            trade_count = result.scalar()
            
            result = session.execute(text("SELECT COUNT(*) FROM pool_swaps"))
            swap_count = result.scalar()
            
            print(f"Database event counts:")
            print(f"   Transfers: {transfer_count:,}")
            print(f"   Positions: {position_count:,}")
            print(f"   Trades: {trade_count:,}")
            print(f"   Pool Swaps: {swap_count:,}")
            
            if transfer_count > 0 and position_count > 0 and trade_count == 0:
                print(f"\n⚠️ Issue: Transfers/Positions exist but no Trades")
                print(f"💡 This suggests:")
                print(f"   1. Transform pipeline is working (generates transfers/positions)")
                print(f"   2. Database persistence is working (saves transfers/positions)")
                print(f"   3. Trade-specific events are not being generated")
                print(f"   4. Check transformer configuration for swap/trade events")


def main():
    """Main execution"""
    if len(sys.argv) < 2:
        print("Usage: python block_processing_validator.py <command> [model_name] [args]")
        print("Commands:")
        print("  health [model] - Full pipeline health check")
        print("  diagnose [model] <issue_type> - Diagnose specific issue")
        print("    Issue types: no_blocks, no_events, missing_trades")
        return 1
    
    command = sys.argv[1]
    
    # Parse arguments
    model_name = None
    if len(sys.argv) > 2 and not sys.argv[2] in ['no_blocks', 'no_events', 'missing_trades']:
        model_name = sys.argv[2]
        args_start = 3
    else:
        args_start = 2
    
    try:
        validator = BlockProcessingValidator(model_name=model_name)
        
        if command == "health":
            validator.validate_pipeline_health()
            
        elif command == "diagnose":
            if len(sys.argv) <= args_start:
                print("Error: diagnose command requires issue_type")
                return 1
            issue_type = sys.argv[args_start]
            validator.diagnose_specific_issue(issue_type)
            
        else:
            print(f"Unknown command: {command}")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())