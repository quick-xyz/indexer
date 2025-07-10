#!/usr/bin/env python3
"""
Debug Domain Event Writer

Test the domain event writer with a simple event to see exactly what's failing.
"""

import sys
import os
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer.cli.context import CLIContext
from indexer.database.writers.domain_event_writer import DomainEventWriter
from indexer.database.repository import RepositoryManager
from indexer.types.new import EvmHash, DomainEventId
from indexer.database.indexer.tables.events.trade import TradeDirection, TradeType
from indexer.database.indexer.tables.events.liquidity import LiquidityAction
from indexer.database.indexer.tables.events.reward import RewardType
from indexer.core.logging_config import IndexerLogger, log_with_context
import logging


class DomainEventWriterDebugger:
    """Debug the domain event writer with simple test data"""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.logger = IndexerLogger.get_logger('tools.domain_event_writer_debug')
        self.context = CLIContext()
        
        # Get database manager and writer
        self.model_db = self.context.get_model_db_manager(model_name)
        self.infrastructure_db = self.context.infrastructure_db_manager
        
        # Create repository manager and writer
        self.repository_manager = RepositoryManager(
            model_db_manager=self.model_db,
            infrastructure_db_manager=self.infrastructure_db
        )
        self.domain_event_writer = DomainEventWriter(self.repository_manager)
        
        log_with_context(self.logger, logging.INFO, "DomainEventWriterDebugger initialized",
                        model_name=model_name)
    
    def test_simple_enum_conversion(self):
        """Test enum conversion logic directly"""
        print("\n🔍 Testing Enum Conversion Logic")
        print("-" * 40)
        
        # Test enum conversions
        test_cases = [
            ("direction", "buy", TradeDirection),
            ("trade_type", "trade", TradeType),
            ("action", "add", LiquidityAction),
            ("reward_type", "fees", RewardType),
        ]
        
        for field_name, string_value, enum_class in test_cases:
            try:
                # Find enum member by value
                enum_instance = None
                for enum_member in enum_class:
                    if enum_member.value == string_value:
                        enum_instance = enum_member
                        break
                
                if enum_instance:
                    print(f"✅ {field_name}: '{string_value}' → {enum_instance}")
                else:
                    print(f"❌ {field_name}: '{string_value}' → No matching enum member")
                    print(f"   Available values: {[m.value for m in enum_class]}")
                    
            except Exception as e:
                print(f"❌ {field_name}: Error - {e}")
    
    def test_repository_access(self):
        """Test that repositories can be accessed"""
        print("\n🔍 Testing Repository Access")
        print("-" * 40)
        
        try:
            # Test getting repositories
            trades_repo = self.repository_manager.trades
            print(f"✅ Trades repository: {trades_repo}")
            
            transfers_repo = self.repository_manager.transfers  
            print(f"✅ Transfers repository: {transfers_repo}")
            
            liquidity_repo = self.repository_manager.liquidity
            print(f"✅ Liquidity repository: {liquidity_repo}")
            
            # Test database connection
            with self.repository_manager.get_session() as session:
                print("✅ Database session created successfully")
            
        except Exception as e:
            print(f"❌ Repository access failed: {e}")
            import traceback
            print(traceback.format_exc())
    
    def test_simple_event_creation(self):
        """Test creating a simple event directly"""
        print("\n🔍 Testing Simple Event Creation")
        print("-" * 40)
        
        try:
            # Create a simple transfer event data
            test_event_data = {
                'content_id': DomainEventId('test_12345'),
                'tx_hash': EvmHash('0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef'),
                'block_number': 12345,
                'timestamp': 1641234567,
                'token': '0x1234567890abcdef1234567890abcdef12345678',
                'from_address': '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd',
                'to_address': '0x9876543210987654321098765432109876543210',
                'amount': '1000000000000000000'
            }
            
            # Try to create using repository directly
            with self.repository_manager.get_transaction() as session:
                transfer_record = self.repository_manager.transfers.create(session, **test_event_data)
                print(f"✅ Transfer created: {transfer_record}")
                
                # Don't commit - this is just a test
                session.rollback()
                print("✅ Transaction rolled back (test only)")
                
        except Exception as e:
            print(f"❌ Event creation failed: {e}")
            import traceback
            print(traceback.format_exc())
    
    def test_domain_event_writer_directly(self):
        """Test the domain event writer with a mock event"""
        print("\n🔍 Testing Domain Event Writer")
        print("-" * 40)
        
        try:
            # Create a mock event object
            class MockEvent:
                def __init__(self):
                    self.token = '0x1234567890abcdef1234567890abcdef12345678'
                    self.from_address = '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd'
                    self.to_address = '0x9876543210987654321098765432109876543210'
                    self.amount = '1000000000000000000'
                
                def __class__(self):
                    return type('Transfer', (), {})
            
            mock_event = MockEvent()
            events = {DomainEventId('test_12345'): mock_event}
            positions = {}
            
            # Test the writer
            tx_hash = EvmHash('0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef')
            
            events_written, positions_written, events_skipped = self.domain_event_writer.write_transaction_results(
                tx_hash=tx_hash,
                block_number=12345,
                timestamp=1641234567,
                events=events,
                positions=positions,
                tx_success=True
            )
            
            print(f"✅ Domain event writer test completed:")
            print(f"   Events written: {events_written}")
            print(f"   Positions written: {positions_written}")
            print(f"   Events skipped: {events_skipped}")
            
        except Exception as e:
            print(f"❌ Domain event writer test failed: {e}")
            import traceback
            print(traceback.format_exc())
    
    def run_full_debug(self):
        """Run all debug tests"""
        print(f"\n🔧 DOMAIN EVENT WRITER DEBUG - MODEL: {self.model_name}")
        print("=" * 60)
        
        self.test_simple_enum_conversion()
        self.test_repository_access()
        self.test_simple_event_creation()
        self.test_domain_event_writer_directly()
        
        print(f"\n✅ Debug completed!")


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/debug_domain_event_writer.py <model_name>")
        print("Example: python scripts/debug_domain_event_writer.py blub_test")
        sys.exit(1)
    
    model_name = sys.argv[1]
    
    try:
        debugger = DomainEventWriterDebugger(model_name)
        debugger.run_full_debug()
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()