# testing/diagnostics/event_troubleshooter.py
"""
Event Generation Troubleshooter
Diagnoses why signals are generated but NO EVENTS are created from them
"""

import sys
import json
from pathlib import Path
from typing import Dict, Any

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from testing import get_testing_environment
from indexer.storage.gcs_handler import GCSHandler
from indexer.decode.block_decoder import BlockDecoder
from indexer.transform.manager import TransformManager
from indexer.transform.registry import TransformRegistry


class EventTroubleshooter:
    """Diagnose signal-to-event conversion issues"""
    
    def __init__(self):
        self.testing_env = get_testing_environment(log_level="ERROR")
        self.storage_handler = self.testing_env.get_service(GCSHandler)
        self.block_decoder = self.testing_env.get_service(BlockDecoder)
        self.transform_manager = self.testing_env.get_service(TransformManager)
        self.transformer_registry = self.testing_env.get_service(TransformRegistry)
    
    def diagnose_event_generation(self, tx_hash: str, block_number: int):
        """Focus on why signals don't become events"""
        
        print(f"🔬 EVENT GENERATION TROUBLESHOOTER")
        print(f"Transaction: {tx_hash[:10]}... Block: {block_number}")
        print("=" * 60)
        
        # Get transaction
        raw_block = self.storage_handler.get_rpc_block(block_number)
        decoded_block = self.block_decoder.decode_block(raw_block)
        transaction = decoded_block.transactions[tx_hash]
        
        # Process transaction
        success, processed_tx = self.transform_manager.process_transaction(transaction)
        
        signal_count = len(processed_tx.signals) if processed_tx.signals else 0
        event_count = len(processed_tx.events) if processed_tx.events else 0
        error_count = len(processed_tx.errors) if processed_tx.errors else 0
        
        print(f"📊 RESULTS: {signal_count} signals → {event_count} events ({error_count} errors)")
        
        if signal_count == 0:
            print("❌ PROBLEM: No signals generated - this contradicts your statement")
            return
        
        if event_count > 0:
            print("✅ Events ARE being generated - check your test setup")
            self._show_generated_events(processed_tx.events)
            return
        
        # Core issue: signals exist but no events
        print("\n🎯 CORE ISSUE: Signals generated but NO events created")
        self._diagnose_signal_to_event_failure(processed_tx)
    
    def _show_generated_events(self, events):
        """Show what events were actually generated"""
        print("\n📋 Generated Events:")
        for event_id, event in events.items():
            event_type = type(event).__name__
            print(f"   {event_id}: {event_type}")
    
    def _diagnose_signal_to_event_failure(self, processed_tx):
        """Diagnose why signals don't convert to events"""
        
        print("\n🔍 SIGNAL ANALYSIS:")
        
        # Analyze each signal
        pattern_groups = {}
        for signal_idx, signal in processed_tx.signals.items():
            pattern = signal.pattern
            signal_type = type(signal).__name__
            
            if pattern not in pattern_groups:
                pattern_groups[pattern] = []
            pattern_groups[pattern].append({
                'index': signal_idx,
                'type': signal_type,
                'signal': signal
            })
            
            print(f"   Signal {signal_idx}: {signal_type} (pattern: {pattern})")
        
        print(f"\n🔗 PATTERN PROCESSING:")
        
        # Check each pattern
        for pattern_name, signals in pattern_groups.items():
            print(f"\n   Pattern: {pattern_name} ({len(signals)} signals)")
            
            # Check if pattern processor exists
            pattern_processor = self.transformer_registry.get_pattern(pattern_name)
            
            if not pattern_processor:
                print(f"      ❌ NO PATTERN PROCESSOR for '{pattern_name}'")
                print(f"      🔧 FIX: Add {pattern_name} to TransformRegistry._load_pattern_classes()")
                continue
            
            print(f"      ✅ Pattern processor: {type(pattern_processor).__name__}")
            
            # Test pattern processing
            self._test_pattern_processing(pattern_name, signals, pattern_processor)
    
    def _test_pattern_processing(self, pattern_name, signals, pattern_processor):
        """Test if pattern processor can create events"""
        
        print(f"      🧪 Testing pattern processing...")
        
        # Special handling for trade patterns
        if pattern_name in ["Swap_A", "Route"]:
            print(f"      ℹ️  Trade pattern - processed via _process_trade()")
            self._diagnose_trade_processing(signals)
            return
        
        # Regular pattern processing
        for signal_info in signals:
            signal = signal_info['signal']
            
            try:
                # This is tricky - we'd need to mock the context
                # For now, just check if the pattern has process_signal method
                if hasattr(pattern_processor, 'process_signal'):
                    print(f"         ✅ Has process_signal method")
                else:
                    print(f"         ❌ Missing process_signal method")
                    
            except Exception as e:
                print(f"         ❌ Pattern processing error: {e}")
    
    def _diagnose_trade_processing(self, signals):
        """Diagnose trade pattern processing specifically"""
        
        print(f"      🔍 Trade Pattern Analysis:")
        
        # Check for Route signals (user intent)
        route_signals = [s for s in signals if 'Route' in s['type']]
        swap_signals = [s for s in signals if 'Swap' in s['type']]
        batch_signals = [s for s in signals if 'Batch' in s['type']]
        
        print(f"         Route signals: {len(route_signals)}")
        print(f"         Swap signals: {len(swap_signals)}")  
        print(f"         Batch signals: {len(batch_signals)}")
        
        if len(route_signals) == 0 and len(swap_signals) == 0:
            print(f"         ❌ No route or swap signals for trade processing")
            return
        
        # The issue might be in TransformManager._process_trade()
        print(f"         ⚠️  Check TransformManager._process_trade() logic")
        print(f"         ⚠️  Trade processing might be failing silently")
    
    def _show_detailed_recommendations(self, processed_tx):
        """Show specific recommendations"""
        
        print(f"\n💡 SPECIFIC RECOMMENDATIONS:")
        
        if not processed_tx.signals:
            print(f"   ❌ This shouldn't happen - you said signals work")
            return
        
        # Check patterns
        patterns = set(signal.pattern for signal in processed_tx.signals.values())
        
        for pattern in patterns:
            pattern_processor = self.transformer_registry.get_pattern(pattern)
            
            if not pattern_processor:
                print(f"   🔧 ADD MISSING PATTERN: {pattern}")
                print(f"      - Add to transform/patterns/__init__.py")
                print(f"      - Add to TransformRegistry._load_pattern_classes()")
            else:
                print(f"   🔍 DEBUG PATTERN: {pattern}")
                print(f"      - Enable logging in {type(pattern_processor).__name__}")
                print(f"      - Check process_signal() method")
        
        # Check for trade processing
        trade_patterns = [p for p in patterns if p in ["Swap_A", "Route"]]
        if trade_patterns:
            print(f"   🔍 DEBUG TRADE PROCESSING:")
            print(f"      - Check TransformManager._process_trade() method")
            print(f"      - Enable DEBUG logging to see trade signal processing")
            print(f"      - Look for silent failures in batch signal aggregation")


def main():
    if len(sys.argv) != 3:
        print("Usage: python testing/diagnostics/event_troubleshooter.py <tx_hash> <block_number>")
        sys.exit(1)
    
    tx_hash = sys.argv[1]
    block_number = int(sys.argv[2])
    
    troubleshooter = EventTroubleshooter()
    troubleshooter.diagnose_event_generation(tx_hash, block_number)


if __name__ == "__main__":
    main()