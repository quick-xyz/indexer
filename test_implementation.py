#!/usr/bin/env python3
"""
Quick test to see what's working in the current implementation
"""

import sys
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    print("🔍 Testing current implementation...")
    
    # Test 1: Can we create IndexerConfig with sources?
    print("\n1. Testing IndexerConfig with sources...")
    try:
        from indexer.admin.admin_context import AdminContext
        from indexer.core.config_service import ConfigService
        from indexer.core.config import IndexerConfig
        
        admin_context = AdminContext()
        config_service = ConfigService(admin_context.infrastructure_db_manager)
        
        # Try to create IndexerConfig
        config = IndexerConfig.from_model("blub_test", config_service)
        
        # Check if sources are loaded
        if hasattr(config, 'sources'):
            print(f"   ✅ IndexerConfig has sources: {len(config.sources)} sources loaded")
            for source_id, source in config.sources.items():
                print(f"      - Source {source_id}: {source.name}")
        else:
            print("   ❌ IndexerConfig missing sources field")
            
        # Test source methods if they exist
        if hasattr(config, 'get_source_by_id'):
            primary_source = config.get_primary_source()
            if primary_source:
                print(f"   ✅ Primary source: {primary_source.name}")
            else:
                print("   ⚠️  No primary source found")
        else:
            print("   ❌ IndexerConfig missing source methods")
            
    except Exception as e:
        print(f"   ❌ IndexerConfig test failed: {e}")
    
    # Test 2: Can we create GCSHandler?
    print("\n2. Testing GCSHandler...")
    try:
        from indexer.core.container import IndexerContainer
        from indexer.storage.gcs_handler import GCSHandler
        
        container = IndexerContainer("blub_test")
        gcs_handler = container.get(GCSHandler)
        
        print(f"   ✅ GCSHandler created: {gcs_handler.__class__.__name__}")
        
        # Test if get_rpc_block accepts source parameter
        import inspect
        sig = inspect.signature(gcs_handler.get_rpc_block)
        params = list(sig.parameters.keys())
        print(f"   ℹ️  get_rpc_block parameters: {params}")
        
        if 'source' in params or 'source_id' in params:
            print("   ✅ GCSHandler supports source parameters")
        else:
            print("   ❌ GCSHandler needs source parameter updates")
            
    except Exception as e:
        print(f"   ❌ GCSHandler test failed: {e}")
    
    # Test 3: ConfigService source methods
    print("\n3. Testing ConfigService source methods...")
    try:
        admin_context = AdminContext()
        config_service = ConfigService(admin_context.infrastructure_db_manager)
        
        # Test source methods
        methods_to_check = [
            'get_source_by_id',
            'get_sources_for_model', 
            'get_model_source_configuration'
        ]
        
        for method_name in methods_to_check:
            if hasattr(config_service, method_name):
                print(f"   ✅ {method_name} method exists")
            else:
                print(f"   ❌ {method_name} method missing")
        
        # Test getting sources
        sources = config_service.get_sources_for_model("blub_test")
        print(f"   ✅ Found {len(sources)} sources for blub_test")
        
    except Exception as e:
        print(f"   ❌ ConfigService test failed: {e}")
    
    print("\n🎯 Summary:")
    print("If you see ❌ errors above, those components need updates from the original artifacts.")
    print("If you see ✅ checkmarks, those components are already working!")
        
except Exception as e:
    print(f"💥 Test script failed: {e}")