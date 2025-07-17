#!/usr/bin/env python3
"""
Simple Diagnostic - Just check what exists and what doesn't

This script will help us understand:
1. What files exist
2. What imports work/don't work
3. What the current structure looks like

No assumptions, just facts.
"""

import sys
from pathlib import Path
import importlib
import traceback

def check_file_structure():
    """Check what files actually exist"""
    print("📁 File Structure Check")
    print("=" * 30)
    
    project_root = Path.cwd()
    indexer_dir = project_root / "indexer"
    
    print(f"Project root: {project_root}")
    print(f"Indexer dir exists: {indexer_dir.exists()}")
    
    if indexer_dir.exists():
        key_files = [
            "indexer/types/__init__.py",
            "indexer/types/config.py", 
            "indexer/core/config.py",
            "indexer/core/di_container.py",
            "indexer/database/shared/tables/config.py",
            "indexer/database/connection.py"
        ]
        
        print("\nKey files:")
        for file_path in key_files:
            full_path = project_root / file_path
            exists = full_path.exists()
            print(f"  {file_path}: {'✅' if exists else '❌'}")
            
            if exists and full_path.suffix == '.py':
                try:
                    with open(full_path) as f:
                        content = f.read()
                    lines = len(content.split('\n'))
                    print(f"    ({lines} lines)")
                except Exception as e:
                    print(f"    (can't read: {e})")
    
    return indexer_dir.exists()

def check_imports():
    """Check what imports work"""
    print("\n🔍 Import Check")
    print("=" * 30)
    
    # Try adding the current directory to path
    sys.path.insert(0, str(Path.cwd()))
    
    imports_to_test = [
        "indexer",
        "indexer.types",
        "indexer.core",
        "indexer.core.config", 
        "indexer.core.di_container",
        "indexer.database",
        "indexer.database.connection"
    ]
    
    working_imports = []
    broken_imports = []
    
    for import_name in imports_to_test:
        try:
            module = importlib.import_module(import_name)
            print(f"  ✅ {import_name}")
            working_imports.append(import_name)
            
            # If it's a types module, see what's available
            if 'types' in import_name:
                attrs = [attr for attr in dir(module) if not attr.startswith('_')]
                print(f"     Available: {attrs[:5]}{'...' if len(attrs) > 5 else ''}")
                
        except Exception as e:
            print(f"  ❌ {import_name}: {str(e)}")
            broken_imports.append((import_name, str(e)))
    
    return working_imports, broken_imports

def check_types_specifically():
    """Check what's in the types module"""
    print("\n🏷️  Types Module Deep Dive")
    print("=" * 30)
    
    try:
        # Try to import and inspect types
        import indexer.types as types_module
        
        all_attrs = dir(types_module)
        public_attrs = [attr for attr in all_attrs if not attr.startswith('_')]
        
        print(f"Total attributes: {len(all_attrs)}")
        print(f"Public attributes: {len(public_attrs)}")
        print(f"First 10: {public_attrs[:10]}")
        
        # Look for key classes
        key_classes = ['IndexerConfig', 'ContractConfig', 'DatabaseConfig', 'Token', 'Contract']
        print(f"\nLooking for key classes:")
        for cls_name in key_classes:
            if hasattr(types_module, cls_name):
                cls = getattr(types_module, cls_name)
                print(f"  ✅ {cls_name}: {type(cls)}")
                
                # If it's a class, show some info
                if hasattr(cls, '__struct_fields__'):
                    fields = [f.name for f in cls.__struct_fields__]
                    print(f"     Fields: {fields[:5]}{'...' if len(fields) > 5 else ''}")
            else:
                print(f"  ❌ {cls_name}: Not found")
                
    except Exception as e:
        print(f"❌ Could not inspect types module: {e}")
        traceback.print_exc()

def check_config_files():
    """Check what config files exist and their structure"""
    print("\n⚙️  Configuration Files")
    print("=" * 30)
    
    project_root = Path.cwd()
    config_dir = project_root / "config"
    
    if config_dir.exists():
        yaml_files = list(config_dir.rglob("*.yaml"))
        json_files = list(config_dir.rglob("*.json"))
        
        print(f"Config directory: {config_dir}")
        print(f"YAML files: {len(yaml_files)}")
        print(f"JSON files: {len(json_files)}")
        
        # Show structure of a few files
        for file_path in (yaml_files + json_files)[:3]:
            print(f"\n📄 {file_path.relative_to(project_root)}:")
            try:
                with open(file_path) as f:
                    content = f.read()
                
                if file_path.suffix == '.yaml':
                    import yaml
                    data = yaml.safe_load(content)
                else:
                    import json
                    data = json.loads(content)
                
                if isinstance(data, dict):
                    keys = list(data.keys())
                    print(f"  Keys: {keys[:5]}{'...' if len(keys) > 5 else ''}")
                else:
                    print(f"  Type: {type(data)}")
                    
            except Exception as e:
                print(f"  ❌ Could not parse: {e}")
    else:
        print("❌ No config directory found")

def check_database_connection():
    """Try to understand database connectivity"""
    print("\n🗄️  Database Connectivity")
    print("=" * 30)
    
    try:
        # Check environment variables
        import os
        db_vars = [
            'INDEXER_GCP_PROJECT_ID',
            'INDEXER_DB_USER', 
            'INDEXER_DB_PASSWORD',
            'INDEXER_DB_HOST',
            'INDEXER_DB_PORT'
        ]
        
        print("Environment variables:")
        for var in db_vars:
            value = os.getenv(var)
            if value:
                # Mask sensitive info
                if 'PASSWORD' in var:
                    display_value = '*' * len(value)
                else:
                    display_value = value
                print(f"  ✅ {var}: {display_value}")
            else:
                print(f"  ❌ {var}: Not set")
        
        # Try basic database import
        try:
            from indexer.database.connection import DatabaseManager
            print("  ✅ DatabaseManager import works")
        except Exception as e:
            print(f"  ❌ DatabaseManager import failed: {e}")
            
    except Exception as e:
        print(f"❌ Database check failed: {e}")

def main():
    """Run all diagnostics"""
    print("🔍 SIMPLE DIAGNOSTIC - UNDERSTANDING CURRENT STATE")
    print("=" * 60)
    
    # 1. Check file structure
    has_indexer = check_file_structure()
    
    if not has_indexer:
        print("\n❌ No indexer directory found. Are you in the right directory?")
        return
    
    # 2. Check imports
    working_imports, broken_imports = check_imports()
    
    # 3. Deep dive into types
    if 'indexer.types' in working_imports:
        check_types_specifically()
    
    # 4. Check config files
    check_config_files()
    
    # 5. Check database setup
    check_database_connection()
    
    # Summary
    print(f"\n📋 SUMMARY")
    print("=" * 30)
    print(f"Working imports: {len(working_imports)}")
    print(f"Broken imports: {len(broken_imports)}")
    
    if broken_imports:
        print(f"\n🔧 TOP ISSUES TO FIX:")
        for import_name, error in broken_imports[:3]:
            print(f"  • {import_name}: {error}")
    
    print(f"\n💡 NEXT STEPS:")
    if not working_imports:
        print("  1. Fix basic Python imports and paths")
    elif 'indexer.types' not in working_imports:
        print("  1. Fix types module imports")
    elif 'indexer.core' not in working_imports:
        print("  1. Fix core module imports") 
    else:
        print("  1. Basic imports work - can proceed to detailed analysis")

if __name__ == "__main__":
    main()