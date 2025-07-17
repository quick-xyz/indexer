#!/usr/bin/env python3
"""
Schema Alignment Diagnostic Tool

This script will help identify misalignments between:
1. Database schema (actual tables/columns)
2. msgspec type definitions 
3. IndexerConfig expectations
4. Legacy configuration files

Run this first to understand the current state before making fixes.
"""

import sys
from pathlib import Path
from typing import Dict, List, Any, Set
import json
from dataclasses import dataclass

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

@dataclass
class SchemaAlignment:
    database_fields: Set[str]
    msgspec_fields: Set[str] 
    config_fields: Set[str]
    missing_in_db: Set[str]
    missing_in_msgspec: Set[str]
    missing_in_config: Set[str]
    extra_in_db: Set[str]
    extra_in_msgspec: Set[str]
    extra_in_config: Set[str]

class SchemaAlignmentDiagnostic:
    def __init__(self, model_name: str = None):
        self.model_name = model_name or "blub_test_v2"
        self.alignments = {}
        
    def run_diagnostic(self):
        """Run complete diagnostic and generate report"""
        print("🔍 Schema Alignment Diagnostic")
        print("=" * 50)
        
        try:
            # 1. Check database schema
            print("\n1️⃣ Analyzing database schema...")
            db_schema = self._get_database_schema()
            
            # 2. Check msgspec types
            print("2️⃣ Analyzing msgspec type definitions...")
            msgspec_schema = self._get_msgspec_schema()
            
            # 3. Check IndexerConfig expectations  
            print("3️⃣ Analyzing IndexerConfig structure...")
            config_schema = self._get_config_schema()
            
            # 4. Compare and identify misalignments
            print("4️⃣ Identifying misalignments...")
            self._compare_schemas(db_schema, msgspec_schema, config_schema)
            
            # 5. Generate recommendations
            print("5️⃣ Generating recommendations...")
            self._generate_recommendations()
            
        except Exception as e:
            print(f"❌ Diagnostic failed: {e}")
            print("\nThis likely means we need to fix basic connectivity first.")
            self._print_basic_troubleshooting()
    
    def _get_database_schema(self) -> Dict[str, Set[str]]:
        """Get actual database schema"""
        try:
            # Initialize database connection
            from indexer.core.di_container import DIContainer
            container = DIContainer.create_container()
            
            shared_schema = {}
            model_schema = {}
            
            # Get shared database schema
            with container.infrastructure_db_manager().get_session() as session:
                result = session.execute("""
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                """)
                
                for table_name, column_name, data_type in result:
                    if table_name not in shared_schema:
                        shared_schema[table_name] = set()
                    shared_schema[table_name].add(f"{column_name}:{data_type}")
            
            # Get model database schema  
            with container.model_db_manager(self.model_name).get_session() as session:
                result = session.execute("""
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                """)
                
                for table_name, column_name, data_type in result:
                    if table_name not in model_schema:
                        model_schema[table_name] = set()
                    model_schema[table_name].add(f"{column_name}:{data_type}")
            
            print(f"   ✅ Found {len(shared_schema)} shared tables, {len(model_schema)} model tables")
            return {"shared": shared_schema, "model": model_schema}
            
        except Exception as e:
            print(f"   ❌ Failed to get database schema: {e}")
            return {}
    
    def _get_msgspec_schema(self) -> Dict[str, Set[str]]:
        """Extract field definitions from msgspec types"""
        try:
            msgspec_schema = {}
            
            # Import and inspect msgspec types
            from indexer.types import (
                IndexerConfig, ContractConfig, DatabaseConfig, 
                Token, Contract, Address, Source
            )
            
            # Inspect key structures
            for name, cls in [
                ("IndexerConfig", IndexerConfig),
                ("ContractConfig", ContractConfig), 
                ("DatabaseConfig", DatabaseConfig),
            ]:
                fields = set()
                if hasattr(cls, '__struct_fields__'):
                    for field in cls.__struct_fields__:
                        fields.add(f"{field.name}:{field.type}")
                msgspec_schema[name] = fields
            
            print(f"   ✅ Analyzed {len(msgspec_schema)} msgspec structures")
            return msgspec_schema
            
        except Exception as e:
            print(f"   ❌ Failed to analyze msgspec types: {e}")
            return {}
    
    def _get_config_schema(self) -> Dict[str, Set[str]]:
        """Get expected fields from configuration files"""
        try:
            config_schema = {}
            
            # Look for YAML configuration files
            config_dir = Path("config")
            if config_dir.exists():
                for yaml_file in config_dir.rglob("*.yaml"):
                    try:
                        import yaml
                        with open(yaml_file) as f:
                            data = yaml.safe_load(f)
                        
                        config_name = yaml_file.stem
                        fields = set()
                        self._extract_fields_recursive(data, fields, "")
                        config_schema[config_name] = fields
                        
                    except Exception as e:
                        print(f"   ⚠️  Could not parse {yaml_file}: {e}")
            
            print(f"   ✅ Analyzed {len(config_schema)} configuration files")
            return config_schema
            
        except Exception as e:
            print(f"   ❌ Failed to analyze configuration: {e}")
            return {}
    
    def _extract_fields_recursive(self, data: Any, fields: Set[str], prefix: str):
        """Recursively extract field names from configuration data"""
        if isinstance(data, dict):
            for key, value in data.items():
                field_name = f"{prefix}.{key}" if prefix else key
                fields.add(field_name)
                self._extract_fields_recursive(value, fields, field_name)
        elif isinstance(data, list) and data:
            # For lists, analyze first element to understand structure
            self._extract_fields_recursive(data[0], fields, f"{prefix}[0]")
    
    def _compare_schemas(self, db_schema: Dict, msgspec_schema: Dict, config_schema: Dict):
        """Compare schemas and identify misalignments"""
        
        print("\n📊 Schema Comparison Results:")
        print("-" * 30)
        
        # For each major component, check alignment
        for component in ["contracts", "tokens", "addresses", "sources"]:
            print(f"\n🔍 {component.upper()}:")
            
            db_fields = set()
            if "shared" in db_schema and component in db_schema["shared"]:
                db_fields = db_schema["shared"][component]
            
            # Find corresponding msgspec and config fields
            msgspec_fields = set()
            config_fields = set()
            
            # Look for related structures
            for name, fields in msgspec_schema.items():
                if component.lower() in name.lower():
                    msgspec_fields.update(fields)
            
            for name, fields in config_schema.items():
                if component.lower() in name.lower():
                    config_fields.update(fields)
            
            # Calculate alignments
            alignment = SchemaAlignment(
                database_fields=db_fields,
                msgspec_fields=msgspec_fields,
                config_fields=config_fields,
                missing_in_db=msgspec_fields - db_fields,
                missing_in_msgspec=db_fields - msgspec_fields,
                missing_in_config=db_fields - config_fields,
                extra_in_db=db_fields - msgspec_fields,
                extra_in_msgspec=msgspec_fields - db_fields,
                extra_in_config=config_fields - db_fields
            )
            
            self.alignments[component] = alignment
            self._print_alignment_summary(component, alignment)
    
    def _print_alignment_summary(self, component: str, alignment: SchemaAlignment):
        """Print summary for one component alignment"""
        total_db = len(alignment.database_fields)
        total_msgspec = len(alignment.msgspec_fields)
        total_config = len(alignment.config_fields)
        
        print(f"   Database: {total_db} fields")
        print(f"   Msgspec:  {total_msgspec} fields")
        print(f"   Config:   {total_config} fields")
        
        if alignment.missing_in_db:
            print(f"   ❌ Missing in DB: {list(alignment.missing_in_db)[:3]}...")
        
        if alignment.missing_in_msgspec:
            print(f"   ❌ Missing in msgspec: {list(alignment.missing_in_msgspec)[:3]}...")
        
        if alignment.extra_in_db:
            print(f"   ⚠️  Extra in DB: {list(alignment.extra_in_db)[:3]}...")
    
    def _generate_recommendations(self):
        """Generate specific recommendations for fixing alignments"""
        print("\n🎯 RECOMMENDATIONS")
        print("=" * 50)
        
        print("\n1️⃣ **Database Schema Issues:**")
        has_db_issues = False
        for component, alignment in self.alignments.items():
            if alignment.missing_in_db or alignment.extra_in_db:
                has_db_issues = True
                print(f"   • {component}: Run migration to add missing fields")
        
        if not has_db_issues:
            print("   ✅ Database schema looks good")
        
        print("\n2️⃣ **Msgspec Type Issues:**")
        has_msgspec_issues = False
        for component, alignment in self.alignments.items():
            if alignment.missing_in_msgspec:
                has_msgspec_issues = True
                print(f"   • {component}: Add missing fields to msgspec definitions")
        
        if not has_msgspec_issues:
            print("   ✅ Msgspec types look aligned")
        
        print("\n3️⃣ **Configuration Issues:**")
        has_config_issues = False
        for component, alignment in self.alignments.items():
            if alignment.missing_in_config or alignment.extra_in_config:
                has_config_issues = True
                print(f"   • {component}: Update configuration files")
        
        if not has_config_issues:
            print("   ✅ Configuration looks aligned")
        
        print("\n4️⃣ **Immediate Actions:**")
        print("   1. Fix database schema (migrations)")
        print("   2. Update msgspec type definitions")
        print("   3. Add validation in IndexerConfig.from_model()")
        print("   4. Test configuration loading")
        print("   5. Update downstream services")
    
    def _print_basic_troubleshooting(self):
        """Print basic troubleshooting steps"""
        print("\n🔧 BASIC TROUBLESHOOTING:")
        print("1. Check database connectivity:")
        print("   python -c \"from indexer.core.di_container import DIContainer; DIContainer.create_container()\"")
        print("\n2. Check environment variables:")
        print("   echo $INDEXER_GCP_PROJECT_ID")
        print("   echo $INDEXER_DB_USER")
        print("\n3. Check if databases exist:")
        print("   python -m indexer.cli migrate status")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Diagnose schema alignments")
    parser.add_argument("--model", default="blub_test_v2", help="Model database name")
    args = parser.parse_args()
    
    diagnostic = SchemaAlignmentDiagnostic(args.model)
    diagnostic.run_diagnostic()