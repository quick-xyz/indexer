#!/usr/bin/env python3
"""
Enum Diagnostic Script

This script diagnoses enum issues in the database by:
1. Checking what enum types exist in PostgreSQL
2. Testing enum value insertion/retrieval 
3. Identifying where uppercase values come from

Usage:
    python scripts/diagnose_enums.py blub_test
"""

import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple
import json

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer.cli.context import CLIContext
from indexer.core.logging_config import IndexerLogger, log_with_context
from indexer.database.indexer.tables.events.trade import TradeDirection, TradeType
from indexer.database.indexer.tables.events.liquidity import LiquidityAction
from indexer.database.indexer.tables.events.reward import RewardType
from indexer.database.indexer.tables.events.staking import StakingAction
from sqlalchemy import text
import logging


class EnumDiagnostic:
    """Diagnose enum issues using proper database connection patterns"""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.logger = IndexerLogger.get_logger('tools.enum_diagnostic')
        self.context = CLIContext()
        
        # Get database managers using CLI context patterns
        self.infrastructure_db = self.context.infrastructure_db_manager
        self.model_db = self.context.get_model_db_manager(model_name)
        
        log_with_context(self.logger, logging.INFO, "EnumDiagnostic initialized",
                        model_name=model_name)
    
    def run_full_diagnostic(self) -> Dict:
        """Run complete enum diagnostic and return results"""
        results = {
            "model_name": self.model_name,
            "postgresql_enums": {},
            "code_enums": {},
            "enum_test_results": {},
            "issues_found": []
        }
        
        print(f"\n🔍 ENUM DIAGNOSTIC FOR MODEL: {self.model_name}")
        print("=" * 60)
        
        # Step 1: Check PostgreSQL enum types
        print("\n1️⃣ Checking PostgreSQL enum types...")
        results["postgresql_enums"] = self._check_postgresql_enums()
        
        # Step 2: Check code-defined enum values  
        print("\n2️⃣ Checking code-defined enum values...")
        results["code_enums"] = self._check_code_enums()
        
        # Step 3: Compare and identify mismatches
        print("\n3️⃣ Comparing PostgreSQL vs Code enums...")
        mismatches = self._compare_enums(results["postgresql_enums"], results["code_enums"])
        if mismatches:
            results["issues_found"].extend(mismatches)
            for issue in mismatches:
                print(f"   ❌ {issue}")
        else:
            print("   ✅ All enums match between PostgreSQL and code")
        
        # Step 4: Test enum insertion/retrieval
        print("\n4️⃣ Testing enum value insertion/retrieval...")
        results["enum_test_results"] = self._test_enum_operations()
        
        # Step 5: Summary
        print("\n📋 DIAGNOSTIC SUMMARY")
        print("-" * 30)
        if results["issues_found"]:
            print(f"❌ Found {len(results['issues_found'])} issues:")
            for issue in results["issues_found"]:
                print(f"   • {issue}")
        else:
            print("✅ No enum issues detected")
        
        return results
    
    def _check_postgresql_enums(self) -> Dict[str, List[str]]:
        """Check what enum types exist in PostgreSQL and their values"""
        postgresql_enums = {}
        
        try:
            with self.model_db.get_session() as session:
                # Query PostgreSQL system catalog for enum types and values
                result = session.execute(text("""
                    SELECT t.typname as enum_name, e.enumlabel as enum_value
                    FROM pg_type t 
                    JOIN pg_enum e ON t.oid = e.enumtypid 
                    ORDER BY t.typname, e.enumsortorder
                """))
                
                for row in result:
                    enum_name = row.enum_name
                    enum_value = row.enum_value
                    
                    if enum_name not in postgresql_enums:
                        postgresql_enums[enum_name] = []
                    postgresql_enums[enum_name].append(enum_value)
        
        except Exception as e:
            log_with_context(self.logger, logging.ERROR, "Failed to check PostgreSQL enums",
                           error=str(e))
            print(f"   ❌ Error checking PostgreSQL enums: {e}")
            return {}
        
        # Display results
        for enum_name, values in postgresql_enums.items():
            print(f"   📊 {enum_name}: {values}")
        
        return postgresql_enums
    
    def _check_code_enums(self) -> Dict[str, List[str]]:
        """Check enum values defined in code"""
        code_enums = {}
        
        # Map of enum classes to check
        enum_classes = {
            "tradedirection": TradeDirection,
            "tradetype": TradeType,
            "liquidityaction": LiquidityAction,
            "rewardtype": RewardType,
            "stakingaction": StakingAction,
        }
        
        for enum_name, enum_class in enum_classes.items():
            values = [member.value for member in enum_class]
            code_enums[enum_name] = values
            print(f"   📝 {enum_name}: {values}")
        
        return code_enums
    
    def _compare_enums(self, postgresql_enums: Dict[str, List[str]], 
                       code_enums: Dict[str, List[str]]) -> List[str]:
        """Compare PostgreSQL and code enum values, return list of issues"""
        issues = []
        
        for enum_name, code_values in code_enums.items():
            if enum_name not in postgresql_enums:
                issues.append(f"Enum '{enum_name}' exists in code but not in PostgreSQL")
                continue
            
            pg_values = postgresql_enums[enum_name]
            
            # Check if values match exactly
            if set(code_values) != set(pg_values):
                issues.append(
                    f"Enum '{enum_name}' values differ: "
                    f"Code={code_values}, PostgreSQL={pg_values}"
                )
        
        # Check for PostgreSQL enums not in code
        for enum_name in postgresql_enums:
            if enum_name not in code_enums:
                issues.append(f"Enum '{enum_name}' exists in PostgreSQL but not tracked in code")
        
        return issues
    
    def _test_enum_operations(self) -> Dict[str, Dict]:
        """Test actual enum insertion and retrieval"""
        test_results = {}
        
        # Test each enum with a simple operation
        enum_tests = [
            ("tradedirection", TradeDirection, TradeDirection.BUY),
            ("liquidityaction", LiquidityAction, LiquidityAction.ADD),
            ("rewardtype", RewardType, RewardType.FEES),
        ]
        
        for enum_name, enum_class, test_value in enum_tests:
            test_results[enum_name] = self._test_single_enum(enum_name, enum_class, test_value)
        
        return test_results
    
    def _test_single_enum(self, enum_name: str, enum_class, test_value) -> Dict:
        """Test a single enum type with insertion/retrieval"""
        result = {
            "enum_name": enum_name,
            "test_value": test_value.value,
            "test_passed": False,
            "error": None
        }
        
        try:
            with self.model_db.get_session() as session:
                # Test direct enum value query
                query = text(f"SELECT '{test_value.value}'::{enum_name}")
                db_result = session.execute(query).scalar()
                
                result["db_returned"] = db_result
                result["test_passed"] = db_result == test_value.value
                
                if result["test_passed"]:
                    print(f"   ✅ {enum_name}: '{test_value.value}' → '{db_result}'")
                else:
                    print(f"   ❌ {enum_name}: '{test_value.value}' → '{db_result}' (mismatch)")
                    
        except Exception as e:
            result["error"] = str(e)
            result["test_passed"] = False
            print(f"   ❌ {enum_name}: Error testing '{test_value.value}' - {e}")
        
        return result
    
    def save_results(self, results: Dict, output_file: str = None):
        """Save diagnostic results to file"""
        if not output_file:
            output_file = f"enum_diagnostic_{self.model_name}.json"
        
        try:
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            print(f"\n💾 Results saved to: {output_file}")
            
        except Exception as e:
            print(f"❌ Failed to save results: {e}")


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/diagnose_enums.py <model_name>")
        print("Example: python scripts/diagnose_enums.py blub_test")
        sys.exit(1)
    
    model_name = sys.argv[1]
    
    try:
        diagnostic = EnumDiagnostic(model_name)
        results = diagnostic.run_full_diagnostic()
        diagnostic.save_results(results)
        
        # Exit with error code if issues found
        if results["issues_found"]:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()