#!/usr/bin/env python3
"""
Quick script to check liquidityaction enum values in your database
"""

import sys
import os
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import create_engine, text
import logging

def get_database_url():
    """Get database URL for your model database using same logic as your main system"""
    # Try to get project ID for GCP secrets
    project_id = os.getenv("INDEXER_GCP_PROJECT_ID")
    
    if project_id:
        try:
            # Try to use GCP Secret Manager (like your main system)
            sys.path.insert(0, str(PROJECT_ROOT))
            from indexer.core.secrets_service import SecretsService
            
            secrets_service = SecretsService(project_id)
            db_credentials = secrets_service.get_database_credentials()
            
            db_user = db_credentials.get('user') or os.getenv("INDEXER_DB_USER")
            db_password = db_credentials.get('password') or os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST") or db_credentials.get('host', "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT") or db_credentials.get('port', "5432")
            
            print(f"✅ Using GCP Secret Manager for credentials (project: {project_id})")
            
        except Exception as e:
            print(f"⚠️  GCP Secret Manager failed, falling back to env vars: {e}")
            # Fallback to environment variables
            db_user = os.getenv("INDEXER_DB_USER")
            db_password = os.getenv("INDEXER_DB_PASSWORD")
            db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
            db_port = os.getenv("INDEXER_DB_PORT", "5432")
    else:
        print("ℹ️  No INDEXER_GCP_PROJECT_ID found, using environment variables")
        # Use environment variables only
        db_user = os.getenv("INDEXER_DB_USER")
        db_password = os.getenv("INDEXER_DB_PASSWORD")
        db_host = os.getenv("INDEXER_DB_HOST", "127.0.0.1")
        db_port = os.getenv("INDEXER_DB_PORT", "5432")
    
    if not db_user or not db_password:
        print("❌ Database credentials not found")
        print("Either set INDEXER_GCP_PROJECT_ID for GCP secrets, or set:")
        print("  INDEXER_DB_USER and INDEXER_DB_PASSWORD environment variables")
        sys.exit(1)
    
    # Connect to your model database (blub_test)
    db_name = "blub_test"
    print(f"🔗 Connecting to: {db_host}:{db_port}/{db_name}")
    return f"postgresql+psycopg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def check_enum_values():
    """Check what enum values exist in the database"""
    print("🔍 Checking liquidityaction enum values in database...")
    
    try:
        engine = create_engine(get_database_url())
        
        with engine.connect() as conn:
            # Check if the enum type exists
            result = conn.execute(text("""
                SELECT typname 
                FROM pg_type 
                WHERE typname = 'liquidityaction'
            """))
            
            enum_exists = result.fetchone()
            if not enum_exists:
                print("❌ liquidityaction enum type does not exist in database")
                return False
            
            print("✅ liquidityaction enum type exists")
            
            # Get the enum values
            result = conn.execute(text("""
                SELECT enumlabel 
                FROM pg_enum 
                WHERE enumtypid = (
                    SELECT oid FROM pg_type WHERE typname = 'liquidityaction'
                )
                ORDER BY enumsortorder
            """))
            
            enum_values = [row[0] for row in result.fetchall()]
            
            print(f"\n📋 Current enum values in database:")
            for i, value in enumerate(enum_values, 1):
                print(f"  {i}. '{value}'")
            
            # Check if 'remove' is present
            if 'remove' in enum_values:
                print("\n✅ 'remove' value is present in database enum")
                return True
            else:
                print("\n❌ 'remove' value is MISSING from database enum")
                print("Expected values: ['add', 'remove', 'update']")
                print(f"Actual values: {enum_values}")
                return False
                
    except Exception as e:
        print(f"❌ Error checking database: {e}")
        return False

def check_table_structure():
    """Check the liquidity table structure"""
    print("\n🏗️ Checking liquidity table structure...")
    
    try:
        engine = create_engine(get_database_url())
        
        with engine.connect() as conn:
            # Check if table exists
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'liquidity'
            """))
            
            if not result.fetchone():
                print("❌ liquidity table does not exist")
                return False
            
            print("✅ liquidity table exists")
            
            # Get column info
            result = conn.execute(text("""
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_name = 'liquidity' 
                AND column_name = 'action'
            """))
            
            column_info = result.fetchone()
            if column_info:
                print(f"✅ action column: {column_info[1]} (type: {column_info[2]})")
                return True
            else:
                print("❌ action column not found in liquidity table")
                return False
                
    except Exception as e:
        print(f"❌ Error checking table: {e}")
        return False

def main():
    """Main function"""
    print("🗄️ Database Enum Checker")
    print("=" * 50)
    
    # Check database connection
    try:
        engine = create_engine(get_database_url())
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)
    
    # Check table structure
    table_ok = check_table_structure()
    
    # Check enum values
    enum_ok = check_enum_values()
    
    print("\n" + "=" * 50)
    if table_ok and enum_ok:
        print("✅ Everything looks good! The 'remove' value should work.")
        print("The error might be coming from somewhere else.")
    else:
        print("❌ Found the issue! The database enum is missing 'remove' value.")
        print("\n💡 To fix:")
        print("1. Recreate the model database:")
        print("   python -m indexer.cli migrate model recreate blub_test")
        print("2. Or manually add the enum value:")
        print("   ALTER TYPE liquidityaction ADD VALUE 'remove';")

if __name__ == "__main__":
    main()