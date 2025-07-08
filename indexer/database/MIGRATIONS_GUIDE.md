# Example usage of the migration system via Python API

from indexer.database.migration_manager import MigrationManager

def example_development_workflow():
    """Example of typical development workflow"""
    
    # Initialize migration manager
    manager = MigrationManager()
    
    # === DEVELOPMENT SETUP ===
    print("🚀 Setting up development environment...")
    
    # Option 1: Clean slate for testing
    manager.reset_everything()
    
    # Option 2: Or just create what you need
    # manager.upgrade_shared()
    # manager.create_model_database("blub_test", drop_if_exists=True)
    
    # === SHARED DATABASE CHANGES ===
    print("\n📊 Managing shared database...")
    
    # Create and apply shared database migrations
    revision = manager.create_shared_migration("Add new pool configuration tables")
    print(f"Created migration: {revision}")
    
    manager.upgrade_shared()
    print("Shared database updated")
    
    # === MODEL DATABASE MANAGEMENT ===
    print("\n🔧 Managing model databases...")
    
    # Create new model database from current template
    manager.create_model_database("blub_test_v2")
    print("New model database created")
    
    # Recreate existing database (your preferred update pattern)
    manager.recreate_model_database("blub_test")
    print("Existing database recreated with latest schema")
    
    # === STATUS CHECKING ===
    print("\n📋 Checking status...")
    
    status = manager.current_status()
    print(f"Shared DB revision: {status['shared']['current_revision']}")
    print(f"Model databases: {list(status['models'].keys())}")
    
    # === VIEW CURRENT SCHEMA ===
    print("\n📄 Current model schema:")
    schema_sql = manager.get_model_schema_sql()
    print(f"Schema has {len(schema_sql.split(';'))} statements")


def example_production_workflow():
    """Example of production deployment workflow"""
    
    manager = MigrationManager()
    
    # === SHARED DATABASE UPDATE ===
    # Apply any pending shared database migrations
    current_rev = manager.get_shared_current_revision()
    print(f"Current shared revision: {current_rev}")
    
    manager.upgrade_shared()
    print("Shared database updated")
    
    # === NEW MODEL DEPLOYMENT ===
    # Create new model database with latest schema
    model_name = "production_model_v3"
    
    success = manager.create_model_database(model_name)
    if success:
        print(f"Production model database '{model_name}' created")
        
        # Your application would then:
        # 1. Run indexing to catch up to current block
        # 2. Test the new database
        # 3. Update frontend configuration to point to new database
        # 4. Retire old database when ready
    
    print("Ready for production deployment")


def example_schema_updates():
    """Example of handling schema updates during development"""
    
    manager = MigrationManager()
    
    # When you add new tables/columns to your indexer schema:
    
    # 1. Update your SQLAlchemy table definitions
    # 2. View what the new schema looks like
    current_schema = manager.get_model_schema_sql()
    print("Updated schema ready")
    
    # 3. For existing model databases, create new ones instead of migrating
    manager.recreate_model_database("test_model")
    print("Test database updated with new schema")
    
    # 4. New model databases automatically get the latest schema
    manager.create_model_database("new_model")
    print("New model gets latest schema automatically")


if __name__ == "__main__":
    # Run example workflows
    print("=== DEVELOPMENT WORKFLOW ===")
    example_development_workflow()
    
    print("\n=== PRODUCTION WORKFLOW ===")
    example_production_workflow()
    
    print("\n=== SCHEMA UPDATE WORKFLOW ===")
    example_schema_updates()