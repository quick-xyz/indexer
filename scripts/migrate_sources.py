#!/usr/bin/env python3
"""
Migration Script: Convert source_paths to Sources table

This script migrates existing models from using source_paths JSONB field
to using the new Sources table with proper foreign key relationships.

Usage:
    python scripts/migrate_sources.py --model <model_name>
    python scripts/migrate_sources.py --all
"""

import sys
import argparse
import logging
from pathlib import Path

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer.admin.admin_context import AdminContext
from indexer.core.config_service import ConfigService
from indexer.database.models.config import Model, Source, ModelSource


def migrate_single_model(admin_context: AdminContext, model_name: str) -> bool:
    """Migrate a single model's sources"""
    logger = logging.getLogger('migrate_sources')
    
    try:
        # Create ConfigService using the infrastructure database manager
        config_service = ConfigService(admin_context.infrastructure_db_manager)
        
        # Check if model exists
        model = config_service.get_model_by_name(model_name)
        if not model:
            logger.error(f"Model '{model_name}' not found")
            return False
        
        logger.info(f"Migrating sources for model: {model_name}")
        
        # Check if already migrated (has sources in new table)
        existing_sources = get_sources_for_model(config_service, model_name)
        if existing_sources:
            logger.warning(f"Model '{model_name}' already has {len(existing_sources)} sources in database")
            logger.info("Existing sources:")
            for source in existing_sources:
                logger.info(f"  - {source.name}: {source.path} | {source.format}")
            return True
        
        # Migrate from source_paths
        if not model.source_paths:
            logger.warning(f"No source_paths to migrate for model '{model_name}'")
            return True
        
        logger.info(f"Found {len(model.source_paths)} source_paths to migrate")
        
        success = migrate_model_sources(config_service, model_name)
        if success:
            logger.info(f"✅ Successfully migrated sources for model '{model_name}'")
            
            # Verify migration
            new_sources = get_sources_for_model(config_service, model_name)
            logger.info(f"Created {len(new_sources)} sources:")
            for source in new_sources:
                logger.info(f"  - {source.name}: {source.path} | {source.format}")
            
            return True
        else:
            logger.error(f"❌ Failed to migrate sources for model '{model_name}'")
            return False
            
    except Exception as e:
        logger.error(f"Error migrating model '{model_name}': {e}")
        return False


def get_sources_for_model(config_service: ConfigService, model_name: str):
    """Get all sources for a model"""
    with config_service.db_manager.get_session() as session:
        model = config_service.get_model_by_name(model_name)
        if not model:
            return []
        
        sources = session.query(Source)\
            .join(ModelSource, Source.id == ModelSource.source_id)\
            .filter(ModelSource.model_id == model.id)\
            .filter(Source.status == 'active')\
            .all()
        
        return sources


def create_source(config_service: ConfigService, name: str, path: str, format_string: str):
    """Create a new source"""
    try:
        with config_service.db_manager.get_session() as session:
            # Check if source already exists
            existing = session.query(Source).filter(Source.name == name).first()
            if existing:
                return existing
            
            source = Source(
                name=name,
                path=path,
                format=format_string,
                status='active'
            )
            
            session.add(source)
            session.commit()
            session.refresh(source)
            
            return source
    except Exception as e:
        config_service.logger.error(f"Failed to create source: {e}")
        return None


def link_model_to_source(config_service: ConfigService, model_name: str, source_id: int) -> bool:
    """Link a model to a source"""
    try:
        with config_service.db_manager.get_session() as session:
            model = config_service.get_model_by_name(model_name)
            if not model:
                return False
            
            # Check if link already exists
            existing = session.query(ModelSource)\
                .filter(ModelSource.model_id == model.id)\
                .filter(ModelSource.source_id == source_id)\
                .first()
            
            if existing:
                return True
            
            model_source = ModelSource(
                model_id=model.id,
                source_id=source_id
            )
            
            session.add(model_source)
            session.commit()
            
            return True
    except Exception as e:
        config_service.logger.error(f"Failed to link model to source: {e}")
        return False


def migrate_model_sources(config_service: ConfigService, model_name: str) -> bool:
    """Migrate a model's source_paths JSONB to Sources table"""
    try:
        model = config_service.get_model_by_name(model_name)
        if not model:
            config_service.logger.error(f"Model {model_name} not found")
            return False
        
        if not model.source_paths:
            config_service.logger.warning(f"No source_paths to migrate for model {model_name}")
            return True
        
        for i, source_data in enumerate(model.source_paths):
            if isinstance(source_data, str):
                # Old format: just a path string
                path = source_data
                format_string = "block_{:012d}.json"  # Default format
            elif isinstance(source_data, dict):
                # New format: {"path": "...", "format": "..."}
                path = source_data.get('path', '')
                format_string = source_data.get('format', 'block_{:012d}.json')
            else:
                config_service.logger.warning(f"Unknown source format: {source_data}")
                continue
            
            # Create source name from model and index
            source_name = f"{model_name}-source-{i}"
            
            # Create or get source
            source = create_source(config_service, source_name, path, format_string)
            if source:
                # Link model to source
                link_model_to_source(config_service, model_name, source.id)
                config_service.logger.info(f"Migrated source {source_name} for model {model_name}")
        
        return True
        
    except Exception as e:
        config_service.logger.error(f"Failed to migrate sources for model {model_name}: {e}")
        return False


def migrate_all_models(admin_context: AdminContext) -> bool:
    """Migrate all models' sources"""
    logger = logging.getLogger('migrate_sources')
    
    try:
        config_service = ConfigService(admin_context.infrastructure_db_manager)
        
        # Get all models
        with config_service.db_manager.get_session() as session:
            models = session.query(Model).filter(Model.status == 'active').all()
        
        if not models:
            logger.warning("No active models found")
            return True
        
        logger.info(f"Found {len(models)} active models to migrate")
        
        success_count = 0
        for model in models:
            logger.info(f"\n--- Migrating model: {model.name} ---")
            if migrate_single_model(admin_context, model.name):
                success_count += 1
        
        logger.info(f"\n✅ Migration complete: {success_count}/{len(models)} models migrated successfully")
        return success_count == len(models)
        
    except Exception as e:
        logger.error(f"Error during bulk migration: {e}")
        return False


def verify_migration(admin_context: AdminContext, model_name: str = None) -> bool:
    """Verify migration results"""
    logger = logging.getLogger('migrate_sources')
    
    try:
        config_service = ConfigService(admin_context.infrastructure_db_manager)
        
        if model_name:
            models = [config_service.get_model_by_name(model_name)]
            if not models[0]:
                logger.error(f"Model '{model_name}' not found")
                return False
        else:
            with config_service.db_manager.get_session() as session:
                models = session.query(Model).filter(Model.status == 'active').all()
        
        logger.info("Migration Verification Report:")
        logger.info("=" * 50)
        
        total_models = len(models)
        migrated_models = 0
        
        for model in models:
            sources = get_sources_for_model(config_service, model.name)
            old_sources = len(model.source_paths) if model.source_paths else 0
            new_sources = len(sources)
            
            status = "✅ MIGRATED" if new_sources > 0 else "❌ NOT MIGRATED"
            logger.info(f"{model.name}: {status} (old: {old_sources}, new: {new_sources})")
            
            if new_sources > 0:
                migrated_models += 1
                for source in sources:
                    logger.info(f"  └─ {source.name}: {source.path}")
        
        logger.info("=" * 50)
        logger.info(f"Summary: {migrated_models}/{total_models} models migrated")
        
        return migrated_models == total_models
        
    except Exception as e:
        logger.error(f"Error during verification: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Migrate model sources to new database structure')
    parser.add_argument('--model', type=str, help='Migrate specific model by name')
    parser.add_argument('--all', action='store_true', help='Migrate all models')
    parser.add_argument('--verify', action='store_true', help='Verify migration status')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger('migrate_sources')
    
    if not (args.model or args.all or args.verify):
        parser.print_help()
        sys.exit(1)
    
    try:
        # Initialize admin context
        logger.info("Initializing admin context...")
        admin_context = AdminContext()
        
        if args.verify:
            logger.info("Running migration verification...")
            success = verify_migration(admin_context, args.model)
        elif args.model:
            logger.info(f"Migrating single model: {args.model}")
            success = migrate_single_model(admin_context, args.model)
        elif args.all:
            logger.info("Migrating all models...")
            success = migrate_all_models(admin_context)
        
        if success:
            logger.info("✅ Operation completed successfully")
            sys.exit(0)
        else:
            logger.error("❌ Operation failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()