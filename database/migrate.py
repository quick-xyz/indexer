# indexer/database/migrate.py

import os
import sys
from pathlib import Path
import argparse
import enum
from sqlalchemy.dialects import postgresql
from alembic.config import Config
from alembic import command
from alembic.script import ScriptDirectory

sys.path.insert(0, str(Path(__file__).parent.parent))


def get_alembic_config():
    alembic_cfg_path = Path(__file__).parent / "migrations" / "alembic.ini"
    
    if not alembic_cfg_path.exists():
        raise FileNotFoundError(f"Alembic config not found at {alembic_cfg_path}")
    
    alembic_cfg = Config(str(alembic_cfg_path))
    
    migrations_dir = Path(__file__).parent / "migrations"
    alembic_cfg.set_main_option("script_location", str(migrations_dir))
    
    return alembic_cfg


def init_migrations():
    migrations_dir = Path(__file__).parent / "migrations"
    
    if migrations_dir.exists() and (migrations_dir / "versions").exists():
        print("Migrations already initialized!")
        return
    
    print("Initializing Alembic migrations...")
    alembic_cfg = get_alembic_config()
    command.init(alembic_cfg, str(migrations_dir))
    print(f"Migrations initialized in {migrations_dir}")


def create_migration(message: str, autogenerate: bool = True):
    print(f"Creating migration: {message}")
    alembic_cfg = get_alembic_config()
    
    command.revision(
        alembic_cfg,
        message=message,
        autogenerate=autogenerate
    )
    print("Migration created successfully!")


def upgrade_database(revision: str = "head"):
    print(f"Upgrading database to {revision}...")
    alembic_cfg = get_alembic_config()
    
    command.upgrade(alembic_cfg, revision)
    print("Database upgrade completed!")


def downgrade_database(revision: str):
    print(f"Downgrading database to {revision}...")
    alembic_cfg = get_alembic_config()
    
    command.downgrade(alembic_cfg, revision)
    print("Database downgrade completed!")


def show_current_revision():
    alembic_cfg = get_alembic_config()
    
    command.current(alembic_cfg)


def show_migration_history():
    alembic_cfg = get_alembic_config()
    
    command.history(alembic_cfg)


def main():
    parser = argparse.ArgumentParser(description="Database migration management")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    subparsers.add_parser("init", help="Initialize migrations")
    
    create_parser = subparsers.add_parser("create", help="Create new migration")
    create_parser.add_argument("message", help="Migration message")
    create_parser.add_argument("--manual", action="store_true", 
                              help="Create empty migration (no autogenerate)")
    
    upgrade_parser = subparsers.add_parser("upgrade", help="Upgrade database")
    upgrade_parser.add_argument("--revision", default="head", 
                               help="Target revision (default: head)")
    
    downgrade_parser = subparsers.add_parser("downgrade", help="Downgrade database")
    downgrade_parser.add_argument("revision", help="Target revision")
    
    subparsers.add_parser("current", help="Show current revision")
    
    subparsers.add_parser("history", help="Show migration history")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == "init":
            init_migrations()
        elif args.command == "create":
            create_migration(args.message, autogenerate=not args.manual)
        elif args.command == "upgrade":
            upgrade_database(args.revision)
        elif args.command == "downgrade":
            downgrade_database(args.revision)
        elif args.command == "current":
            show_current_revision()
        elif args.command == "history":
            show_migration_history()
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()