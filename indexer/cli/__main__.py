# indexer/cli/__main__.py

"""
Unified Indexer CLI Tool

Usage: python -m indexer.cli [command] [options]

This replaces the old admin CLI and provides a single entry point for all
indexer configuration and management operations.
"""

import click
import os
import sys
import atexit
from pathlib import Path
from typing import Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from indexer.cli.context import CLIContext
from indexer.core.logging import IndexerLogger

# Create global CLI context
cli_context = CLIContext()

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--model', help='Model name (for model-specific operations)')
@click.pass_context
def cli(ctx, verbose, model):
    """Indexer CLI - Configuration and Management Tool
    
    Unified command-line interface for all indexer operations including:
    - Configuration management (models, contracts, tokens)
    - Pool pricing configuration
    - Service operations (pricing and calculation)
    - Database administration
    - Batch processing
    """
    # Ensure context object exists
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['model'] = model
    ctx.obj['cli_context'] = cli_context
    
    # Configure logging
    log_level = "DEBUG" if verbose else "INFO"
    IndexerLogger.configure(
        log_dir=Path.cwd() / "logs",
        log_level=log_level,
        console_enabled=True,
        file_enabled=False,
        structured_format=False
    )


# Import command groups
from indexer.cli.commands.config import config
from indexer.cli.commands.model import model
from indexer.cli.commands.contract import contract
from indexer.cli.commands.token import token
from indexer.cli.commands.address import address
from indexer.cli.commands.pool_pricing import pool_pricing
from indexer.cli.commands.pricing import pricing
from indexer.cli.commands.service import service  # NEW: Service commands
from indexer.cli.commands.migrate import migrate
from indexer.cli.commands.batch import batch

# Register command groups
cli.add_command(config)
cli.add_command(model) 
cli.add_command(contract)
cli.add_command(token)
cli.add_command(address)
cli.add_command(pool_pricing)
cli.add_command(pricing)
cli.add_command(service)  # NEW: Service command group
cli.add_command(migrate)
cli.add_command(batch)

def cleanup():
    """Cleanup function to properly shutdown database connections"""
    cli_context.shutdown()


# Register cleanup handler
atexit.register(cleanup)


if __name__ == '__main__':
    cli()