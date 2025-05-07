#!/usr/bin/env python3
"""
Script to run the blockchain indexer pipeline.

This script demonstrates how to use the Launch indexer pipeline 
for continuous block processing or processing specific blocks.
"""
import argparse
import logging
import sys
import os
from pathlib import Path

# Ensure project root is in path
script_dir = Path(__file__).resolve().parent
backend_dir = script_dir.parent
project_root = backend_dir.parent
sys.path.append(str(project_root))

# Import from launch
from backend.indexer.pipeline import get_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("run_indexer")

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run the blockchain indexer pipeline")
    
    # Config options
    parser.add_argument("--config", type=str, default=None,
                       help="Path to indexer configuration file")
    
    # Command selection
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Continuous processing command
    continuous_parser = subparsers.add_parser("continuous", help="Start continuous processing")
    continuous_parser.add_argument("--start-block", type=int, default=None,
                                 help="Starting block number (default: latest)")
    continuous_parser.add_argument("--end-block", type=int, default=None,
                                 help="Ending block number (default: none, runs indefinitely)")
    
    # Specific blocks command
    blocks_parser = subparsers.add_parser("blocks", help="Process specific blocks")
    blocks_parser.add_argument("block_numbers", type=int, nargs="+",
                              help="Block numbers to process")
    blocks_parser.add_argument("--force", action="store_true",
                              help="Force reprocessing even if already processed")
    
    # Range command
    range_parser = subparsers.add_parser("range", help="Process a range of blocks")
    range_parser.add_argument("start_block", type=int, help="Starting block number")
    range_parser.add_argument("end_block", type=int, help="Ending block number")
    range_parser.add_argument("--force", action="store_true",
                              help="Force reprocessing even if already processed")
    
    # Failed blocks command
    failed_parser = subparsers.add_parser("failed", help="Reprocess failed blocks")
    failed_parser.add_argument("--limit", type=int, default=100,
                              help="Maximum number of blocks to reprocess (default: 100)")
    
    # Missing blocks command
    missing_parser = subparsers.add_parser("missing", help="Find and process missing blocks")
    missing_parser.add_argument("start_block", type=int, help="Starting block number")
    missing_parser.add_argument("end_block", type=int, help="Ending block number")
    
    args = parser.parse_args()
    
    # Get the pipeline
    try:
        pipeline = get_pipeline(config_file=args.config)
    except Exception as e:
        logger.error(f"Error initializing pipeline: {e}")
        return 1
    
    try:
        # Execute command
        if args.command == "continuous":
            logger.info(f"Starting continuous processing from block {args.start_block or 'latest'}")
            pipeline.start_processing(args.start_block, args.end_block)
            
        elif args.command == "blocks":
            logger.info(f"Processing {len(args.block_numbers)} specific blocks")
            results = pipeline.process_specific_blocks(args.block_numbers, args.force)
            _print_results_summary(results)
            
        elif args.command == "range":
            logger.info(f"Processing block range from {args.start_block} to {args.end_block}")
            results = pipeline.process_block_range(args.start_block, args.end_block, args.force)
            _print_results_summary(results)
            
        elif args.command == "failed":
            logger.info(f"Reprocessing failed blocks (limit: {args.limit})")
            results = pipeline.reprocess_failed_blocks(args.limit)
            _print_results_summary(results)
            
        elif args.command == "missing":
            logger.info(f"Finding and processing missing blocks in range {args.start_block} to {args.end_block}")
            results = pipeline.process_missing_blocks(args.start_block, args.end_block)
            _print_results_summary(results)
            
        else:
            logger.error("No command specified")
            parser.print_help()
            return 1
            
        return 0
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
        pipeline.shutdown()
        return 0
        
    except Exception as e:
        logger.error(f"Error running command: {e}")
        return 1
    
    finally:
        # Ensure clean shutdown
        pipeline.shutdown()

def _print_results_summary(results):
    """Print a summary of processing results."""
    if not results:
        logger.info("No results to display")
        return
        
    # Count successes, failures, and total events
    success_count = sum(1 for r in results if r.get("decoding") and r.get("transformation", False))
    failure_count = len(results) - success_count
    event_count = sum(r.get("event_count", 0) for r in results)
    
    logger.info(f"Processing completed:")
    logger.info(f"  Total blocks: {len(results)}")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failure: {failure_count}")
    logger.info(f"  Total events: {event_count}")
    
    # Print failures
    if failure_count > 0:
        logger.info("Failed blocks:")
        for result in results:
            if not (result.get("decoding") and result.get("transformation", False)):
                block_number = result.get("block_number", "unknown")
                errors = result.get("errors", [])
                logger.info(f"  Block {block_number}: {'; '.join(errors)}")

if __name__ == "__main__":
    sys.exit(main())