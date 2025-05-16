"""
Integrated pipeline for blockchain indexing.

This module provides a complete pipeline that connects the streamer,
decoder, and transformer components into a unified workflow.
"""
import logging
import time
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime, timedelta
import json

from indexer.stream.interfaces import BlockStreamerInterface
from indexer.decode.interfaces import BlockProcessorInterface
from indexer.database.registry.block_registry import BlockRegistry
from indexer.transform.framework.manager import TransformationManager
from indexer.transform.interfaces import EventListener


class DecoderListener:
    """
    Listener that connects streamer to decoder.
    
    Implements the BlockListener interface from the streamer component
    to automatically process new blocks as they arrive.
    """
    
    def __init__(self, block_processor: BlockProcessorInterface, block_registry: BlockRegistry):
        """
        Initialize decoder listener.
        
        Args:
            block_processor: Block processor for decoding blocks
            block_registry: Block registry for tracking processing status
        """
        self.block_processor = block_processor
        self.block_registry = block_registry
        self.logger = logging.getLogger(__name__)



    def on_new_block(self, block_number: int, block_data: Optional[bytes] = None, 
                    block_path: Optional[str] = None) -> None:
        """
        Called when a new block is available.
        
        Args:
            block_number: Block number
            block_data: Block data (optional)
            block_path: Path to block file (optional)
        """
        self.logger.info(f"Received new block: {block_number}")
        
        # Extract block metadata for registry
        try:
            if block_data:
                block_json = json.loads(block_data)
                block_hash = block_json.get('hash', '')
                parent_hash = block_json.get('parentHash', '')
                timestamp = block_json.get('timestamp', 0)
                if isinstance(timestamp, str) and timestamp.startswith('0x'):
                    timestamp = int(timestamp, 16)
                    timestamp = datetime.fromtimestamp(timestamp)
                
                # Register block in registry
                self.block_registry.register_block(
                    block_number=block_number,
                    block_hash=block_hash,
                    parent_hash=parent_hash,
                    timestamp=timestamp
                )
                
                # Update block path in registry if provided
                if block_path:
                    self.block_registry.update_block_status(
                        block_number=block_number,
                        storage_type="raw",
                        status="AVAILABLE",
                        path=block_path
                    )
        except Exception as e:
            self.logger.error(f"Error extracting block metadata: {str(e)}")
        
        # Process block
        if block_path:
            try:
                success, result = self.block_processor.process_block(block_path)
                if success:
                    self.logger.info(f"Successfully processed block {block_number}")
                else:
                    self.logger.error(f"Failed to process block {block_number}: {result.get('errors', [])}")
            except Exception as e:
                self.logger.error(f"Error processing block {block_number}: {str(e)}")


class IntegratedPipeline:
    """
    Integrated pipeline that connects all components.
    
    This class provides a unified interface for the complete indexing pipeline,
    coordinating the flow of data between the streamer, decoder, and transformer.
    """
    
    def __init__(self, streamer: BlockStreamerInterface,
                 block_processor: BlockProcessorInterface,
                 block_registry: BlockRegistry,
                 transformation_manager: TransformationManager,
                 event_listeners: Optional[List[EventListener]] = None):
        """
        Initialize integrated pipeline.
        
        Args:
            streamer: Block streamer
            block_processor: Block processor
            block_registry: Block registry
            transformation_manager: Transformation manager
            event_listeners: Event listeners (optional)
        """
        self.streamer = streamer
        self.block_processor = block_processor
        self.block_registry = block_registry
        self.transformation_manager = transformation_manager
        self.event_listeners = event_listeners or []
        self.logger = logging.getLogger(__name__)
        
        # Connect streamer to decoder
        self.decoder_listener = DecoderListener(block_processor, block_registry)
        self.streamer.register_listener(self.decoder_listener)
    
    def add_event_listener(self, listener: EventListener) -> None:
        """
        Add an event listener to the pipeline.
        
        Args:
            listener: Event listener
        """
        self.event_listeners.append(listener)
    
    def process_block(self, block_number: int, force: bool = False) -> Dict[str, Any]:
        """
        Process a single block through the entire pipeline.
        
        Args:
            block_number: Block number
            force: Whether to force reprocessing
            
        Returns:
            Processing results
        """
        start_time = time.time()
        results = {
            "block_number": block_number,
            "streaming": False,
            "decoding": False,
            "transformation": False,
            "event_count": 0,
            "processing_time": 0,
            "errors": []
        }
        
        try:
            # Step 1: Ensure raw block is available
            raw_block_available = self.streamer.raw_block_exists(block_number)
            
            if not raw_block_available:
                # Fetch from RPC
                self.logger.info(f"Fetching block {block_number} from RPC")
                raw_block = self.streamer.fetch_block(block_number)
                
                # Save raw block
                raw_path = self.streamer.save_raw_block(block_number, raw_block)
                
                # Extract metadata and register block
                self._extract_and_register_block(block_number, raw_block, raw_path)
                
                results["streaming"] = True
            else:
                self.logger.debug(f"Block {block_number} already exists in storage")
                results["streaming"] = True
            
            # Step 2: Decode block if needed
            block_info = self.block_registry.get_block_info(block_number)
            
            if not block_info:
                error_msg = f"Block {block_number} not found in registry"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                return results
            
            # Check if decoded block already exists
            needs_decoding = (force or 
                             block_info.decoded_storage_status != "AVAILABLE" or
                             block_info.processing_status != "VALID")
            
            if needs_decoding:
                # Get raw block path
                raw_path = block_info.raw_block_path
                
                if not raw_path:
                    error_msg = f"Raw block path not found for block {block_number}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
                    return results
                
                # Process block
                success, result = self.block_processor.process_block(raw_path, force)
                
                if not success:
                    error_msg = f"Failed to decode block {block_number}: {result.get('errors', [])}"
                    self.logger.error(error_msg)
                    results["errors"].append(error_msg)
                    return results
                
                results["decoding"] = True
            else:
                self.logger.debug(f"Block {block_number} already decoded")
                results["decoding"] = True
            
            # Step 3: Transform block to generate business events
            block_info = self.block_registry.get_block_info(block_number)
            
            if not block_info or not block_info.decoded_block_path:
                error_msg = f"Decoded block path not found for block {block_number}"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                return results
            
            # Load decoded block
            decoded_block = self._load_decoded_block(block_number)
            
            if not decoded_block:
                error_msg = f"Failed to load decoded block {block_number}"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                return results
            
            # Transform block
            try:
                business_events = self.transformation_manager.process_block(decoded_block)
                
                # Count events
                event_count = sum(len(events) for events in business_events.values())
                results["event_count"] = event_count
                
                # Dispatch events to listeners
                for tx_hash, events in business_events.items():
                    for listener in self.event_listeners:
                        try:
                            listener.process_events(events, block_number, tx_hash)
                        except Exception as e:
                            error_msg = f"Error in event listener: {str(e)}"
                            self.logger.error(error_msg)
                            results["errors"].append(error_msg)
                
                # Update event count in registry
                self.block_registry.set_event_count(block_number, event_count)
                
                results["transformation"] = True
                
                self.logger.info(f"Generated {event_count} business events from block {block_number}")
            except Exception as e:
                error_msg = f"Error in transformation layer: {str(e)}"
                self.logger.error(error_msg)
                results["errors"].append(error_msg)
                return results
            
            # Calculate processing time
            results["processing_time"] = time.time() - start_time
            
            return results
            
        except Exception as e:
            error_msg = f"Error processing block {block_number}: {str(e)}"
            self.logger.error(error_msg)
            results["errors"].append(error_msg)
            results["processing_time"] = time.time() - start_time
            return results
    
    def process_blocks(self, block_numbers: List[int], force: bool = False) -> List[Dict[str, Any]]:
        """
        Process multiple blocks through the pipeline.
        
        Args:
            block_numbers: List of block numbers
            force: Whether to force reprocessing
            
        Returns:
            List of processing results
        """
        results = []
        
        for block_number in block_numbers:
            result = self.process_block(block_number, force)
            results.append(result)
        
        return results
    
    def start_continuous_processing(self, start_block: Optional[int] = None, 
                                   end_block: Optional[int] = None,
                                   sleep_seconds: int = 5) -> None:
        """
        Start continuous processing of blocks.
        
        This method uses the streamer to get new blocks and processes them
        through the pipeline.
        
        Args:
            start_block: Starting block number (optional)
            end_block: Ending block number (optional)
            sleep_seconds: Seconds to sleep between checks for new blocks
        """
        self.logger.info(f"Starting continuous processing from block {start_block or 'latest'}")
        
        # Start streaming blocks
        # This will trigger on_new_block in the decoder listener for each new block
        self.streamer.stream_blocks(start_block)
    
    def reprocess_missing_blocks(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        """
        Find and reprocess missing blocks in a range.
        
        Args:
            start_block: Start block number
            end_block: End block number
            
        Returns:
            List of processing results
        """
        # Find missing blocks
        missing_blocks = self.block_registry.get_missing_blocks(start_block, end_block)
        
        if not missing_blocks:
            self.logger.info(f"No missing blocks found in range {start_block} to {end_block}")
            return []
        
        self.logger.info(f"Found {len(missing_blocks)} missing blocks in range {start_block} to {end_block}")
        
        # Process missing blocks
        return self.process_blocks(missing_blocks)
    
    def reprocess_failed_blocks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Reprocess blocks that failed previously.
        
        Args:
            limit: Maximum number of blocks to reprocess
            
        Returns:
            List of processing results
        """
        # Get failed blocks
        failed_blocks = self.block_registry.get_blocks_by_status("INVALID", limit)
        block_numbers = [block.block_number for block in failed_blocks]
        
        if not block_numbers:
            self.logger.info("No failed blocks found")
            return []
        
        self.logger.info(f"Reprocessing {len(block_numbers)} failed blocks")
        
        # Process failed blocks
        return self.process_blocks(block_numbers, force=True)
    
    def _extract_and_register_block(self, block_number: int, raw_block: Union[bytes, Dict], 
                                   raw_path: str) -> None:
        """
        Extract metadata from raw block and register in registry.
        
        Args:
            block_number: Block number
            raw_block: Raw block data
            raw_path: Path to raw block file
        """
        # Parse block data if needed
        if isinstance(raw_block, bytes):
            try:
                raw_block = json.loads(raw_block)
            except Exception as e:
                self.logger.error(f"Error parsing raw block: {str(e)}")
                return
        
        # Extract metadata
        block_hash = raw_block.get('hash', '')
        parent_hash = raw_block.get('parentHash', '')
        timestamp = raw_block.get('timestamp', 0)
        if isinstance(timestamp, str) and timestamp.startswith('0x'):
            timestamp = int(timestamp, 16)
            timestamp = datetime.fromtimestamp(timestamp)
        
        # Register block
        self.block_registry.register_block(
            block_number=block_number,
            block_hash=block_hash,
            parent_hash=parent_hash,
            timestamp=timestamp
        )
        
        # Update block path
        self.block_registry.update_block_status(
            block_number=block_number,
            storage_type="raw",
            status="AVAILABLE",
            path=raw_path
        )
    
    def _load_decoded_block(self, block_number: int) -> Optional[Dict]:
        """
        Load decoded block from storage.
        
        Args:
            block_number: Block number
            
        Returns:
            Decoded block data
        """
        # Get block info
        block_info = self.block_registry.get_block_info(block_number)
        
        if not block_info:
            self.logger.error(f"Block {block_number} not found in registry")
            return None
        
        # Get storage handler from block processor
        storage_handler = getattr(self.block_processor, 'storage_handler', None)
        
        if not storage_handler:
            self.logger.error("Storage handler not found in block processor")
            return None
        
        # Load decoded block
        try:
            decoded_block = storage_handler.get_decoded_block(block_number)
            return decoded_block
        except Exception as e:
            self.logger.error(f"Error loading decoded block: {str(e)}")
            return None