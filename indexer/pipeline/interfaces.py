"""
Interfaces for blockchain indexing pipeline components.

This module defines the interfaces for the integrated pipeline that
connects all components together.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union


class PipelineInterface(ABC):
    """Interface for integrated pipeline components."""
    
    @abstractmethod
    def process_block(self, block_number: int, force: bool = False) -> Dict[str, Any]:
        """
        Process a single block through the entire pipeline.
        
        Args:
            block_number: Block number
            force: Whether to force reprocessing
            
        Returns:
            Processing results
        """
        pass
    
class BlockProcessorInterface(ABC):
    """Interface for block processors."""
    
    @abstractmethod
    def process_block(self, block_data: Union[Dict[str, Any], EvmFilteredBlock]) -> Block:
        """
        Process a block.
        """
        pass
    

    @abstractmethod
    def process_blocks(self, block_numbers: List[int], force: bool = False) -> List[Dict[str, Any]]:
        """
        Process multiple blocks through the pipeline.
        
        Args:
            block_numbers: List of block numbers
            force: Whether to force reprocessing
            
        Returns:
            List of processing results
        """
        pass
    
    @abstractmethod
    def start_continuous_processing(self, start_block: Optional[int] = None, 
                                  end_block: Optional[int] = None) -> None:
        """
        Start continuous processing of blocks.
        
        Args:
            start_block: Starting block number (optional)
            end_block: Ending block number (optional)
        """
        pass
    
    @abstractmethod
    def reprocess_missing_blocks(self, start_block: int, end_block: int) -> List[Dict[str, Any]]:
        """
        Find and reprocess missing blocks in a range.
        
        Args:
            start_block: Start block number
            end_block: End block number
            
        Returns:
            List of processing results
        """
        pass
    
    @abstractmethod
    def reprocess_failed_blocks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Reprocess blocks that failed previously.
        
        Args:
            limit: Maximum number of blocks to reprocess
            
        Returns:
            List of processing results
        """
        pass