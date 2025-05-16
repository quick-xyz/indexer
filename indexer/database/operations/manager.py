"""
Database operations manager for the blockchain decoder.

This module provides a high-level interface for database operations:
- Block status tracking
- GCS object tracking
- Query operations
- Bulk operations
- Transaction management

TODO: Implement the DatabaseManager class
"""

from typing import List, Optional
from datetime import datetime
from sqlalchemy import desc

from ...utils.env import env
from ..db_models.status import ProcessingStatus, BlockProcess
from ..db_models.gcs import GcsObject
from .session import ConnectionManager
from ...processing.factory import ComponentFactory


class DatabaseManager:
    def __init__(self, db_conn: ConnectionManager):
        self.db = db_conn

    def record_block(self, block_number: int, gcs_path: str, 
                    status: ProcessingStatus = ProcessingStatus.PENDING) -> BlockProcess:
        """Record or update a block's validation status."""
        with self.db.get_session() as session:
            block = session.query(BlockProcess).get(block_number)
            if block:
                block.status = status
                block.gcs_path = gcs_path
                block.updated_at = datetime.now()
            else:
                block = BlockProcess(
                    block_number=block_number,
                    gcs_path=gcs_path,
                    status=status
                )
                session.add(block)
            session.commit()
            return block

    def update_status(self, block_number: int, status: ProcessingStatus, 
                     error_message: Optional[str] = None) -> BlockProcess:
        """Update validation status for a block."""
        with self.db.get_session() as session:
            block = session.query(BlockProcess).get(block_number)
            if not block:
                raise ValueError(f"Block {block_number} not found")
            
            block.status = status
            block.error_message = error_message
            block.updated_at = datetime.now()
            session.commit()
            return block

    def get_blocks_by_status(self, status: ProcessingStatus, 
                           limit: int = 100) -> List[BlockProcess]:
        """Get blocks with a specific status."""
        with self.db.get_session() as session:
            return session.query(BlockProcess)\
                .filter(BlockProcess.status == status)\
                .order_by(desc(BlockProcess.block_number))\
                .limit(limit)\
                .all()

    def get_block(self, block_number: int) -> Optional[BlockProcess]:
        """Get a specific block's validation status."""
        with self.db.get_session() as session:
            return session.query(BlockProcess).get(block_number)
        
    def sync_gcs_objects(self, prefix=None, limit=None, batch_size=1000):
        """
        Sync GCS objects to database in memory-efficient batches.
        
        Args:
            prefix: Optional prefix to filter GCS objects
            limit: Optional limit to number of objects to sync
            batch_size: Number of objects to process per batch
            
        Returns:
            Count of objects synced
        """
        
        gcs_handler = ComponentFactory.get_gcs_handler()
        
        # Process in batches to avoid memory issues
        count = 0
        batch_count = 0
        
        # Use an iterator to avoid loading all blobs into memory
        blob_iterator = gcs_handler.list_blobs(prefix=prefix)
        
        # Process in batches
        current_batch = []
        
        for blob in blob_iterator:
            # Stop if we've hit the limit
            if limit and count >= limit:
                break
                
            current_batch.append(blob)
            count += 1
            
            # Process batch if we've reached batch size
            if len(current_batch) >= batch_size:
                self._process_gcs_batch(current_batch)
                batch_count += 1
                self.logger.info(f"Processed batch {batch_count} ({len(current_batch)} objects)")
                current_batch = []  # Reset batch
        
        # Process any remaining objects
        if current_batch:
            self._process_gcs_batch(current_batch)
            batch_count += 1
            self.logger.info(f"Processed final batch {batch_count} ({len(current_batch)} objects)")
        
        return count

    def _process_gcs_batch(self, blobs):
        """Process a batch of GCS blobs to update the database."""
        
        with self.db.get_session() as session:
            for blob in blobs:
                try:
                    # Try to extract block number
                    block_number = None
                    try:
                        block_number = env.extract_block_number(blob.name)
                    except ValueError:
                        pass
                    
                    # Determine file type
                    file_type = 'unknown'
                    if blob.name.startswith(env.get_rpc_prefix()):
                        file_type = 'raw'
                    elif blob.name.startswith(env.get_decoded_prefix()):
                        file_type = 'decoded'
                    
                    # Create or update record
                    obj = session.query(GcsObject).filter(GcsObject.path == blob.name).one_or_none()
                    if obj:
                        obj.block_number = block_number
                        obj.file_type = file_type
                        obj.size = blob.size
                        obj.updated_at = datetime.now()
                    else:
                        obj = GcsObject(
                            path=blob.name,
                            block_number=block_number,
                            file_type=file_type,
                            size=blob.size
                        )
                        session.add(obj)
                except Exception as e:
                    self.logger.warning(f"Error processing blob {blob.name}: {e}")
                    
            # Commit the batch
            session.commit()

    def get_available_block_paths(self, file_type='raw', min_block=None, max_block=None, limit=None):
        """
        Get available block paths from database.
        
        Args:
            file_type: Type of files to get ('raw' or 'decoded')
            min_block: Optional minimum block number
            max_block: Optional maximum block number
            limit: Optional limit to number of results
            
        Returns:
            List of block paths
        """
        
        with self.db.get_session() as session:
            query = session.query(GcsObject.path).filter(GcsObject.file_type == file_type)
            
            if min_block is not None:
                query = query.filter(GcsObject.block_number >= min_block)
            
            if max_block is not None:
                query = query.filter(GcsObject.block_number <= max_block)
            
            if limit:
                query = query.limit(limit)
            
            return [row[0] for row in query.all()]

    def block_exists_in_gcs(self, block_number, file_type='raw'):
        """
        Check if a block exists in GCS.
        
        Args:
            block_number: Block number to check
            file_type: Type of file to check for ('raw' or 'decoded')
            
        Returns:
            True if block exists, False otherwise
        """
        
        with self.db.get_session() as session:
            return session.query(GcsObject).filter(
                GcsObject.block_number == block_number,
                GcsObject.file_type == file_type
            ).count() > 0