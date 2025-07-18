# indexer/database/writers/domain_event_writer.py

from typing import Dict, Tuple, Any, List
import traceback
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..connection import ModelDatabaseManager
from ..model.tables.processing import DBTransactionProcessing, TransactionStatus
from ...core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ...types.new import EvmHash, DomainEventId
from ...types.model.positions import Position


class DomainEventWriter:
    """
    Service for writing domain events to the database with dual database support.
    
    FIXED: Updated to use the new database connection pattern where repositories
    are accessed through database managers instead of a central RepositoryManager.
    
    Handles thread-safe persistence of domain events, positions, and processing status
    updates for the indexing pipeline with optimized bulk operations.
    
    Architecture:
    - Full signals/events data stored in GCS blocks (stateful)
    - Structured domain events stored in indexer database tables (queryable)
    - Processing status with counts in transaction_processing table (monitoring)
    - Block prices and infrastructure data in shared database
    - Bulk operations for high-performance processing
    """
    
    def __init__(self, model_db_manager: ModelDatabaseManager):
        self.model_db_manager = model_db_manager
        self.logger = IndexerLogger.get_logger('database.writers.domain_event_writer')
        
        log_with_context(self.logger, INFO, "DomainEventWriter initialized")
    
    def write_transaction_results(
        self,
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int,
        events: Dict[DomainEventId, Any],  # Domain events to persist
        positions: Dict[DomainEventId, Position],  # Positions to persist
        tx_success: bool = True
    ) -> Tuple[int, int, int]:
        """
        Write domain events and positions for a transaction using bulk operations.
        
        Returns (events_written, positions_written, events_skipped)
        
        This method:
        1. Updates/creates transaction processing record
        2. Writes domain events to appropriate tables (using bulk operations)
        3. Writes positions to positions table (using bulk operations)
        4. Marks transaction as complete with counts
        """
        
        log_with_context(
            self.logger, DEBUG, "Writing transaction results (bulk)",
            tx_hash=tx_hash,
            block_number=block_number,
            event_count=len(events),
            position_count=len(positions),
            tx_success=tx_success
        )
        
        try:
            with self.model_db_manager.get_transaction() as session:
                # 1. Update or create transaction processing record
                self._update_transaction_processing(
                    session, tx_hash, block_number, timestamp, tx_success
                )
                
                # 2. Write domain events to structured tables (bulk operations)
                events_written, events_skipped = self._write_events_bulk(
                    session, events, tx_hash, block_number, timestamp
                )
                
                # 3. Write positions to positions table (bulk operations)
                positions_written = self._write_positions_bulk(
                    session, positions, tx_hash, block_number, timestamp
                )
                
                # 4. Mark transaction as complete with final counts
                self._mark_transaction_complete(
                    session, tx_hash, len(events)
                )
                
                log_with_context(
                    self.logger, INFO, "Transaction results written successfully (bulk)",
                    tx_hash=tx_hash,
                    events_written=events_written,
                    positions_written=positions_written,
                    events_skipped=events_skipped
                )
                
                return (events_written, positions_written, events_skipped)
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to write transaction results",
                tx_hash=tx_hash,
                error=str(e),
                exception_type=type(e).__name__,
                traceback=traceback.format_exc()
            )
            raise
    
    def _update_transaction_processing(
        self,
        session: Session,
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int,
        tx_success: bool
    ) -> None:
        """Update or create transaction processing record"""
        
        processing_repo = self.model_db_manager.get_processing_repo()
        processing_record = processing_repo.get_by_tx_hash(session, tx_hash)
        
        if not processing_record:
            # Create new processing record
            processing_record = processing_repo.create(session,
                tx_hash=tx_hash,
                block_number=block_number,
                timestamp=timestamp,
                status=TransactionStatus.PROCESSING,
                tx_success=tx_success
            )
        else:
            # Update existing record
            processing_record.status = TransactionStatus.PROCESSING
            processing_record.tx_success = tx_success
    
    def _write_events_bulk(
        self, 
        session: Session,
        events: Dict[DomainEventId, Any], 
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int
    ) -> Tuple[int, int]:
        """
        Write domain events to appropriate tables using bulk operations.
        
        Groups events by type and uses bulk operations for each table.
        Returns (events_written, events_skipped)
        """
        
        if not events:
            return (0, 0)
        
        try:
            # Group events by type and prepare data for bulk insertion
            events_by_type = self._group_events_by_type(events, tx_hash, block_number, timestamp)
            
            total_written = 0
            total_skipped = 0
            
            for event_type, event_data_list in events_by_type.items():
                try:
                    # Get appropriate repository for this event type
                    repository = self._get_event_repository(event_type)
                    
                    # Use bulk operations to insert/update events
                    written_count = repository.bulk_create_skip_existing(session, event_data_list)
                    skipped_count = len(event_data_list) - written_count
                    
                    total_written += written_count
                    total_skipped += skipped_count
                    
                    log_with_context(
                        self.logger, DEBUG, "Bulk wrote events by type",
                        event_type=event_type,
                        written=written_count,
                        skipped=skipped_count,
                        total=len(event_data_list)
                    )
                    
                except Exception as e:
                    log_with_context(
                        self.logger, ERROR, "Failed to bulk write events by type",
                        event_type=event_type,
                        event_count=len(event_data_list),
                        error=str(e)
                    )
                    # Continue with other event types instead of failing completely
                    continue
            
            return (total_written, total_skipped)
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to bulk write events",
                tx_hash=tx_hash,
                error=str(e),
                exception_type=type(e).__name__,
                traceback=traceback.format_exc()
            )
            raise
    
    def _write_positions_bulk(
        self,
        session: Session,
        positions: Dict[DomainEventId, Position],
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int
    ) -> int:
        """Write positions to positions table using bulk operations, returning written count"""
        
        if not positions:
            return 0
        
        try:
            # Prepare position data for bulk insert
            position_data_list = []
            
            for position_id, position in positions.items():
                position_data = self._extract_position_data(position)
                position_data.update({
                    'content_id': position_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp
                })
                position_data_list.append(position_data)
            
            # Get position repository and use bulk operations
            position_repo = self.model_db_manager.get_position_repo()
            written_count = position_repo.bulk_create_skip_existing(session, position_data_list)
            
            log_with_context(
                self.logger, DEBUG, "Bulk wrote positions",
                written=written_count,
                total_positions=len(position_data_list),
                skipped=len(position_data_list) - written_count
            )
            
            return written_count
            
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to bulk write positions",
                tx_hash=tx_hash,
                error=str(e),
                exception_type=type(e).__name__,
                traceback=traceback.format_exc()
            )
            raise
    
    def _mark_transaction_complete(
        self,
        session: Session,
        tx_hash: EvmHash,
        events_generated: int
    ) -> None:
        """Mark transaction as complete with final counts"""
        
        processing_repo = self.model_db_manager.get_processing_repo()
        processing_record = processing_repo.get_by_tx_hash(session, tx_hash)
        
        if processing_record:
            processing_record.status = TransactionStatus.COMPLETED
            processing_record.events_generated = events_generated
    
    def _group_events_by_type(
        self, 
        events: Dict[DomainEventId, Any], 
        tx_hash: EvmHash,
        block_number: int, 
        timestamp: int
    ) -> Dict[str, List[Dict]]:
        """Group events by type and prepare data for bulk insertion"""
        
        events_by_type = defaultdict(list)
        
        for event_id, event in events.items():
            try:
                event_type = type(event).__name__.lower()
                
                # Extract event data
                event_data = self._extract_event_data(event)
                event_data.update({
                    'content_id': event_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp
                })

                events_by_type[event_type].append(event_data)
                
                # FIXED: Extract and add nested pool swaps from Trade events
                if event_type == 'dbtrade' and hasattr(event, 'swaps') and event.swaps:
                    for swap_id, pool_swap in event.swaps.items():
                        swap_data = self._extract_event_data(pool_swap)
                        swap_data.update({
                            'content_id': swap_id,
                            'tx_hash': tx_hash,
                            'block_number': block_number,
                            'timestamp': timestamp,
                            'trade_id': event_id  # Link back to parent trade
                        })
                        events_by_type['dbpoolswap'].append(swap_data)
                
            except Exception as e:
                log_with_context(
                    self.logger, ERROR, "Error processing event for grouping",
                    event_id=event_id,
                    event_type=type(event).__name__,
                    error=str(e)
                )
                continue
        
        return events_by_type
    
    def _get_event_repository(self, event_type: str):
        """Get appropriate repository for event type"""
        event_type_lower = event_type.lower()
        
        if event_type_lower in ['dbtrade', 'trade']:
            return self.model_db_manager.get_trade_repo()
        elif event_type_lower in ['dbpoolswap', 'poolswap']:
            return self.model_db_manager.get_pool_swap_repo()
        elif event_type_lower in ['dbtransfer', 'transfer']:
            return self.model_db_manager.get_transfer_repo()
        elif event_type_lower in ['dbliquidity', 'liquidity']:
            return self.model_db_manager.get_liquidity_repo()
        elif event_type_lower in ['dbreward', 'reward']:
            return self.model_db_manager.get_reward_repo()
        elif event_type_lower in ['dbposition', 'position']:
            return self.model_db_manager.get_position_repo()
        else:
            raise ValueError(f"Unknown event type: {event_type}")
    
    def _extract_event_data(self, event) -> Dict[str, Any]:
        """Extract data from event object for database insertion"""
        try:
            if hasattr(event, 'to_dict'):
                return event.to_dict()
            elif hasattr(event, '__dict__'):
                # Filter out internal fields and convert to database format
                data = {}
                for key, value in event.__dict__.items():
                    if not key.startswith('_'):
                        data[key] = value
                return data
            else:
                raise ValueError(f"Cannot extract data from event type: {type(event)}")
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error extracting event data",
                event_type=type(event).__name__,
                error=str(e)
            )
            raise
    
    def _extract_position_data(self, position: Position) -> Dict[str, Any]:
        """Extract data from position object for database insertion"""
        try:
            if hasattr(position, 'to_dict'):
                return position.to_dict()
            elif hasattr(position, '__dict__'):
                data = {}
                for key, value in position.__dict__.items():
                    if not key.startswith('_'):
                        data[key] = value
                return data
            else:
                raise ValueError(f"Cannot extract data from position type: {type(position)}")
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Error extracting position data",
                position_type=type(position).__name__,
                error=str(e)
            )
            raise