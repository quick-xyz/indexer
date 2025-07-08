# indexer/database/writers/domain_event_writer.py

from typing import Dict, Tuple

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..repository import RepositoryManager
from ..indexer.tables.processing import TransactionProcessing, TransactionStatus
from ...core.logging_config import IndexerLogger, log_with_context
from ...types.new import EvmHash, DomainEventId
from ...types.model.positions import Position

import logging


class DomainEventWriter:
    """
    Service for writing domain events to the database.
    
    Handles thread-safe persistence of domain events, positions, and processing status
    updates for the indexing pipeline.
    
    Architecture:
    - Full signals/events data stored in GCS blocks (stateful)
    - Structured domain events stored in dedicated database tables (queryable)
    - Processing status with counts in transaction_processing table (monitoring)
    """
    
    def __init__(self, repository_manager: RepositoryManager):
        self.repository_manager = repository_manager
        self.logger = IndexerLogger.get_logger('database.writers.domain_event_writer')
        
        log_with_context(self.logger, logging.INFO, "DomainEventWriter initialized")
    
    def write_transaction_results(
        self,
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int,
        events: Dict[DomainEventId, any],  # Domain events to persist
        positions: Dict[DomainEventId, Position],  # Positions to persist
        tx_success: bool = True
    ) -> Tuple[int, int, int]:
        """
        Write domain events and positions for a transaction.
        
        Args:
            tx_hash: Transaction hash
            block_number: Block number
            timestamp: Block timestamp
            events: Domain events to persist in structured tables
            positions: Position changes to persist
            tx_success: Whether the blockchain transaction succeeded
        
        Returns:
            Tuple of (events_written, positions_written, events_skipped)
        """
        
        with self.repository_manager.get_transaction() as session:
            try:
                # Update or create transaction processing record
                self._update_transaction_processing(
                    session, tx_hash, block_number, timestamp, tx_success
                )
                
                # Write domain events to structured tables
                events_written, events_skipped = self._write_events(
                    session, events, tx_hash, block_number, timestamp
                )
                
                # Write positions to structured tables
                positions_written = self._write_positions(
                    session, positions, tx_hash, block_number, timestamp
                )
                
                # Mark transaction as complete with counts
                self._mark_transaction_complete(
                    session, tx_hash, events_written, positions_written
                )
                
                log_with_context(
                    self.logger, logging.DEBUG, "Transaction results written",
                    tx_hash=tx_hash,
                    events_written=events_written,
                    positions_written=positions_written,
                    events_skipped=events_skipped
                )
                
                return events_written, positions_written, events_skipped
                
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to write transaction results",
                    tx_hash=tx_hash,
                    error=str(e)
                )
                
                # Mark transaction as failed with error
                self._mark_transaction_failed(session, tx_hash, str(e))
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
        
        processing_record = self.repository_manager.processing.get_by_tx_hash(session, tx_hash)
        
        if processing_record is None:
            # Create new processing record with required defaults
            processing_record = TransactionProcessing(
                tx_hash=tx_hash,
                block_number=block_number,
                timestamp=timestamp,
                status=TransactionStatus.PROCESSING,
                tx_index=0,  # Default position
                logs_processed=0,  # Default value for NOT NULL field
                events_generated=0  # Default value for NOT NULL field
            )
            session.add(processing_record)
        else:
            # Update existing record
            processing_record.mark_processing()
        
        session.flush()
    
    def _write_events(
        self,
        session: Session,
        events: Dict[DomainEventId, any],
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int
    ) -> Tuple[int, int]:
        """Write domain events to structured tables, returning (written_count, skipped_count)"""
        
        if not events:
            return 0, 0
        
        events_written = 0
        events_skipped = 0
        
        for event_id, event in events.items():
            try:
                # Check if event already exists
                existing_event = self._get_existing_event(session, event_id, event)
                
                if existing_event is not None:
                    events_skipped += 1
                    continue
                
                # Create new event record in appropriate table
                event_data = {
                    'content_id': event_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp,
                    **self._extract_event_data(event)
                }
                
                repository = self._get_event_repository(event)
                repository.create(session, **event_data)
                events_written += 1
                
            except IntegrityError:
                # Event already exists (race condition)
                events_skipped += 1
                session.rollback()
                session.begin()
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to write event",
                    event_id=event_id,
                    event_type=type(event).__name__,
                    error=str(e)
                )
                raise
        
        return events_written, events_skipped
    
    def _write_positions(
        self,
        session: Session,
        positions: Dict[DomainEventId, Position],
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int
    ) -> int:
        """Write positions to structured table, returning written count"""
        
        if not positions:
            return 0
        
        positions_written = 0
        
        for position_id, position in positions.items():
            try:
                # Check if position already exists
                existing_position = self.repository_manager.positions.get_by_content_id(
                    session, position_id
                )
                
                if existing_position is not None:
                    continue
                
                # Create new position record
                position_data = {
                    'content_id': position_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp,
                    'user': position.user,
                    'token': position.token,
                    'amount': str(position.amount),
                    'position_type': position.position_type
                }
                
                self.repository_manager.positions.create(session, **position_data)
                positions_written += 1
                
            except IntegrityError:
                # Position already exists (race condition)
                session.rollback()
                session.begin()
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to write position",
                    position_id=position_id,
                    error=str(e)
                )
                raise
        
        return positions_written
    
    def _mark_transaction_complete(
        self,
        session: Session,
        tx_hash: EvmHash,
        events_written: int,
        positions_written: int
    ) -> None:
        """Mark transaction processing as complete with counts"""
        
        processing_record = self.repository_manager.processing.get_by_tx_hash(session, tx_hash)
        
        if processing_record is not None:
            # Total events includes both domain events and positions
            total_events = events_written + positions_written
            processing_record.mark_complete(events_generated=total_events)
    
    def _mark_transaction_failed(self, session: Session, tx_hash: EvmHash, error_message: str = None) -> None:
        """Mark transaction processing as failed"""
        
        processing_record = self.repository_manager.processing.get_by_tx_hash(session, tx_hash)
        
        if processing_record is not None:
            processing_record.mark_failed(error_message=error_message)
    
    def _get_existing_event(self, session: Session, event_id: DomainEventId, event: any):
        """Check if domain event already exists"""
        repository = self._get_event_repository(event)
        return repository.get_by_content_id(session, event_id)
    
    def _get_event_repository(self, event: any):
        """Get appropriate repository for domain event type"""
        event_type = type(event).__name__
        
        if event_type in ['Trade']:
            return self.repository_manager.trades
        elif event_type in ['PoolSwap']:
            return self.repository_manager.pool_swaps
        elif event_type in ['Transfer']:
            return self.repository_manager.transfers
        elif event_type in ['Liquidity']:
            return self.repository_manager.liquidity
        elif event_type in ['Reward']:
            return self.repository_manager.rewards
        else:
            raise ValueError(f"Unknown event type: {event_type}")
    
    def _extract_event_data(self, event: any) -> Dict:
        """Extract database-specific data from domain event"""
        # Handle different event structures
        if hasattr(event, 'to_dict'):
            # msgspec.Struct with to_dict method
            data = event.to_dict()
        elif hasattr(event, '__dict__'):
            # Regular object with attributes
            data = {
                attr: getattr(event, attr) 
                for attr in dir(event) 
                if not attr.startswith('_') and not callable(getattr(event, attr))
            }
        else:
            # Fallback to empty dict
            data = {}
        
        # Remove fields that are handled separately
        excluded_fields = ['content_id', 'tx_hash', 'timestamp', 'block_number']
        return {k: v for k, v in data.items() if k not in excluded_fields}