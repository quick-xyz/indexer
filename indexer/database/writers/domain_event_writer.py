# indexer/database/writers/domain_event_writer.py

from typing import Dict, Tuple, Any

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
    Service for writing domain events to the database with dual database support.
    
    Handles thread-safe persistence of domain events, positions, and processing status
    updates for the indexing pipeline.
    
    Architecture:
    - Full signals/events data stored in GCS blocks (stateful)
    - Structured domain events stored in indexer database tables (queryable)
    - Processing status with counts in transaction_processing table (monitoring)
    - Block prices and infrastructure data in shared database
    """
    
    def __init__(self, repository_manager: RepositoryManager):
        self.repository_manager = repository_manager
        self.logger = IndexerLogger.get_logger('database.writers.domain_event_writer')
        
        log_with_context(self.logger, logging.INFO, "DomainEventWriter initialized",
                        has_shared_db=repository_manager.has_shared_access())
    
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
        Write domain events and positions for a transaction.
        
        Returns (events_written, positions_written, events_skipped)
        
        This method:
        1. Updates/creates transaction processing record
        2. Writes domain events to appropriate tables
        3. Writes positions to positions table
        4. Marks transaction as complete with counts
        """
        
        log_with_context(
            self.logger, logging.DEBUG, "Writing transaction results",
            tx_hash=tx_hash,
            block_number=block_number,
            event_count=len(events),
            position_count=len(positions),
            tx_success=tx_success
        )
        
        try:
            with self.repository_manager.get_transaction() as session:
                # 1. Update or create transaction processing record
                self._update_transaction_processing(
                    session, tx_hash, block_number, timestamp, tx_success
                )
                
                # 2. Write domain events
                events_written, events_skipped = self._write_events(
                    session, events, tx_hash, block_number, timestamp
                )
                
                # 3. Write positions
                positions_written = self._write_positions(
                    session, positions, tx_hash, block_number, timestamp
                )
                
                # 4. Mark transaction as complete
                self._mark_transaction_complete(
                    session, tx_hash, events_written, positions_written
                )
                
                log_with_context(
                    self.logger, logging.DEBUG, "Transaction results written successfully",
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
                error=str(e),
                exception_type=type(e).__name__
            )
            
            # Try to mark transaction as failed
            try:
                with self.repository_manager.get_session() as session:
                    self._mark_transaction_failed(session, tx_hash, str(e))
            except Exception as mark_error:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to mark transaction as failed",
                    tx_hash=tx_hash,
                    mark_error=str(mark_error)
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
        
        processing_record = self.repository_manager.processing.get_by_tx_hash(session, tx_hash)
        
        if processing_record is None:
            # Create new processing record
            processing_record = self.repository_manager.processing.create(
                session,
                tx_hash=tx_hash,
                block_number=block_number,
                timestamp=timestamp,
                status=TransactionStatus.PROCESSING,
                tx_index=0,  # Default value, can be updated later if needed
                logs_processed=0,  # Will be updated when marking complete
                events_generated=0,  # Will be updated when marking complete
                tx_success=tx_success
            )
        else:
            # Update existing record to processing status
            processing_record.status = TransactionStatus.PROCESSING
            processing_record.tx_success = tx_success
        
        session.flush()
    
    def _write_events(
        self,
        session: Session,
        events: Dict[DomainEventId, Any],
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
                # Get appropriate repository for this event type
                repository = self._get_event_repository(event)
                
                # Check if event already exists
                existing_event = repository.get_by_content_id(session, event_id)
                
                if existing_event is not None:
                    events_skipped += 1
                    log_with_context(
                        self.logger, logging.DEBUG, "Event already exists, skipping",
                        event_id=event_id,
                        event_type=type(event).__name__
                    )
                    continue
                
                # Extract event data and create record
                event_data = self._extract_event_data(event)
                event_data.update({
                    'content_id': event_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp
                })
                
                repository.create(session, **event_data)
                events_written += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "Event written successfully",
                    event_id=event_id,
                    event_type=type(event).__name__
                )
                
            except IntegrityError:
                # Event already exists (race condition)
                events_skipped += 1
                session.rollback()
                session.begin()
                
                log_with_context(
                    self.logger, logging.DEBUG, "Event creation failed due to integrity constraint, skipping",
                    event_id=event_id
                )
                
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
                    log_with_context(
                        self.logger, logging.DEBUG, "Position already exists, skipping",
                        position_id=position_id
                    )
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
                
                log_with_context(
                    self.logger, logging.DEBUG, "Position written successfully",
                    position_id=position_id,
                    user=position.user,
                    token=position.token
                )
                
            except IntegrityError:
                # Position already exists (race condition)
                session.rollback()
                session.begin()
                
                log_with_context(
                    self.logger, logging.DEBUG, "Position creation failed due to integrity constraint, skipping",
                    position_id=position_id
                )
                
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
            # Update record with completion status and counts
            processing_record.status = TransactionStatus.COMPLETED
            processing_record.events_generated = events_written + positions_written
            processing_record.logs_processed = events_written  # Approximate - could be more precise
            
            # Update timestamps
            from datetime import datetime, timezone
            processing_record.last_processed_at = datetime.now(timezone.utc)
            
            log_with_context(
                self.logger, logging.DEBUG, "Transaction marked as complete",
                tx_hash=tx_hash,
                total_events=events_written + positions_written
            )
        else:
            log_with_context(
                self.logger, logging.WARNING, "No processing record found to mark complete",
                tx_hash=tx_hash
            )
    
    def _mark_transaction_failed(self, session: Session, tx_hash: EvmHash, error_message: str = None) -> None:
        """Mark transaction processing as failed"""
        
        processing_record = self.repository_manager.processing.get_by_tx_hash(session, tx_hash)
        
        if processing_record is not None:
            processing_record.status = TransactionStatus.FAILED
            if error_message:
                processing_record.error_message = error_message[:1000]  # Truncate if too long
            
            # Update timestamps
            from datetime import datetime, timezone
            processing_record.last_processed_at = datetime.now(timezone.utc)
            
            log_with_context(
                self.logger, logging.DEBUG, "Transaction marked as failed",
                tx_hash=tx_hash,
                error_message=error_message[:100] if error_message else None
            )
    
    def _get_event_repository(self, event: Any):
        """Get appropriate repository for domain event type"""
        event_type = type(event).__name__
        
        try:
            return self.repository_manager.get_event_repository(event_type)
        except ValueError as e:
            log_with_context(
                self.logger, logging.ERROR, "Unknown event type for repository routing",
                event_type=event_type,
                available_types=['Trade', 'PoolSwap', 'Transfer', 'Liquidity', 'Reward', 'Position']
            )
            raise ValueError(f"Unknown event type: {event_type}. Available types: Trade, PoolSwap, Transfer, Liquidity, Reward, Position")
    
    def _extract_event_data(self, event: Any) -> Dict:
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
            # Try to extract key attributes manually
            data = {}
            common_attrs = ['taker', 'pool', 'direction', 'base_token', 'base_amount', 
                          'quote_token', 'quote_amount', 'trade_type', 'user', 'token', 
                          'amount', 'position_type', 'from_address', 'to_address', 
                          'provider', 'action', 'contract', 'recipient', 'reward_type']
            
            for attr in common_attrs:
                if hasattr(event, attr):
                    data[attr] = getattr(event, attr)
        
        # Remove fields that are handled separately or don't belong in database
        excluded_fields = ['content_id', 'tx_hash', 'timestamp', 'block_number', 'signals', 'positions', 'swaps']
        filtered_data = {k: v for k, v in data.items() if k not in excluded_fields}
        
        # Convert any complex objects to strings if needed
        for key, value in filtered_data.items():
            if isinstance(value, dict) or isinstance(value, list):
                # Convert complex types to JSON strings or handle appropriately
                continue  # Most should be simple types, but this allows for expansion
        
        log_with_context(
            self.logger, logging.DEBUG, "Extracted event data",
            event_type=type(event).__name__,
            field_count=len(filtered_data)
        )
        
        return filtered_data