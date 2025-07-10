# indexer/database/writers/domain_event_writer.py

from typing import Dict, Tuple, Any
import traceback

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
                
                # 2. Write domain events to structured tables
                events_written, events_skipped = self._write_events(
                    session, events, tx_hash, block_number, timestamp
                )
                
                # 3. Write positions to positions table
                positions_written = self._write_positions(
                    session, positions, tx_hash, block_number, timestamp
                )
                
                # 4. Update transaction processing with final counts
                self._mark_transaction_complete(
                    session, tx_hash, events_written + positions_written
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
        
        processing_record = session.query(TransactionProcessing).filter(
            TransactionProcessing.tx_hash == tx_hash
        ).first()
        
        if processing_record is None:
            # Create new transaction processing record
            processing_record = TransactionProcessing(
                tx_hash=tx_hash,
                block_number=block_number,
                timestamp=timestamp,
                status=TransactionStatus.PROCESSING,
                tx_index=0,  # Default value, can be updated later if needed
                logs_processed=0,  # Will be updated when marking complete
                events_generated=0,  # Will be updated when marking complete
                tx_success=tx_success
            )
            session.add(processing_record)
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
                event_data = self._extract_event_data(event, repository)

                event_data.update({
                    'content_id': event_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp
                })
                
                log_with_context(
                    self.logger, logging.DEBUG, "About to write event",
                    event_id=event_id,
                    event_type=type(event).__name__,
                    event_data_keys=list(event_data.keys()),
                    event_data=event_data
                )
                
                repository.create(session, **event_data)

                events_written += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "Event written successfully",
                    event_id=event_id,
                    event_type=type(event).__name__
                )
                
            except Exception as e:
                import traceback

                log_with_context(
                    self.logger, logging.ERROR, "Failed to write event",
                    event_id=event_id,
                    event_type=type(event).__name__ if event else "Unknown",
                    error=str(e),
                    exception_type=type(e).__name__,
                    traceback=traceback.format_exc(),
                    event_data_keys=list(event_data.keys()) if 'event_data' in locals() else "not_extracted",
                    event_data=event_data if 'event_data' in locals() else "not_extracted"
                )
                # Continue processing other events rather than failing the entire batch
                continue
        
        return events_written, events_skipped
    
    def _write_positions(
        self,
        session: Session,
        positions: Dict[DomainEventId, Position],
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int
    ) -> int:
        """Write positions to positions table, returning written count"""
        
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
                
                # Extract position data for database
                position_data = self._extract_position_data(position)
                position_data.update({
                    'content_id': position_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp
                })
                
                self.repository_manager.positions.create(session, **position_data)
                positions_written += 1
                
                log_with_context(
                    self.logger, logging.DEBUG, "Position written successfully",
                    position_id=position_id
                )
                
            except Exception as e:
                log_with_context(
                    self.logger, logging.ERROR, "Failed to write position",
                    position_id=position_id,
                    error=str(e),
                    exception_type=type(e).__name__,
                    traceback=traceback.format_exc()
                )
                # Continue processing other positions
                continue
        
        return positions_written
    
    def _mark_transaction_complete(
        self,
        session: Session,
        tx_hash: EvmHash,
        events_generated: int
    ) -> None:
        """Mark transaction as complete with final counts"""
        
        processing_record = session.query(TransactionProcessing).filter(
            TransactionProcessing.tx_hash == tx_hash
        ).first()
        
        if processing_record:
            processing_record.status = TransactionStatus.COMPLETED
            processing_record.events_generated = events_generated
    
    def _get_event_repository(self, event: Any):
        """Get appropriate repository for domain event type"""
        event_type = type(event).__name__.lower()
        
        if event_type == 'trade':
            return self.repository_manager.trades
        elif event_type in ['poolswap', 'pool_swap']:
            return self.repository_manager.pool_swaps
        elif event_type in ['transfer', 'unknowntransfer']:
            return self.repository_manager.transfers
        elif event_type == 'liquidity':
            return self.repository_manager.liquidity
        elif event_type == 'reward':
            return self.repository_manager.rewards
        else:
            raise ValueError(f"Unknown event type: {event_type}. "
                           f"Available types: Trade, PoolSwap, Transfer, Liquidity, Reward")
    
    def _extract_event_data(self, event: Any, repository: Any) -> Dict:
        """Extract database-specific data from domain event, with enhanced error handling and conversion"""
        
        # Step 1: Extract raw data from event
        if hasattr(event, 'to_dict'):
            # msgspec.Struct with to_dict method
            raw_data = event.to_dict()
        elif hasattr(event, '__dict__'):
            # Regular object with attributes
            raw_data = {
                attr: getattr(event, attr) 
                for attr in dir(event) 
                if not attr.startswith('_') and not callable(getattr(event, attr))
            }
        else:
            # Try to extract key attributes manually
            raw_data = {}
            common_attrs = ['taker', 'pool', 'direction', 'base_token', 'base_amount', 
                          'quote_token', 'quote_amount', 'trade_type', 'user', 'token', 
                          'amount', 'position_type', 'from_address', 'to_address', 
                          'provider', 'action', 'contract', 'recipient', 'reward_type']
            
            for attr in common_attrs:
                if hasattr(event, attr):
                    raw_data[attr] = getattr(event, attr)
        
        log_with_context(
            self.logger, logging.DEBUG, "Extracted raw event data",
            event_type=type(event).__name__,
            raw_fields=list(raw_data.keys()),
            raw_data=raw_data
        )
        
        # Step 2: Remove fields that are handled separately or don't belong in database
        excluded_fields = ['content_id', 'tx_hash', 'timestamp', 'block_number', 'signals', 'positions', 'swaps', 'transfers']
        filtered_data = {k: v for k, v in raw_data.items() if k not in excluded_fields}
        
        # Step 3: Get valid database columns
        if hasattr(repository, 'model_class') and hasattr(repository.model_class, '__table__'):
            valid_columns = {col.name for col in repository.model_class.__table__.columns}
            
            # Step 4: Filter to only valid columns
            db_filtered_data = {}
            for key, value in filtered_data.items():
                if key in valid_columns:
                    db_filtered_data[key] = value
            
            # Step 5: Apply event-type specific conversions
            converted_data = self._convert_event_data(event, db_filtered_data)
            
            log_with_context(
                self.logger, logging.DEBUG, "Filtered and converted event data",
                event_type=type(event).__name__,
                original_fields=len(raw_data),
                filtered_fields=len(db_filtered_data),
                final_fields=len(converted_data),
                excluded_fields=[k for k in filtered_data.keys() if k not in valid_columns],
                final_data=converted_data
            )
            
            return converted_data
        else:
            # Fallback to original filtering if can't inspect model
            log_with_context(
                self.logger, logging.WARNING, "Using fallback event data filtering - no model introspection",
                event_type=type(event).__name__,
                field_count=len(filtered_data)
            )
            
            return filtered_data
    
    def _convert_event_data(self, event: Any, data: Dict) -> Dict:
        """Apply generic data conversions for database compatibility"""
        converted_data = data.copy()
        
        try:
            # Get the database model class to understand expected types
            repository = self._get_event_repository(event)
            if not hasattr(repository, 'model_class') or not hasattr(repository.model_class, '__table__'):
                return converted_data
            
            model_class = repository.model_class
            
            # Generic enum conversion: string -> enum instance
            for column in model_class.__table__.columns:
                field_name = column.name
                if field_name in converted_data:
                    field_value = converted_data[field_name]
                    
                    # Check if this column expects an enum
                    if hasattr(column.type, 'enum_class') and column.type.enum_class:
                        enum_class = column.type.enum_class
                        
                        # Convert string to enum instance if needed
                        if isinstance(field_value, str) and hasattr(enum_class, '__members__'):
                            # Find enum member by value (your enums use lowercase values)
                            for enum_member in enum_class:
                                if enum_member.value == field_value:
                                    converted_data[field_name] = enum_member
                                    log_with_context(
                                        self.logger, logging.DEBUG, "Converted enum field",
                                        field_name=field_name,
                                        string_value=field_value,
                                        enum_value=enum_member
                                    )
                                    break
                        elif not isinstance(field_value, enum_class):
                            log_with_context(
                                self.logger, logging.WARNING, "Unexpected enum field type",
                                field_name=field_name,
                                expected_enum=enum_class.__name__,
                                actual_type=type(field_value).__name__,
                                actual_value=field_value
                            )
            
            # Calculate derived fields based on rich domain event data
            self._add_derived_fields(event, converted_data)
            
        except Exception as e:
            log_with_context(
                self.logger, logging.WARNING, "Error during event data conversion",
                event_type=type(event).__name__,
                error=str(e),
                data=data
            )
            # Return original data if conversion fails
            return data
        
        return converted_data
    
    def _add_derived_fields(self, event: Any, data: Dict) -> None:
        """Add derived fields that are calculated from rich domain event data"""
        event_type = type(event).__name__.lower()
        
        # Calculate swap_count for Trade events
        if event_type == 'trade' and hasattr(event, 'swaps') and event.swaps:
            if 'swap_count' not in data:
                data['swap_count'] = len(event.swaps)
        
        # Add more derived field calculations as needed...
    
    def _extract_position_data(self, position: Position) -> Dict:
        """Extract database-specific data from position"""
        if hasattr(position, 'to_dict'):
            data = position.to_dict()
        else:
            # Extract key position attributes
            data = {
                'user': getattr(position, 'user', None),
                'token': getattr(position, 'token', None),
                'amount': getattr(position, 'amount', None),
                'position_type': getattr(position, 'position_type', None)
            }
        
        # Remove fields handled separately
        excluded_fields = ['content_id', 'tx_hash', 'timestamp', 'block_number']
        filtered_data = {k: v for k, v in data.items() if k not in excluded_fields}
        
        log_with_context(
            self.logger, logging.DEBUG, "Extracted position data",
            position_type=type(position).__name__,
            field_count=len(filtered_data)
        )
        
        return filtered_data