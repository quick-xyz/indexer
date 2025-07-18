# indexer/database/writers/domain_event_writer.py

from typing import Dict, Tuple, Any, List
import traceback
from collections import defaultdict

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..repository_manager import RepositoryManager
from ..indexer.tables.processing import TransactionProcessing, TransactionStatus
from ...core.logging import IndexerLogger, log_with_context, INFO, DEBUG, WARNING, ERROR, CRITICAL
from ...types.new import EvmHash, DomainEventId
from ...types.model.positions import Position


class DomainEventWriter:
    """
    Service for writing domain events to the database with dual database support.
    
    Handles thread-safe persistence of domain events, positions, and processing status
    updates for the indexing pipeline with optimized bulk operations.
    
    Architecture:
    - Full signals/events data stored in GCS blocks (stateful)
    - Structured domain events stored in indexer database tables (queryable)
    - Processing status with counts in transaction_processing table (monitoring)
    - Block prices and infrastructure data in shared database
    - Bulk operations for high-performance processing
    """
    
    def __init__(self, repository_manager: RepositoryManager):
        self.repository_manager = repository_manager
        self.logger = IndexerLogger.get_logger('database.writers.domain_event_writer')
        
        log_with_context(self.logger, INFO, "DomainEventWriter initialized",
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
        Write domain events and positions for a transaction using bulk operations.
        
        Returns (events_written, positions_written, events_skipped)
        
        This method:
        1. Updates/creates transaction processing record
        2. Writes domain events to appropriate tables (using bulk operations)
        3. Writes positions to positions table (using bulk operations)
        4. Marks transaction as complete with counts
        """
        
        print(f"🔍 DEBUG: DomainEventWriter received:")
        print(f"   TX Hash: {tx_hash}")
        print(f"   Event count: {len(events)}")
        print(f"   Position count: {len(positions)}")
        if events:
            print(f"   Event types: {[type(event).__name__ for event in events.values()]}")
            for i, (event_id, event) in enumerate(list(events.items())[:2]):  # Show first 2 events
                print(f"   Event {i+1}: {event_id} = {type(event).__name__}")
                print(f"     String: {str(event)}")

        log_with_context(
            self.logger, DEBUG, "Writing transaction results (bulk)",
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
                
                # 2. Write domain events to structured tables (bulk operations)
                events_written, events_skipped = self._write_events_bulk(
                    session, events, tx_hash, block_number, timestamp
                )
                
                # 3. Write positions to positions table (bulk operations)
                positions_written = self._write_positions_bulk(
                    session, positions, tx_hash, block_number, timestamp
                )
                
                # 4. Update transaction processing with final counts
                self._mark_transaction_complete(
                    session, tx_hash, events_written + positions_written
                )
                
                log_with_context(
                    self.logger, DEBUG, "Transaction results written successfully (bulk)",
                    tx_hash=tx_hash,
                    events_written=events_written,
                    positions_written=positions_written,
                    events_skipped=events_skipped
                )
                
                return events_written, positions_written, events_skipped
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to write transaction results (bulk)",
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
    
    def _write_events_bulk(
        self,
        session: Session,
        events: Dict[DomainEventId, Any],
        tx_hash: EvmHash,
        block_number: int,
        timestamp: int
    ) -> Tuple[int, int]:
        """Write domain events to structured tables using bulk operations, returning (written_count, skipped_count)"""
        if not events:
            return 0, 0
        
        try:
            # Group events by type for bulk operations
            events_by_type = self._group_events_by_type(events, tx_hash, block_number, timestamp)
            
            total_written = 0
            total_skipped = 0
            
            # Bulk insert each event type
            for event_type, event_data_list in events_by_type.items():
                if not event_data_list:
                    continue
                    
                repository = self._get_event_repository_by_type(event_type)
                
                # Use bulk_create_skip_existing to handle duplicates efficiently
                written_count = repository.bulk_create_skip_existing(session, event_data_list)
                skipped_count = len(event_data_list) - written_count
                
                total_written += written_count
                total_skipped += skipped_count
                
                log_with_context(
                    self.logger, DEBUG, "Bulk wrote events",
                    event_type=event_type,
                    written=written_count,
                    skipped=skipped_count,
                    total_events=len(event_data_list)
                )
            
            log_with_context(
                self.logger, DEBUG, "All events bulk written",
                total_written=total_written,
                total_skipped=total_skipped,
                event_types=len(events_by_type)
            )
            
            return total_written, total_skipped
            
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
            
            # Bulk insert positions with duplicate checking
            written_count = self.repository_manager.positions.bulk_create_skip_existing(
                session, position_data_list
            )
            
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
        
        processing_record = session.query(TransactionProcessing).filter(
            TransactionProcessing.tx_hash == tx_hash
        ).first()
        
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
                # 🔍 DEBUG: Log each event we're processing
                log_with_context(
                    self.logger, INFO, "🔍 DEBUG: Processing individual event",
                    event_id=event_id,
                    event_type=type(event).__name__,
                    event_data_preview=str(event)[:200],
                    has_to_dict=hasattr(event, 'to_dict'),
                    has_dict=hasattr(event, '__dict__')
                )

                event_type = type(event).__name__.lower()
                
                # Get appropriate repository to extract data format
                repository = self._get_event_repository(event)
                
                # Extract event data
                event_data = self._extract_event_data(event, repository)
                event_data.update({
                    'content_id': event_id,
                    'tx_hash': tx_hash,
                    'block_number': block_number,
                    'timestamp': timestamp
                })
                
                # 🔍 DEBUG: Log extracted data
                log_with_context(
                    self.logger, INFO, "🔍 DEBUG: Extracted event data for DB",
                    event_id=event_id,
                    event_type=type(event).__name__,
                    event_type_lower=event_type,
                    extracted_keys=list(event_data.keys()),
                    extracted_data=event_data
                )

                events_by_type[event_type].append(event_data)
                
                # FIXED: Extract and add nested pool swaps from Trade events
                if event_type == 'trade' and hasattr(event, 'swaps') and event.swaps:
                    log_with_context(
                        self.logger, INFO, "🔍 DEBUG: Extracting nested pool swaps from trade",
                        trade_id=event_id,
                        swap_count=len(event.swaps)
                    )
                    
                    try:
                        for swap_id, pool_swap in event.swaps.items():
                            try:
                                # Extract pool swap data for database (don't modify the original object)
                                swap_repository = self._get_event_repository_by_type('poolswap')
                                swap_data = self._extract_event_data(pool_swap, swap_repository)
                                swap_data.update({
                                    'content_id': swap_id,
                                    'tx_hash': tx_hash,
                                    'block_number': block_number,
                                    'timestamp': timestamp,
                                    'trade_id': event_id  # Link back to parent trade
                                })
                                
                                events_by_type['poolswap'].append(swap_data)
                                
                                log_with_context(
                                    self.logger, INFO, "🔍 DEBUG: Added nested pool swap to database events",
                                    trade_id=event_id,
                                    swap_id=swap_id,
                                    swap_pool=swap_data.get('pool', 'unknown')
                                )
                                
                            except Exception as swap_error:
                                log_with_context(
                                    self.logger, ERROR, "🔍 DEBUG: Failed to extract individual pool swap",
                                    trade_id=event_id,
                                    swap_id=swap_id,
                                    error=str(swap_error),
                                    exception_type=type(swap_error).__name__,
                                    traceback=traceback.format_exc()
                                )
                                # Continue with other swaps instead of failing entirely
                                continue
                                
                    except Exception as e:
                        log_with_context(
                            self.logger, ERROR, "🔍 DEBUG: Failed to extract nested pool swaps from trade",
                            trade_id=event_id,
                            error=str(e),
                            exception_type=type(e).__name__,
                            traceback=traceback.format_exc()
                        )
                        # Continue processing the trade without swaps
                    
            except Exception as e:
                log_with_context(
                    self.logger, ERROR, "Failed to prepare event for bulk insert",
                    event_id=event_id,
                    event_type=type(event).__name__ if event else "Unknown",
                    error=str(e),
                    exception_type=type(e).__name__,
                    traceback=traceback.format_exc()
                )
                # Continue processing other events
                continue
        
        # 🔍 DEBUG: Log grouping results
        log_with_context(
            self.logger, INFO, "🔍 DEBUG: Events grouped by type",
            total_events=len(events),
            event_types_found=list(events_by_type.keys()),
            counts_by_type={k: len(v) for k, v in events_by_type.items()}
        )

        print(f"🔍 DEBUG: Events grouped by type:")
        for event_type, event_list in events_by_type.items():
            print(f"   {event_type}: {len(event_list)} events")
            if event_list:
                sample_event = event_list[0]
                print(f"     Sample data: {sample_event}")

        return events_by_type
    
    def _get_event_repository(self, event: Any):
        """Get appropriate repository for domain event type"""
        event_type = type(event).__name__.lower()
        return self._get_event_repository_by_type(event_type)
    
    def _get_event_repository_by_type(self, event_type: str):
        """Get appropriate repository by event type string"""
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
                           f"Supported types: trade, poolswap, transfer, liquidity, reward")
    
    def _extract_event_data(self, event: Any, repository) -> Dict[str, Any]:
        """Extract event data in format suitable for repository"""
        try:
            print(f"🔍 DEBUG: Extracting event data for {type(event).__name__}")
            print(f"   Event string: {str(event)}")
            print(f"   Has __dict__: {hasattr(event, '__dict__')}")
            print(f"   Has to_dict: {hasattr(event, 'to_dict')}")

            # Use to_dict() method for msgspec objects (preferred)
            if hasattr(event, 'to_dict'):
                raw_data = event.to_dict()
                print(f"   Raw data from to_dict(): {raw_data}")
                
                # Filter out complex nested objects that don't belong in database
                excluded_fields = ['signals', 'positions', 'swaps', 'transfers']
                event_data = {k: v for k, v in raw_data.items() 
                             if k not in excluded_fields and not k.startswith('_')}
                
                # Convert complex types to appropriate database format
                for key, value in event_data.items():
                    if hasattr(value, 'hex'):  # Handle hash types
                        event_data[key] = value.hex()
                    elif isinstance(value, dict):  # Skip complex nested objects
                        continue
                    elif hasattr(value, '__str__') and not isinstance(value, (int, float, bool)):
                        event_data[key] = str(value)
                
                print(f"   Final extracted data: {event_data}")
                return event_data
                
            # Fallback to __dict__ for other objects
            elif hasattr(event, '__dict__'):
                raw_attrs = {k: v for k, v in event.__dict__.items() if not k.startswith('_')}
                print(f"   Raw attributes from __dict__: {raw_attrs}")
                
                event_data = {}
                for key, value in raw_attrs.items():
                    # Convert complex types to appropriate database format
                    if hasattr(value, 'hex'):  # Handle hash types
                        event_data[key] = value.hex()
                    elif isinstance(value, dict):  # Skip complex nested objects
                        continue
                    elif hasattr(value, '__str__') and not isinstance(value, (int, float, bool)):
                        event_data[key] = str(value)
                    else:
                        event_data[key] = value
                
                print(f"   Final extracted data: {event_data}")
                return event_data
            else:
                print(f"   No extraction method available")
                return {}
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to extract event data",
                event_type=type(event).__name__,
                error=str(e),
                traceback=traceback.format_exc()
            )
            return {}
    
    def _extract_position_data(self, position: Position) -> Dict[str, Any]:
        """Extract position data in format suitable for repository"""
        try:
            # Use to_dict() method for msgspec objects (preferred)
            if hasattr(position, 'to_dict'):
                raw_data = position.to_dict()
                
                # Filter out fields that don't belong in database
                excluded_fields = ['signals', 'positions']
                position_data = {k: v for k, v in raw_data.items() 
                               if k not in excluded_fields and not k.startswith('_')}
                
                # Convert complex types to appropriate database format
                for key, value in position_data.items():
                    if hasattr(value, 'hex'):  # Handle hash types
                        position_data[key] = value.hex()
                    elif hasattr(value, '__str__') and not isinstance(value, (int, float, bool)):
                        position_data[key] = str(value)
                
                return position_data
                
            # Fallback to __dict__ for other objects
            elif hasattr(position, '__dict__'):
                position_data = {}
                for key, value in position.__dict__.items():
                    if not key.startswith('_'):
                        # Convert complex types to appropriate database format
                        if hasattr(value, 'hex'):  # Handle hash types
                            position_data[key] = value.hex()
                        elif hasattr(value, '__str__') and not isinstance(value, (int, float, bool)):
                            position_data[key] = str(value)
                        else:
                            position_data[key] = value
                return position_data
            else:
                return {}
                
        except Exception as e:
            log_with_context(
                self.logger, ERROR, "Failed to extract position data",
                error=str(e),
                traceback=traceback.format_exc()
            )
            return {}