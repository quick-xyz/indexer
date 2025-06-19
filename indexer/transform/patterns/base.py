# indexer/transform/patterns/base.py

from typing import Dict
from abc import ABC, abstractmethod

from ..context import TransformContext
from ...types import (
    Signal,
    EvmAddress,
    ZERO_ADDRESS,
    TransferSignal,
    DomainEventId,
    DomainEvent,
    Position
)
from ...core.mixins import LoggingMixin
from ...utils.amounts import amount_to_negative_str


class TransferPattern(ABC, LoggingMixin):
    def __init__(self, name: str):
        if not name:
            raise ValueError("Pattern name cannot be empty")
        
        self.name = name
        
        self.log_debug("Pattern initialized", pattern_name=name)

    @abstractmethod
    def produce_events(self, signals: Dict[int, Signal], context: TransformContext) -> Dict[DomainEventId, DomainEvent]:
        """
        Produce events based on transfer signals.
        This method should be implemented by subclasses to define specific event
        production logic based on the transfer signals provided.
        """
        pass

    def _generate_positions(self, transfers: Dict[int, TransferSignal], context: TransformContext) -> Dict[DomainEventId, Position]:
        """Generate standard position changes from transfers"""
        if not transfers:
            self.log_debug("No transfers provided for position generation", 
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash if context else None)
            return {}
        
        if not context:
            self.log_error("TransformContext cannot be None for position generation",
                          pattern_name=self.name)
            raise ValueError("TransformContext cannot be None")
        
        try:
            positions = {}
            
            self.log_debug("Generating standard positions from transfers",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          transfer_count=len(transfers))
            
            for idx, transfer in transfers.items():
                if not transfer:
                    self.log_warning("Null transfer found in position generation",
                                   pattern_name=self.name,
                                   tx_hash=context.transaction.tx_hash,
                                   transfer_index=idx)
                    continue
                
                # Validate transfer data
                if not hasattr(transfer, 'to_address') or not hasattr(transfer, 'from_address'):
                    self.log_error("Transfer missing required address fields",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  transfer_index=idx)
                    continue
                
                # Generate position for recipient (if not zero address and is indexer token)
                if (transfer.to_address and 
                    transfer.to_address != ZERO_ADDRESS and 
                    transfer.token in context.indexer_tokens):
                    
                    try:
                        position_in = Position(
                            timestamp=context.transaction.timestamp,
                            tx_hash=context.transaction.tx_hash,
                            user=transfer.to_address,
                            custodian=transfer.to_address,
                            token=transfer.token,
                            amount=transfer.amount,
                        )
                        positions[position_in.content_id] = position_in
                        
                        self.log_debug("Created incoming position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      user=transfer.to_address,
                                      token=transfer.token,
                                      amount=transfer.amount)
                        
                    except Exception as e:
                        self.log_error("Failed to create incoming position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      transfer_index=idx,
                                      error=str(e),
                                      exception_type=type(e).__name__)

                # Generate position for sender (if not zero address and is indexer token)
                if (transfer.from_address and 
                    transfer.from_address != ZERO_ADDRESS and 
                    transfer.token in context.indexer_tokens):
                    
                    try:
                        position_out = Position(
                            timestamp=context.transaction.timestamp,
                            tx_hash=context.transaction.tx_hash,
                            user=transfer.from_address,
                            custodian=transfer.from_address,
                            token=transfer.token,
                            amount=amount_to_negative_str(transfer.amount),
                        )
                        positions[position_out.content_id] = position_out
                        
                        self.log_debug("Created outgoing position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      user=transfer.from_address,
                                      token=transfer.token,
                                      amount=amount_to_negative_str(transfer.amount))
                        
                    except Exception as e:
                        self.log_error("Failed to create outgoing position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      transfer_index=idx,
                                      error=str(e),
                                      exception_type=type(e).__name__)

            if positions:
                context.add_positions(positions)
                
                self.log_debug("Standard positions generated successfully",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              positions_created=len(positions))
            else:
                self.log_debug("No positions generated from transfers",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              transfer_count=len(transfers))
            
            return positions
            
        except Exception as e:
            self.log_error("Position generation failed",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash if context else None,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise
    
    def _generate_lp_positions(self, pool: EvmAddress, transfers: Dict[int, TransferSignal], context: TransformContext) -> Dict[DomainEventId, Position]:
        """Generate LP-specific position changes with pool as custodian"""
        if not transfers:
            self.log_debug("No transfers provided for LP position generation",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash if context else None,
                          pool=pool)
            return {}
        
        if not context:
            self.log_error("TransformContext cannot be None for LP position generation",
                          pattern_name=self.name)
            raise ValueError("TransformContext cannot be None")
        
        if not pool:
            self.log_error("Pool address cannot be empty for LP position generation",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash)
            raise ValueError("Pool address cannot be empty")
        
        try:
            positions = {}
            
            self.log_debug("Generating LP positions from transfers",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash,
                          pool=pool,
                          transfer_count=len(transfers))
            
            for idx, transfer in transfers.items():
                if not transfer:
                    self.log_warning("Null transfer found in LP position generation",
                                   pattern_name=self.name,
                                   tx_hash=context.transaction.tx_hash,
                                   transfer_index=idx)
                    continue
                
                # Validate transfer data
                if not hasattr(transfer, 'to_address') or not hasattr(transfer, 'from_address'):
                    self.log_error("Transfer missing required address fields",
                                  pattern_name=self.name,
                                  tx_hash=context.transaction.tx_hash,
                                  transfer_index=idx)
                    continue
                
                # Generate position for recipient (if not zero address, not pool, and is indexer token)
                if (transfer.to_address and 
                    transfer.to_address not in (ZERO_ADDRESS, pool) and 
                    transfer.token in context.indexer_tokens):
                    
                    try:
                        position_in = Position(
                            timestamp=context.transaction.timestamp,
                            tx_hash=context.transaction.tx_hash,
                            user=transfer.to_address,
                            custodian=pool,  # Pool acts as custodian for LP positions
                            token=transfer.token,
                            amount=transfer.amount,
                        )
                        positions[position_in.content_id] = position_in
                        
                        self.log_debug("Created LP incoming position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      user=transfer.to_address,
                                      custodian=pool,
                                      token=transfer.token,
                                      amount=transfer.amount)
                        
                    except Exception as e:
                        self.log_error("Failed to create LP incoming position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      transfer_index=idx,
                                      error=str(e),
                                      exception_type=type(e).__name__)

                # Generate position for sender (if not zero address, not pool, and is indexer token)
                if (transfer.from_address and 
                    transfer.from_address not in (ZERO_ADDRESS, pool) and 
                    transfer.token in context.indexer_tokens):
                    
                    try:
                        position_out = Position(
                            timestamp=context.transaction.timestamp,
                            tx_hash=context.transaction.tx_hash,
                            user=transfer.from_address,
                            custodian=pool,  # Pool acts as custodian for LP positions
                            token=transfer.token,
                            amount=amount_to_negative_str(transfer.amount),
                        )
                        positions[position_out.content_id] = position_out
                        
                        self.log_debug("Created LP outgoing position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      user=transfer.from_address,
                                      custodian=pool,
                                      token=transfer.token,
                                      amount=amount_to_negative_str(transfer.amount))
                        
                    except Exception as e:
                        self.log_error("Failed to create LP outgoing position",
                                      pattern_name=self.name,
                                      tx_hash=context.transaction.tx_hash,
                                      transfer_index=idx,
                                      error=str(e),
                                      exception_type=type(e).__name__)

            if positions:
                context.add_positions(positions)
                
                self.log_debug("LP positions generated successfully",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              pool=pool,
                              positions_created=len(positions))
            else:
                self.log_debug("No LP positions generated from transfers",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              pool=pool,
                              transfer_count=len(transfers))
            
            return positions
            
        except Exception as e:
            self.log_error("LP position generation failed",
                          pattern_name=self.name,
                          tx_hash=context.transaction.tx_hash if context else None,
                          pool=pool,
                          error=str(e),
                          exception_type=type(e).__name__)
            raise

    def _validate_signal_data(self, signals: Dict[int, Signal], context: TransformContext) -> bool:
        """Validate signals and context before processing"""
        if not signals:
            self.log_warning("No signals provided for pattern processing",
                           pattern_name=self.name,
                           tx_hash=context.transaction.tx_hash if context else None)
            return False
        
        if not context:
            self.log_error("TransformContext cannot be None",
                          pattern_name=self.name)
            return False
        
        # Validate signal indices and types
        for idx, signal in signals.items():
            if not isinstance(idx, int):
                self.log_error("Signal index must be integer",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              index_type=type(idx).__name__)
                return False
            
            if not signal:
                self.log_error("Signal cannot be None",
                              pattern_name=self.name,
                              tx_hash=context.transaction.tx_hash,
                              signal_index=idx)
                return False
        
        return True

    def _log_pattern_start(self, signals: Dict[int, Signal], context: TransformContext) -> None:
        """Log pattern processing start with context"""
        signal_types = [type(signal).__name__ for signal in signals.values()]
        
        self.log_debug("Pattern processing started",
                      pattern_name=self.name,
                      tx_hash=context.transaction.tx_hash,
                      signal_count=len(signals),
                      signal_types=signal_types)

    def _log_pattern_success(self, events: Dict[DomainEventId, DomainEvent], context: TransformContext) -> None:
        """Log successful pattern processing"""
        event_types = [type(event).__name__ for event in events.values()]
        
        self.log_debug("Pattern processing completed successfully",
                      pattern_name=self.name,
                      tx_hash=context.transaction.tx_hash,
                      events_created=len(events),
                      event_types=event_types)

    def _log_pattern_failure(self, reason: str, context: TransformContext, **additional_context) -> None:
        """Log pattern processing failure with context"""
        self.log_warning("Pattern processing failed",
                        pattern_name=self.name,
                        tx_hash=context.transaction.tx_hash,
                        reason=reason,
                        **additional_context)