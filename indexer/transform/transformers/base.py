# indexer/transform/transformers/base.py

from abc import ABC
from typing import List, Any, Optional, Dict, Tuple

from ...types import (
    ZERO_ADDRESS,
    DecodedLog,
    EvmAddress,
    Signal,
    ProcessingError,
    ErrorId,
    create_transform_error,
    EvmHash,
)
from ...utils.amounts import is_positive
from ...core.logging import log_with_context, LoggingMixin, INFO, DEBUG, WARNING, ERROR, CRITICAL


class BaseTransformer(ABC, LoggingMixin):
    def __init__(self, contract_address: Optional[str] = None):
        self.contract_address = EvmAddress(contract_address.lower()) if contract_address else None
        self.name = self.__class__.__name__
        self.handler_map = {}
        
        self.log_info("Transformer initialized", 
                     contract_address=self.contract_address,
                     transformer_name=self.name)
    
    def process_logs(self, logs: List[DecodedLog]) -> Tuple[
        Optional[Dict[int, Signal]], Optional[Dict[ErrorId, ProcessingError]]
    ]:
        """Process logs with comprehensive error handling and logging"""
        if not isinstance(logs, list):
            self.log_error("Invalid logs parameter - expected list",
                          logs_type=type(logs).__name__,
                          transformer_name=self.name)
            raise TypeError("logs parameter must be a list")
            
        signals, errors = {}, {}
        
        self.log_debug("Starting log processing", 
                      log_count=len(logs),
                      transformer_name=self.name,
                      contract_address=self.contract_address)

        if not logs:
            self.log_debug("No logs to process",
                          transformer_name=self.name)
            return None, None

        # Validate handler map
        if not self.handler_map:
            self.log_error("Transformer has no handler map configured - this is a configuration error",
                          transformer_name=self.name,
                          contract_address=self.contract_address)
            raise ValueError(f"Transformer {self.name} has no handler map configured")

        try:
            processed_count = 0
            skipped_count = 0
            error_count = 0
            
            for log in logs:
                try:
                    if not isinstance(log, DecodedLog):
                        self.log_error("Invalid log type in logs list",
                                      log_type=type(log).__name__,
                                      transformer_name=self.name)
                        error_count += 1
                        continue
                        
                    self.log_debug("Processing individual log",
                                  log_index=log.index,
                                  log_name=log.name,
                                  contract=log.contract,
                                  transformer_name=self.name)
                    
                    handler = self.handler_map.get(log.name)
                    if handler:
                        # Validate log before processing
                        if self._validate_log(log, errors):
                            handler(log, signals, errors)
                            processed_count += 1
                            
                            self.log_debug("Log processed successfully",
                                          log_index=log.index,
                                          log_name=log.name,
                                          handler_name=handler.__name__,
                                          transformer_name=self.name)
                        else:
                            skipped_count += 1
                            self.log_warning("Log validation failed",
                                           log_index=log.index,
                                           log_name=log.name,
                                           transformer_name=self.name)
                    else:
                        skipped_count += 1
                        self.log_debug("No handler found for log",
                                      log_index=log.index,
                                      log_name=log.name,
                                      available_handlers=list(self.handler_map.keys()),
                                      transformer_name=self.name)

                except Exception as e:
                    self.log_error("Exception while processing individual log",
                                  log_index=getattr(log, 'index', 'unknown'),
                                  error=str(e),
                                  exception_type=type(e).__name__,
                                  transformer_name=self.name)
                    self._create_log_exception(e, getattr(log, 'index', -1), errors)
                    error_count += 1

        except Exception as e:
            self.log_error("Exception during log processing loop",
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_general_exception(e, errors)
            error_count += 1
        
        # Log final statistics
        signal_count = len(signals)
        final_error_count = len(errors)
        
        # Calculate success rate
        total_attempted = processed_count + skipped_count + error_count
        success_rate = f"{(processed_count/total_attempted*100):.1f}%" if total_attempted > 0 else "0%"
        
        self.log_info("Log processing completed",
                     transformer_name=self.name,
                     total_logs=len(logs),
                     processed_logs=processed_count,
                     skipped_logs=skipped_count,
                     error_logs=error_count,
                     signals_generated=signal_count,
                     errors_generated=final_error_count,
                     success_rate=success_rate)
        
        # Log signal breakdown
        if signals:
            signal_types = {}
            for signal in signals.values():
                signal_type = type(signal).__name__
                signal_types[signal_type] = signal_types.get(signal_type, 0) + 1
            
            self.log_debug("Signal type breakdown",
                          transformer_name=self.name,
                          signal_types=signal_types)
        
        # Log error breakdown  
        if errors:
            error_types = {}
            for error in errors.values():
                error_types[error.error_type] = error_types.get(error.error_type, 0) + 1
            
            self.log_warning("Error type breakdown",
                           transformer_name=self.name,
                           error_types=error_types)
        
        # Fail loudly if too many errors
        if final_error_count > 0 and processed_count == 0:
            self.log_error("No logs processed successfully - all logs failed",
                          transformer_name=self.name,
                          total_errors=final_error_count,
                          total_logs=len(logs))
        
        return signals if signals else None, errors if errors else None
    
    def _validate_log(self, log: DecodedLog, errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate log before processing"""
        try:
            # Basic validation
            if not log.name:
                self.log_error("Log missing name - this should not happen in decoded logs",
                              log_index=log.index,
                              transformer_name=self.name)
                self._create_validation_error("Missing log name", log.index, errors)
                return False
                
            if not log.contract:
                self.log_error("Log missing contract address",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                self._create_validation_error("Missing contract address", log.index, errors)
                return False
                
            if not hasattr(log, 'attributes') or log.attributes is None:
                self.log_debug("Log has no attributes - may be valid for some events",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                # Not necessarily an error, some events might have no attributes
                
            # Contract-specific validation
            if self.contract_address and log.contract.lower() != self.contract_address.lower():
                self.log_error("Contract address mismatch",
                              expected_contract=self.contract_address,
                              actual_contract=log.contract,
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                self._create_validation_error(
                    f"Contract mismatch: expected {self.contract_address}, got {log.contract}",
                    log.index, errors
                )
                return False
                
            return True
            
        except Exception as e:
            self.log_error("Exception during log validation",
                          log_index=getattr(log, 'index', 'unknown'),
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_validation_error(f"Validation exception: {str(e)}", log.index, errors)
            return False
    
    def _create_transform_error(self, error_type: str, message: str, 
                               log_index: Optional[int] = None, 
                               tx_hash: Optional[EvmHash] = None) -> ProcessingError:
        """Create a transform error with consistent context"""
        return create_transform_error(
            error_type=error_type,
            message=message,
            tx_hash=tx_hash,
            contract_address=self.contract_address,
            transformer_name=self.name,
            log_index=log_index
        )

    def _create_general_exception(self, e: Exception, 
                                 errors: Dict[ErrorId, ProcessingError],
                                 tx_hash: Optional[EvmHash] = None) -> None:
        """Handle general processing exceptions"""
        error = self._create_transform_error(
            error_type="processing_exception",
            message=f"Transformer processing exception: {str(e)}",
        )
        errors[error.error_id] = error

        self.log_error("General processing exception",
                      error=str(e),
                      exception_type=type(e).__name__,
                      tx_hash=tx_hash,
                      contract_address=self.contract_address,
                      transformer_name=self.name,
                      error_id=error.error_id)
    
    def _create_log_exception(self, e: Exception, log_index: int, 
                             errors: Dict[ErrorId, ProcessingError],
                             tx_hash: Optional[EvmHash] = None) -> None:
        """Handle log-specific processing exceptions"""
        
        error = self._create_transform_error(
            error_type="log_processing_exception",
            message=f"Log processing exception: {str(e)}",
            log_index=log_index,
        )
        errors[error.error_id] = error

        self.log_error("Log processing exception",
                      error=str(e),
                      exception_type=type(e).__name__,
                      tx_hash=tx_hash,
                      contract_address=self.contract_address,
                      transformer_name=self.name,
                      log_index=log_index,
                      error_id=error.error_id)

    def _create_attr_error(self, log_index: int, errors: Dict[ErrorId, ProcessingError]) -> None:
        """Create attribute validation error"""
        error = self._create_transform_error(
            error_type="missing_attributes",
            message="Required attributes missing or invalid in log",
            log_index=log_index
        )
        errors[error.error_id] = error

        self.log_error("Attribute validation error - this indicates data quality issues",
                      log_index=log_index,
                      transformer_name=self.name,
                      error_id=error.error_id)

    def _create_validation_error(self, message: str, log_index: int, 
                                errors: Dict[ErrorId, ProcessingError]) -> None:
        """Create validation error"""
        error = self._create_transform_error(
            error_type="validation_failed",
            message=message,
            log_index=log_index
        )
        errors[error.error_id] = error

        self.log_error("Validation error",
                      log_index=log_index,
                      message=message,
                      transformer_name=self.name,
                      error_id=error.error_id)

    def _validate_null_attr(self, values: List[Any], log_index: int, 
                        errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate that required attributes are not null/None"""
        if not isinstance(values, list):
            self.log_error("Invalid values parameter for null validation",
                          values_type=type(values).__name__,
                          log_index=log_index,
                          transformer_name=self.name)
            raise TypeError("values parameter must be a list")
            
        null_count = 0
        null_indices = []
        
        for i, v in enumerate(values):
            # Check for None (null in Python)
            if v is None:
                null_count += 1
                null_indices.append(i)
                self.log_debug("Found None value in validation", 
                            log_index=log_index,
                            value_index=i,
                            transformer_name=self.name)
            # Check for empty strings (but allow integers, including 0)
            elif isinstance(v, str) and v == "":
                null_count += 1
                null_indices.append(i)
                self.log_debug("Found empty string in validation",
                            log_index=log_index, 
                            value_index=i,
                            transformer_name=self.name)
        
        if null_count > 0:
            self.log_error("Null/empty attribute validation failed",
                          log_index=log_index,
                          null_values=null_count,
                          null_indices=null_indices,
                          total_values=len(values),
                          transformer_name=self.name)
            
            self._create_attr_error(log_index, errors)
            return False
            
        self.log_debug("Attribute validation passed",
                      log_index=log_index,
                      attribute_count=len(values),
                      transformer_name=self.name)
        return True
    
    def _validate_addresses(self, *addresses: str) -> bool:
        """Validate Ethereum addresses"""
        for i, addr in enumerate(addresses):
            if not addr:
                self.log_warning("Empty address found in validation", 
                                address_index=i,
                                transformer_name=self.name)
                return False
            if addr == ZERO_ADDRESS:
                self.log_debug("Zero address found - may be valid", 
                              address_index=i, 
                              address=addr,
                              transformer_name=self.name)
                # Zero address might be valid in some contexts, so just log
                continue
            if not isinstance(addr, str) or len(addr) != 42 or not addr.startswith('0x'):
                self.log_error("Invalid address format", 
                              address_index=i, 
                              address=addr,
                              address_length=len(addr) if addr else 0,
                              transformer_name=self.name)
                return False
        return True

    def _validate_amounts(self, *amounts: Any) -> bool:
        """Validate amount values (accepts both int and string)"""
        for i, amount in enumerate(amounts):
            # Handle None
            if amount is None:
                self.log_error("Amount is None", 
                              amount_index=i,
                              transformer_name=self.name)
                return False
            
            # Handle integers (from EVM/ABI)
            if isinstance(amount, (int, float)):
                if amount < 0:
                    self.log_error("Negative amount found", 
                                  amount_index=i, 
                                  amount=amount,
                                  transformer_name=self.name)
                    return False
                continue
            
            # Handle strings    
            if isinstance(amount, str):
                if not amount or amount == "":
                    self.log_debug("Empty amount string found", 
                                  amount_index=i,
                                  transformer_name=self.name)
                    continue
                try:
                    amount_val = int(amount)
                    if amount_val < 0:
                        self.log_error("Negative string amount", 
                                      amount_index=i, 
                                      amount=amount,
                                      transformer_name=self.name)
                        return False
                except ValueError:
                    self.log_error("Invalid amount string - cannot parse as integer", 
                                  amount_index=i, 
                                  amount=amount,
                                  transformer_name=self.name)
                    return False
            else:
                self.log_error("Amount is not int or string", 
                              amount_index=i, 
                              amount_type=type(amount).__name__,
                              transformer_name=self.name)
                return False
        
        return True

    def _validate_log_attributes(self, log: DecodedLog, required_attrs: List[str],
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate that log has required attributes"""
        if not isinstance(required_attrs, list):
            self.log_error("Invalid required_attrs parameter",
                          required_attrs_type=type(required_attrs).__name__,
                          transformer_name=self.name)
            raise TypeError("required_attrs must be a list")
            
        missing_attrs = []
        
        for attr in required_attrs:
            if attr not in log.attributes or log.attributes[attr] is None:
                missing_attrs.append(attr)
        
        if missing_attrs:
            self.log_error("Required attributes missing from log",
                          log_index=log.index,
                          log_name=log.name,
                          missing_attributes=missing_attrs,
                          available_attributes=list(log.attributes.keys()),
                          transformer_name=self.name)
            
            error = self._create_transform_error(
                error_type="missing_required_attributes",
                message=f"Missing required attributes: {missing_attrs}",
                log_index=log.index
            )
            errors[error.error_id] = error
            return False
            
        self.log_debug("All required attributes present",
                      log_index=log.index,
                      log_name=log.name,
                      required_attributes=required_attrs,
                      transformer_name=self.name)
        return True