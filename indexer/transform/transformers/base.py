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
from ...core.mixins import LoggingMixin


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
            self.log_warning("Transformer has no handler map configured",
                           transformer_name=self.name,
                           contract_address=self.contract_address)
            return None, None

        try:
            processed_count = 0
            skipped_count = 0
            
            for log in logs:
                try:
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
                                          handler_name=handler.__name__)
                        else:
                            skipped_count += 1
                            self.log_warning("Log validation failed",
                                           log_index=log.index,
                                           log_name=log.name)
                    else:
                        skipped_count += 1
                        self.log_debug("No handler found for log",
                                      log_index=log.index,
                                      log_name=log.name,
                                      available_handlers=list(self.handler_map.keys()))

                except Exception as e:
                    self._create_log_exception(e, log.index, errors)

        except Exception as e:
            self._create_general_exception(e, errors)
        
        # Log final statistics
        signal_count = len(signals)
        error_count = len(errors)
        
        self.log_info("Log processing completed",
                     transformer_name=self.name,
                     total_logs=len(logs),
                     processed_logs=processed_count,
                     skipped_logs=skipped_count,
                     signals_generated=signal_count,
                     errors_generated=error_count,
                     success_rate=f"{(processed_count/len(logs)*100):.1f}%" if logs else "0%")
        
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
        
        return signals if signals else None, errors if errors else None
    
    def _validate_log(self, log: DecodedLog, errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate log before processing"""
        try:
            # Basic validation
            if not log.name:
                self._create_validation_error("Missing log name", log.index, errors)
                return False
                
            if not log.contract:
                self._create_validation_error("Missing contract address", log.index, errors)
                return False
                
            if not log.attributes:
                self.log_debug("Log has no attributes",
                              log_index=log.index,
                              log_name=log.name)
                # Not necessarily an error, some events might have no attributes
                
            # Contract-specific validation
            if self.contract_address and log.contract.lower() != self.contract_address.lower():
                self._create_validation_error(
                    f"Contract mismatch: expected {self.contract_address}, got {log.contract}",
                    log.index, errors
                )
                return False
                
            return True
            
        except Exception as e:
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

        self.log_warning("Attribute validation error",
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

        self.log_warning("Validation error",
                        log_index=log_index,
                        message=message,
                        transformer_name=self.name,
                        error_id=error.error_id)

    def _validate_null_attr(self, values: List[Any], log_index: int, 
                      errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate that required attributes are not null"""
        null_count = sum(1 for v in values if v is None or v == "")
        
        if null_count > 0:
            self.log_warning("Null attribute validation failed",
                           log_index=log_index,
                           null_values=null_count,
                           total_values=len(values),
                           transformer_name=self.name)
            
            self._create_attr_error(log_index, errors)
            return False
            
        self.log_debug("Attribute validation passed",
                      log_index=log_index,
                      attribute_count=len(values))
        return True
    
    def _validate_addresses(self, *addresses: str) -> bool:
        """Validate Ethereum addresses"""
        for i, addr in enumerate(addresses):
            if not addr:
                self.log_debug("Empty address found", address_index=i)
                return False
            if addr == ZERO_ADDRESS:
                self.log_debug("Zero address found", address_index=i, address=addr)
                # Zero address might be valid in some contexts, so just log
                continue
            if not isinstance(addr, str) or len(addr) != 42 or not addr.startswith('0x'):
                self.log_warning("Invalid address format", 
                               address_index=i, 
                               address=addr,
                               address_length=len(addr) if addr else 0)
                return False
        return True

    def _validate_amounts(self, *amounts: str) -> bool:
        """Validate amount strings"""
        for i, amount in enumerate(amounts):
            if not isinstance(amount, str):
                self.log_warning("Amount is not string", amount_index=i, amount_type=type(amount).__name__)
                return False
            if not amount or amount == "0":
                self.log_debug("Zero amount found", amount_index=i, amount=amount)
                continue
            if not is_positive(amount):
                self.log_warning("Invalid amount", amount_index=i, amount=amount)
                return False
        return True

    def _validate_log_attributes(self, log: DecodedLog, required_attrs: List[str],
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate that log has required attributes"""
        missing_attrs = []
        
        for attr in required_attrs:
            if attr not in log.attributes or log.attributes[attr] is None:
                missing_attrs.append(attr)
        
        if missing_attrs:
            self.log_warning("Required attributes missing",
                           log_index=log.index,
                           log_name=log.name,
                           missing_attributes=missing_attrs,
                           available_attributes=list(log.attributes.keys()))
            
            error = self._create_transform_error(
                error_type="missing_required_attributes",
                message=f"Missing required attributes: {missing_attrs}",
                log_index=log.index
            )
            errors[error.error_id] = error
            return False
            
        return True