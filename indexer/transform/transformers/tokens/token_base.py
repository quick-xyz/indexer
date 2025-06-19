# indexer/transform/transformers/tokens/token_base.py

from typing import Dict, Tuple

from ..base import BaseTransformer
from ....types import (
    ProcessingError,    
    DecodedLog,
    Signal,
    TransferSignal,
    ErrorId,
    EvmAddress,
)


class TokenTransformer(BaseTransformer):   
    def __init__(self, contract: str):
        if not contract:
            raise ValueError("Contract address is required for TokenTransformer")
            
        super().__init__(contract_address=contract)
        
        self.handler_map = {
            "Transfer": self._handle_transfer,
        }
        
        self.log_info("TokenTransformer initialized",
                     contract_address=self.contract_address,
                     handler_count=len(self.handler_map),
                     supported_events=list(self.handler_map.keys()),
                     transformer_type="ERC20_Token")

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        """Extract transfer attributes from ERC20 Transfer event"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for transfer extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for transfer extraction")
            
            # Extract raw attributes
            from_addr_raw = log.attributes.get("from", "")
            to_addr_raw = log.attributes.get("to", "")
            value_raw = log.attributes.get("value", 0)
            sender_raw = log.attributes.get("sender", "")
            
            # Convert addresses to strings with validation
            from_addr = str(from_addr_raw) if from_addr_raw is not None else ""
            to_addr = str(to_addr_raw) if to_addr_raw is not None else ""
            sender = str(sender_raw) if sender_raw is not None else ""
            
            # Validate required addresses are present
            if not from_addr or not to_addr:
                self.log_error("Missing required transfer addresses",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              available_attributes=list(log.attributes.keys()),
                              transformer_name=self.name)
                raise ValueError("Both from and to addresses are required for transfers")
            
            # Validate value is present and not None
            if value_raw is None:
                self.log_error("Transfer value is None",
                              log_index=log.index,
                              transformer_name=self.name)
                raise ValueError("Transfer value cannot be None")
            
            self.log_debug("Transfer attributes extracted successfully",
                          log_index=log.index,
                          from_addr=from_addr,
                          to_addr=to_addr,
                          value=value_raw,
                          value_type=type(value_raw).__name__,
                          sender=sender,
                          transformer_name=self.name)
            
            return from_addr, to_addr, value_raw, sender
            
        except Exception as e:
            self.log_error("Exception in transfer attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          available_attributes=list(log.attributes.keys()) if log.attributes else [],
                          transformer_name=self.name)
            raise

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate transfer data with comprehensive checks"""
        try:
            from_addr, to_addr, value, sender = trf
            
            # Check for None/null values explicitly
            if from_addr is None or to_addr is None or value is None:
                self.log_error("Transfer has null attributes - data quality issue",
                              log_index=log.index,
                              from_addr_null=from_addr is None,
                              to_addr_null=to_addr is None,
                              value_null=value is None,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Check for empty string addresses (critical failure)
            if (isinstance(from_addr, str) and from_addr == "") or \
               (isinstance(to_addr, str) and to_addr == ""):
                self.log_error("Transfer has empty address strings - invalid transfer",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate addresses format
            if not self._validate_addresses(from_addr, to_addr):
                self.log_error("Invalid address format in transfer",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Check if amount is zero (handle both int and string)
            is_zero_amount = False
            if isinstance(value, (int, float)):
                is_zero_amount = value == 0
            elif isinstance(value, str):
                is_zero_amount = value == "0" or value == ""
            else:
                self.log_error("Transfer value has unexpected type",
                              log_index=log.index,
                              value_type=type(value).__name__,
                              value=value,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            if is_zero_amount:
                self.log_error("Transfer amount is zero - invalid transfer",
                              log_index=log.index,
                              value=value,
                              value_type=type(value).__name__,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate amount is not negative
            try:
                if isinstance(value, (int, float)) and value < 0:
                    self.log_error("Transfer amount is negative",
                                  log_index=log.index,
                                  value=value,
                                  transformer_name=self.name)
                    self._create_attr_error(log.index, errors)
                    return False
                elif isinstance(value, str):
                    int_value = int(value)
                    if int_value < 0:
                        self.log_error("Transfer amount string is negative",
                                      log_index=log.index,
                                      value=value,
                                      transformer_name=self.name)
                        self._create_attr_error(log.index, errors)
                        return False
            except ValueError:
                self.log_error("Cannot parse transfer amount as integer",
                              log_index=log.index,
                              value=value,
                              value_type=type(value).__name__,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            self.log_debug("Transfer data validation passed",
                          log_index=log.index,
                          from_addr=from_addr,
                          to_addr=to_addr,
                          value=value,
                          transformer_name=self.name)
            return True
            
        except Exception as e:
            self.log_error("Exception during transfer validation",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_attr_error(log.index, errors)
            return False

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle ERC20 transfer events with comprehensive error handling"""
        self.log_debug("Handling ERC20 transfer log",
                      log_index=log.index,
                      log_name=log.name,
                      contract=log.contract,
                      transformer_name=self.name)

        try:
            trf = self._get_transfer_attributes(log)
            if not self._validate_transfer_data(log, trf, errors):
                self.log_error("Transfer validation failed - skipping signal creation",
                              log_index=log.index,
                              transformer_name=self.name)
                return
            
            from_addr, to_addr, value, sender = trf
            
            # Convert amount to string for signal creation
            try:
                amount_str = str(value)
            except Exception as e:
                self.log_error("Failed to convert transfer amount to string",
                              log_index=log.index,
                              value=value,
                              value_type=type(value).__name__,
                              error=str(e),
                              transformer_name=self.name)
                
                error = self._create_transform_error(
                    error_type="amount_conversion_failed",
                    message=f"Failed to convert amount to string: {str(e)}",
                    log_index=log.index
                )
                errors[error.error_id] = error
                return
            
            # Create transfer signal
            signal = TransferSignal(
                log_index=log.index,
                pattern="Transfer",
                token=self.contract_address,
                from_address=EvmAddress(from_addr.lower()),
                to_address=EvmAddress(to_addr.lower()),
                amount=amount_str,
                sender=EvmAddress(sender.lower()) if sender else None
            )
            
            signals[log.index] = signal
            
            self.log_info("Transfer signal created successfully",
                         log_index=log.index,
                         token=self.contract_address,
                         from_address=from_addr,
                         to_address=to_addr,
                         amount=amount_str,
                         has_sender=bool(sender),
                         transformer_name=self.name)
            
        except Exception as e:
            self.log_error("Exception while handling transfer log",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            
            error = self._create_transform_error(
                error_type="transfer_handler_exception",
                message=f"Exception in transfer handler: {str(e)}",
                log_index=log.index
            )
            errors[error.error_id] = error