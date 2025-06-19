# indexer/transform/transformers/aggregators/agg_base.py

from typing import Dict, Tuple, List

from ..base import BaseTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
    ProcessingError,
    Signal,
    RouteSignal,
    MultiRouteSignal,
    ErrorId,
)
from ....utils.amounts import amount_to_str, is_zero


class AggregatorTransformer(BaseTransformer):    
    def __init__(self, contract: EvmAddress):
        if not contract:
            raise ValueError("Contract address is required for AggregatorTransformer")
            
        super().__init__(contract_address=contract.lower())
        self.handler_map = {}
        
        self.log_info("AggregatorTransformer initialized",
                     contract_address=self.contract_address,
                     transformer_name=self.name)

    def _get_route_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str, str]:
        """Extract route attributes from log with validation"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for route extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for route extraction")
                
            sender = str(log.attributes.get("sender", ""))
            to = str(log.attributes.get("to", ""))
            token_in = str(log.attributes.get("tokenIn", ""))
            token_out = str(log.attributes.get("tokenOut", ""))
            
            # Handle amount extraction with error checking
            try:
                amount_in = amount_to_str(log.attributes.get("amountIn", 0))
                amount_out = amount_to_str(log.attributes.get("amountOut", 0))
            except Exception as e:
                self.log_error("Failed to extract amounts from route log",
                              log_index=log.index,
                              error=str(e),
                              amount_in_raw=log.attributes.get("amountIn"),
                              amount_out_raw=log.attributes.get("amountOut"),
                              transformer_name=self.name)
                raise
            
            self.log_debug("Route attributes extracted",
                          log_index=log.index,
                          sender=sender,
                          to=to,
                          token_in=token_in,
                          token_out=token_out,
                          amount_in=amount_in,
                          amount_out=amount_out,
                          transformer_name=self.name)
            
            return sender, to, token_in, token_out, amount_in, amount_out
            
        except Exception as e:
            self.log_error("Exception in route attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise
    
    def _get_multi_route_attributes(self, log: DecodedLog) -> Tuple[str, str, List[str], List[str], List[str], List[str]]:
        """Extract multi-route attributes from log with validation"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for multi-route extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for multi-route extraction")
                
            sender = str(log.attributes.get("sender", ""))
            to = str(log.attributes.get("to", ""))
            
            # Extract token arrays
            tokens_in_raw = log.attributes.get("tokensIn", [])
            tokens_out_raw = log.attributes.get("tokensOut", [])
            amounts_in_raw = log.attributes.get("amountsIn", [])
            amounts_out_raw = log.attributes.get("amountsOut", [])
            
            if not isinstance(tokens_in_raw, list) or not isinstance(tokens_out_raw, list):
                self.log_error("Token arrays are not lists",
                              log_index=log.index,
                              tokens_in_type=type(tokens_in_raw).__name__,
                              tokens_out_type=type(tokens_out_raw).__name__,
                              transformer_name=self.name)
                raise ValueError("Token arrays must be lists")
            
            if not isinstance(amounts_in_raw, list) or not isinstance(amounts_out_raw, list):
                self.log_error("Amount arrays are not lists",
                              log_index=log.index,
                              amounts_in_type=type(amounts_in_raw).__name__,
                              amounts_out_type=type(amounts_out_raw).__name__,
                              transformer_name=self.name)
                raise ValueError("Amount arrays must be lists")
            
            # Convert to strings with error handling
            try:
                tokens_in = [str(t) for t in tokens_in_raw]
                tokens_out = [str(t) for t in tokens_out_raw]
                amounts_in = [amount_to_str(a) for a in amounts_in_raw]
                amounts_out = [amount_to_str(a) for a in amounts_out_raw]
            except Exception as e:
                self.log_error("Failed to convert multi-route arrays",
                              log_index=log.index,
                              error=str(e),
                              transformer_name=self.name)
                raise
            
            # Validate array lengths match
            if len(tokens_in) != len(amounts_in):
                self.log_error("Tokens in and amounts in array length mismatch",
                              log_index=log.index,
                              tokens_in_length=len(tokens_in),
                              amounts_in_length=len(amounts_in),
                              transformer_name=self.name)
                raise ValueError("Input token and amount arrays must have same length")
                
            if len(tokens_out) != len(amounts_out):
                self.log_error("Tokens out and amounts out array length mismatch",
                              log_index=log.index,
                              tokens_out_length=len(tokens_out),
                              amounts_out_length=len(amounts_out),
                              transformer_name=self.name)
                raise ValueError("Output token and amount arrays must have same length")
            
            self.log_debug("Multi-route attributes extracted",
                          log_index=log.index,
                          sender=sender,
                          to=to,
                          tokens_in_count=len(tokens_in),
                          tokens_out_count=len(tokens_out),
                          transformer_name=self.name)
            
            return sender, to, tokens_in, tokens_out, amounts_in, amounts_out
            
        except Exception as e:
            self.log_error("Exception in multi-route attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise

    def _validate_route_attributes(self, log: DecodedLog, route: Tuple[str, str, str, str, str, str],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate route attributes with comprehensive checks"""
        try:
            sender, to, token_in, token_out, amount_in, amount_out = route
            
            # Check for zero amounts (both amounts zero is invalid)
            if is_zero(amount_in) and is_zero(amount_out):
                self.log_error("Both swap amounts are zero - invalid route",
                              log_index=log.index,
                              amount_in=amount_in,
                              amount_out=amount_out,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate token addresses are not empty
            if not token_in or not token_out:
                self.log_error("Missing token addresses in route",
                              log_index=log.index,
                              token_in=token_in,
                              token_out=token_out,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate addresses if provided
            addresses_to_check = []
            if sender:
                addresses_to_check.append(sender)
            if to:
                addresses_to_check.append(to)
            if token_in:
                addresses_to_check.append(token_in)
            if token_out:
                addresses_to_check.append(token_out)
                
            if addresses_to_check and not self._validate_addresses(*addresses_to_check):
                self.log_error("Invalid addresses in route",
                              log_index=log.index,
                              sender=sender,
                              to=to,
                              token_in=token_in,
                              token_out=token_out,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            self.log_debug("Route attributes validation passed",
                          log_index=log.index,
                          transformer_name=self.name)
            return True
            
        except Exception as e:
            self.log_error("Exception during route validation",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_attr_error(log.index, errors)
            return False

    def _validate_multi_route_attributes(self, log: DecodedLog, route: Tuple[str, str, List[str], List[str], List[str], List[str]],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate multi-route attributes with comprehensive checks"""
        try:
            sender, to, tokens_in, tokens_out, amounts_in, amounts_out = route
            
            # Validate the basic structure
            route_data = [sender, to, tokens_in, tokens_out, amounts_in, amounts_out]
            if not self._validate_null_attr(route_data, log.index, errors):
                return False
            
            # Validate arrays are not empty
            if not tokens_in or not tokens_out or not amounts_in or not amounts_out:
                self.log_error("Empty arrays in multi-route",
                              log_index=log.index,
                              tokens_in_count=len(tokens_in) if tokens_in else 0,
                              tokens_out_count=len(tokens_out) if tokens_out else 0,
                              amounts_in_count=len(amounts_in) if amounts_in else 0,
                              amounts_out_count=len(amounts_out) if amounts_out else 0,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate all amounts are not zero
            zero_amounts_in = sum(1 for amt in amounts_in if is_zero(amt))
            zero_amounts_out = sum(1 for amt in amounts_out if is_zero(amt))
            
            if zero_amounts_in > 0 or zero_amounts_out > 0:
                self.log_warning("Zero amounts found in multi-route",
                                log_index=log.index,
                                zero_amounts_in=zero_amounts_in,
                                zero_amounts_out=zero_amounts_out,
                                transformer_name=self.name)
            
            # Validate token addresses
            all_tokens = tokens_in + tokens_out
            if all_tokens and not self._validate_addresses(*all_tokens):
                self.log_error("Invalid token addresses in multi-route",
                              log_index=log.index,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            self.log_debug("Multi-route attributes validation passed",
                          log_index=log.index,
                          tokens_in_count=len(tokens_in),
                          tokens_out_count=len(tokens_out),
                          transformer_name=self.name)
            return True
            
        except Exception as e:
            self.log_error("Exception during multi-route validation",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_attr_error(log.index, errors)
            return False
    
    def _handle_route(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle single route log with comprehensive error handling"""
        self.log_debug("Handling route log",
                      log_index=log.index,
                      log_name=log.name,
                      transformer_name=self.name)
        
        try:
            route = self._get_route_attributes(log)
            if not self._validate_route_attributes(log, route, errors):
                self.log_warning("Route validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            sender, to, token_in, token_out, amount_in, amount_out = route
            
            signal = RouteSignal(
                log_index=log.index,
                pattern="Route",
                contract=self.contract_address,
                token_in=EvmAddress(token_in.lower()),
                amount_in=amount_in,
                token_out=EvmAddress(token_out.lower()),
                amount_out=amount_out,
                to=EvmAddress(to.lower()) if to else None,
                sender=EvmAddress(sender.lower()) if sender else None,
            )
            
            signals[log.index] = signal
            
            self.log_info("Route signal created successfully",
                         log_index=log.index,
                         token_in=token_in,
                         token_out=token_out,
                         amount_in=amount_in,
                         amount_out=amount_out,
                         transformer_name=self.name)
            
        except Exception as e:
            self.log_error("Exception while handling route log",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            
            # Create error for this failure
            error = self._create_transform_error(
                error_type="route_handler_exception",
                message=f"Exception in route handler: {str(e)}",
                log_index=log.index
            )
            errors[error.error_id] = error
    
    def _handle_multi_route(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle multi-route log with comprehensive error handling"""
        self.log_debug("Handling multi-route log",
                      log_index=log.index,
                      log_name=log.name,
                      transformer_name=self.name)
        
        try:
            route = self._get_multi_route_attributes(log)
            if not self._validate_multi_route_attributes(log, route, errors):
                self.log_warning("Multi-route validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            sender, to, tokens_in, tokens_out, amounts_in, amounts_out = route
            
            signal = MultiRouteSignal(
                log_index=log.index,
                pattern="Route",
                contract=self.contract_address,
                tokens_in=[EvmAddress(t.lower()) for t in tokens_in],
                amounts_in=amounts_in,
                tokens_out=[EvmAddress(t.lower()) for t in tokens_out],
                amounts_out=amounts_out,
                to=EvmAddress(to.lower()) if to else None,
                sender=EvmAddress(sender.lower()) if sender else None,
            )
            
            signals[log.index] = signal
            
            self.log_info("Multi-route signal created successfully",
                         log_index=log.index,
                         tokens_in_count=len(tokens_in),
                         tokens_out_count=len(tokens_out),
                         total_amount_in=sum(float(amt) for amt in amounts_in),
                         total_amount_out=sum(float(amt) for amt in amounts_out),
                         transformer_name=self.name)
            
        except Exception as e:
            self.log_error("Exception while handling multi-route log",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            
            # Create error for this failure
            error = self._create_transform_error(
                error_type="multi_route_handler_exception",
                message=f"Exception in multi-route handler: {str(e)}",
                log_index=log.index
            )
            errors[error.error_id] = error