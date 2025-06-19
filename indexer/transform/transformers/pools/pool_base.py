# indexer/transform/transformers/pools/pool_base.py

from typing import Optional, Dict, Tuple, Any

from ..base import BaseTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
    ProcessingError,
    Signal,
    SwapSignal,
    TransferSignal,
    LiquiditySignal,
    ErrorId,
)
from ....utils.amounts import amount_to_int, amount_to_str

class PoolTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, 
                 base_token: EvmAddress, fee_collector: Optional[EvmAddress] = None):
        if not contract or not token0 or not token1 or not base_token:
            raise ValueError("Contract address and all token addresses are required for PoolTransformer")
            
        super().__init__(contract_address=contract)
        
        self.handler_map = {
            "Swap": self._handle_swap,
            "Mint": self._handle_mint,
            "Burn": self._handle_burn,
            "Transfer": self._handle_transfer
        }
        
        self.token0 = token0
        self.token1 = token1
        self.base_token = base_token
        self.quote_token = self.token1 if self.token0 == self.base_token else self.token0
        self.fee_collector = fee_collector if fee_collector else None
        
        # Validate base token is one of the pool tokens
        if self.base_token not in [self.token0, self.token1]:
            self.log_error("Base token must be one of the pool tokens",
                          base_token=self.base_token,
                          token0=self.token0,
                          token1=self.token1,
                          transformer_name=self.name)
            raise ValueError(f"Base token {self.base_token} must be one of the pool tokens")
        
        self.log_info("PoolTransformer initialized",
                     contract_address=self.contract_address,
                     token0=self.token0,
                     token1=self.token1,
                     base_token=self.base_token,
                     quote_token=self.quote_token,
                     has_fee_collector=bool(self.fee_collector),
                     fee_collector=self.fee_collector,
                     handler_count=len(self.handler_map),
                     supported_events=list(self.handler_map.keys()),
                     transformer_type="AMM_Pool")

    def _get_in_out_amounts(self, log: DecodedLog) -> Tuple[str, str]:
        """Extract in/out amounts from swap event with validation"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for in/out amount extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for in/out amount extraction")
            
            # Extract raw amounts with error handling
            try:
                amount0_in = amount_to_int(log.attributes.get("amount0In", 0))
                amount0_out = amount_to_int(log.attributes.get("amount0Out", 0))
                amount1_in = amount_to_int(log.attributes.get("amount1In", 0))
                amount1_out = amount_to_int(log.attributes.get("amount1Out", 0))
            except Exception as e:
                self.log_error("Failed to extract in/out amounts from swap",
                              log_index=log.index,
                              error=str(e),
                              amount0_in_raw=log.attributes.get("amount0In"),
                              amount0_out_raw=log.attributes.get("amount0Out"),
                              amount1_in_raw=log.attributes.get("amount1In"),
                              amount1_out_raw=log.attributes.get("amount1Out"),
                              transformer_name=self.name)
                raise
            
            # Calculate net amounts
            amount0 = amount_to_str(amount0_out - amount0_in)
            amount1 = amount_to_str(amount1_out - amount1_in)
            
            # Map to base/quote based on token configuration
            if self.token0 == self.base_token:
                base_amount, quote_amount = amount0, amount1
            else:
                base_amount, quote_amount = amount1, amount0
            
            self.log_debug("In/out amounts extracted and calculated",
                          log_index=log.index,
                          amount0_in=amount0_in,
                          amount0_out=amount0_out,
                          amount1_in=amount1_in,
                          amount1_out=amount1_out,
                          amount0_net=amount0,
                          amount1_net=amount1,
                          base_amount=base_amount,
                          quote_amount=quote_amount,
                          transformer_name=self.name)
            
            return base_amount, quote_amount
            
        except Exception as e:
            self.log_error("Exception in in/out amount extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise

    def _get_amounts(self, log: DecodedLog) -> Tuple[str, str]:
        """Extract direct amounts from mint/burn events with validation"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for amount extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for amount extraction")
            
            # Extract amounts with error handling
            try:
                amount0 = amount_to_str(log.attributes.get("amount0", 0))
                amount1 = amount_to_str(log.attributes.get("amount1", 0))
            except Exception as e:
                self.log_error("Failed to extract amounts from liquidity event",
                              log_index=log.index,
                              error=str(e),
                              amount0_raw=log.attributes.get("amount0"),
                              amount1_raw=log.attributes.get("amount1"),
                              transformer_name=self.name)
                raise
            
            # Map to base/quote based on token configuration
            if self.token0 == self.base_token:
                base_amount, quote_amount = amount0, amount1
            else:
                base_amount, quote_amount = amount1, amount0
            
            self.log_debug("Direct amounts extracted",
                          log_index=log.index,
                          amount0=amount0,
                          amount1=amount1,
                          base_amount=base_amount,
                          quote_amount=quote_amount,
                          transformer_name=self.name)
            
            return base_amount, quote_amount
            
        except Exception as e:
            self.log_error("Exception in amount extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise

    def _get_swap_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        """Extract swap attributes with validation"""
        try:
            base_amount, quote_amount = self._get_in_out_amounts(log)
            
            to = str(log.attributes.get("to", ""))
            sender = str(log.attributes.get("sender", ""))
            
            # Validate required addresses
            if not to:
                self.log_error("Missing 'to' address in swap event",
                              log_index=log.index,
                              available_attributes=list(log.attributes.keys()),
                              transformer_name=self.name)
                raise ValueError("'to' address is required for swap events")
            
            self.log_debug("Swap attributes extracted",
                          log_index=log.index,
                          base_amount=base_amount,
                          quote_amount=quote_amount,
                          to=to,
                          sender=sender,
                          transformer_name=self.name)
            
            return base_amount, quote_amount, to, sender
            
        except Exception as e:
            self.log_error("Exception in swap attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        """Extract transfer attributes with validation"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for transfer extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for transfer extraction")
            
            from_addr = str(log.attributes.get("from", ""))
            to_addr = str(log.attributes.get("to", ""))
            sender = str(log.attributes.get("sender", ""))
            
            # Extract value with error handling
            try:
                value = amount_to_str(log.attributes.get("value", 0))
            except Exception as e:
                self.log_error("Failed to extract value from transfer event",
                              log_index=log.index,
                              error=str(e),
                              value_raw=log.attributes.get("value"),
                              transformer_name=self.name)
                raise
            
            # Validate required addresses
            if not from_addr or not to_addr:
                self.log_error("Missing required addresses in transfer event",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              transformer_name=self.name)
                raise ValueError("Both from and to addresses are required for transfer events")
            
            self.log_debug("Transfer attributes extracted",
                          log_index=log.index,
                          from_addr=from_addr,
                          to_addr=to_addr,
                          value=value,
                          sender=sender,
                          transformer_name=self.name)
            
            return from_addr, to_addr, value, sender
            
        except Exception as e:
            self.log_error("Exception in transfer attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise
    
    def _get_liquidity_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        """Extract liquidity attributes with validation"""
        try:
            base_amount, quote_amount = self._get_amounts(log)
            
            sender = str(log.attributes.get("sender", ""))
            to = str(log.attributes.get("to", ""))
            
            self.log_debug("Liquidity attributes extracted",
                          log_index=log.index,
                          base_amount=base_amount,
                          quote_amount=quote_amount,
                          sender=sender,
                          to=to,
                          transformer_name=self.name)
            
            return base_amount, quote_amount, sender, to
            
        except Exception as e:
            self.log_error("Exception in liquidity attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise
    
    def _validate_swap_data(self, log: DecodedLog, swap: Tuple[str, str, str, str],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate swap data with comprehensive checks"""
        try:
            base_amount, quote_amount, to, sender = swap
            
            # Check for None values
            if any(v is None for v in [base_amount, quote_amount, to]):
                self.log_error("Swap has null required attributes",
                              log_index=log.index,
                              base_amount_null=base_amount is None,
                              quote_amount_null=quote_amount is None,
                              to_null=to is None,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Check amounts are not zero
            if self._is_zero_amount(base_amount) or self._is_zero_amount(quote_amount):
                self.log_error("Swap amounts are zero - invalid swap",
                              log_index=log.index,
                              base_amount=base_amount,
                              quote_amount=quote_amount,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate addresses
            addresses_to_check = [to]
            if sender:
                addresses_to_check.append(sender)
                
            if not self._validate_addresses(*addresses_to_check):
                self.log_error("Invalid addresses in swap event",
                              log_index=log.index,
                              to=to,
                              sender=sender,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            self.log_debug("Swap data validation passed",
                          log_index=log.index,
                          transformer_name=self.name)
            return True
            
        except Exception as e:
            self.log_error("Exception during swap validation",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_attr_error(log.index, errors)
            return False

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate transfer data with comprehensive checks"""
        try:
            from_addr, to_addr, value, sender = trf
            
            # Check for None values
            if any(v is None for v in [from_addr, to_addr, value]):
                self.log_error("Transfer has null required attributes",
                              log_index=log.index,
                              from_addr_null=from_addr is None,
                              to_addr_null=to_addr is None,
                              value_null=value is None,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Check amount is not zero
            if self._is_zero_amount(value):
                self.log_error("Transfer amount is zero - invalid transfer",
                              log_index=log.index,
                              value=value,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate addresses
            addresses_to_check = [from_addr, to_addr]
            if sender:
                addresses_to_check.append(sender)
                
            if not self._validate_addresses(*addresses_to_check):
                self.log_error("Invalid addresses in transfer event",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              sender=sender,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            self.log_debug("Transfer data validation passed",
                          log_index=log.index,
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

    def _validate_liquidity_data(self, log: DecodedLog, liq: Tuple[str, str, str, str],
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate liquidity data with comprehensive checks"""
        try:
            base_amount, quote_amount, sender, to = liq
            
            # Check for None values
            if any(v is None for v in [base_amount, quote_amount]):
                self.log_error("Liquidity has null required amounts",
                              log_index=log.index,
                              base_amount_null=base_amount is None,
                              quote_amount_null=quote_amount is None,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Check that not both amounts are zero
            if self._is_zero_amount(base_amount) and self._is_zero_amount(quote_amount):
                self.log_error("Both liquidity amounts are zero - invalid liquidity operation",
                              log_index=log.index,
                              base_amount=base_amount,
                              quote_amount=quote_amount,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate addresses if provided
            addresses_to_check = []
            if sender:
                addresses_to_check.append(sender)
            if to:
                addresses_to_check.append(to)
                
            if addresses_to_check and not self._validate_addresses(*addresses_to_check):
                self.log_error("Invalid addresses in liquidity event",
                              log_index=log.index,
                              sender=sender,
                              to=to,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            self.log_debug("Liquidity data validation passed",
                          log_index=log.index,
                          transformer_name=self.name)
            return True
            
        except Exception as e:
            self.log_error("Exception during liquidity validation",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_attr_error(log.index, errors)
            return False

    def _handle_swap(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle swap events with comprehensive error handling"""
        self.log_debug("Handling pool swap log",
                      log_index=log.index,
                      log_name=log.name,
                      pool=self.contract_address,
                      transformer_name=self.name)
        
        try:
            swap = self._get_swap_attributes(log)
            if not self._validate_swap_data(log, swap, errors):
                self.log_warning("Swap validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            base_amount, quote_amount, to, sender = swap
            
            signal = SwapSignal(
                log_index=log.index,
                pattern="Swap_A",
                pool=self.contract_address,
                base_amount=str(base_amount),
                base_token=self.base_token,
                quote_amount=str(quote_amount),
                quote_token=self.quote_token,
                to=EvmAddress(to.lower()),
                sender=EvmAddress(sender.lower()) if sender else None,
            )
            
            signals[log.index] = signal
            
            self.log_info("Pool swap signal created successfully",
                         log_index=log.index,
                         pool=self.contract_address,
                         base_amount=base_amount,
                         quote_amount=quote_amount,
                         to=to,
                         has_sender=bool(sender),
                         transformer_name=self.name)
            
        except Exception as e:
            self.log_error("Exception while handling swap log",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            
            error = self._create_transform_error(
                error_type="swap_handler_exception",
                message=f"Exception in swap handler: {str(e)}",
                log_index=log.index
            )
            errors[error.error_id] = error

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle transfer events with comprehensive error handling"""
        self.log_debug("Handling pool transfer log",
                      log_index=log.index,
                      log_name=log.name,
                      pool=self.contract_address,
                      transformer_name=self.name)

        try:
            trf = self._get_transfer_attributes(log)
            if not self._validate_transfer_data(log, trf, errors):
                self.log_warning("Transfer validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            from_addr, to_addr, value, sender = trf
            
            signal = TransferSignal(
                log_index=log.index,
                pattern="Transfer",
                token=self.contract_address,
                from_address=EvmAddress(from_addr.lower()),
                to_address=EvmAddress(to_addr.lower()),
                amount=str(value),
                sender=EvmAddress(sender.lower()) if sender else None
            )
            
            signals[log.index] = signal
            
            self.log_info("Pool transfer signal created successfully",
                         log_index=log.index,
                         pool=self.contract_address,
                         from_address=from_addr,
                         to_address=to_addr,
                         amount=value,
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

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle mint events with comprehensive error handling"""
        self.log_debug("Handling pool mint log",
                      log_index=log.index,
                      log_name=log.name,
                      pool=self.contract_address,
                      transformer_name=self.name)

        try:
            liq = self._get_liquidity_attributes(log)
            if not self._validate_liquidity_data(log, liq, errors):
                self.log_warning("Liquidity validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            base_amount, quote_amount, sender, to = liq
            
            signal = LiquiditySignal(
                log_index=log.index,
                pattern="Mint_A",
                pool=self.contract_address,
                base_amount=str(base_amount),
                base_token=self.base_token,
                quote_amount=str(quote_amount),
                quote_token=self.quote_token,
                action="add",
                sender=EvmAddress(sender.lower()) if sender else None,
                owner=EvmAddress(to.lower()) if to else None
            )
            
            signals[log.index] = signal
            
            self.log_info("Pool mint signal created successfully",
                         log_index=log.index,
                         pool=self.contract_address,
                         base_amount=base_amount,
                         quote_amount=quote_amount,
                         has_sender=bool(sender),
                         has_owner=bool(to),
                         transformer_name=self.name)
            
        except Exception as e:
            self.log_error("Exception while handling mint log",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            
            error = self._create_transform_error(
                error_type="mint_handler_exception",
                message=f"Exception in mint handler: {str(e)}",
                log_index=log.index
            )
            errors[error.error_id] = error

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle burn events with comprehensive error handling"""
        self.log_debug("Handling pool burn log",
                      log_index=log.index,
                      log_name=log.name,
                      pool=self.contract_address,
                      transformer_name=self.name)

        try:
            liq = self._get_liquidity_attributes(log)
            if not self._validate_liquidity_data(log, liq, errors):
                self.log_warning("Liquidity validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            base_amount, quote_amount, sender, to = liq
            
            # Convert to string and make negative for burn operations
            base_amount_str = str(base_amount)
            quote_amount_str = str(quote_amount)
            
            negative_base = f"-{base_amount_str}" if not base_amount_str.startswith('-') else base_amount_str
            negative_quote = f"-{quote_amount_str}" if not quote_amount_str.startswith('-') else quote_amount_str
            
            signal = LiquiditySignal(
                log_index=log.index,
                pattern="Burn_A",
                pool=self.contract_address,
                base_amount=negative_base,
                base_token=self.base_token,
                quote_amount=negative_quote,
                quote_token=self.quote_token,
                action="remove",
                sender=EvmAddress(sender.lower()) if sender else None,
                owner=EvmAddress(to.lower()) if to else None
            )
            
            signals[log.index] = signal
            
            self.log_info("Pool burn signal created successfully",
                         log_index=log.index,
                         pool=self.contract_address,
                         base_amount=negative_base,
                         quote_amount=negative_quote,
                         has_sender=bool(sender),
                         has_owner=bool(to),
                         transformer_name=self.name)
            
        except Exception as e:
            self.log_error("Exception while handling burn log",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            
            error = self._create_transform_error(
                error_type="burn_handler_exception",
                message=f"Exception in burn handler: {str(e)}",
                log_index=log.index
            )
            errors[error.error_id] = error

    def _is_zero_amount(self, amount: Any) -> bool:
        """Check if amount is zero (handles both int and string)"""
        if amount is None:
            return True
        
        if isinstance(amount, (int, float)):
            return amount == 0
        
        if isinstance(amount, str):
            return amount == "" or amount == "0"
        
        # Unknown type, log warning and treat as zero
        self.log_warning("Unknown amount type encountered",
                        amount=amount,
                        amount_type=type(amount).__name__,
                        transformer_name=self.name)
        return True