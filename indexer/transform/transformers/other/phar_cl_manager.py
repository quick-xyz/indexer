# indexer/transform/transformers/other/phar_cl_manager.py

from typing import Dict, Tuple

from ..base import BaseTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
    ProcessingError,
    Signal,
    NfpCollectSignal,
    NfpLiquiditySignal,
    TransferSignal,
    ErrorId,
)
from ....utils.amounts import amount_to_str, is_zero


class PharNfpTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress):
        if not contract:
            raise ValueError("Contract address is required for PharNfpTransformer")
            
        super().__init__(contract_address=contract)
        
        self.handler_map = {
            "Collect": self._handle_collect,
            "IncreaseLiquidity": self._handle_mint,
            "DecreaseLiquidity": self._handle_burn,
            "Transfer": self._handle_transfer
        }
        
        self.log_info("PharNfpTransformer initialized",
                     contract_address=self.contract_address,
                     handler_count=len(self.handler_map),
                     supported_events=list(self.handler_map.keys()),
                     transformer_type="Pharaoh_NFP_Manager")
    
    def _get_collect_attributes(self, log: DecodedLog) -> Tuple[int, str, str, str]:
        """Extract collect attributes from NFP collect event"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for collect extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for collect extraction")
            
            # Extract and validate token ID
            try:
                token_id = int(log.attributes.get("tokenId", 0))
                if token_id <= 0:
                    self.log_error("Invalid or missing tokenId in collect event",
                                  log_index=log.index,
                                  token_id=token_id,
                                  transformer_name=self.name)
                    raise ValueError("Valid tokenId is required for collect events")
            except (ValueError, TypeError) as e:
                self.log_error("Failed to parse tokenId in collect event",
                              log_index=log.index,
                              token_id_raw=log.attributes.get("tokenId"),
                              error=str(e),
                              transformer_name=self.name)
                raise
            
            recipient = str(log.attributes.get("recipient", ""))
            
            # Extract amounts with error handling
            try:
                amount0 = amount_to_str(log.attributes.get("amount0", 0))
                amount1 = amount_to_str(log.attributes.get("amount1", 0))
            except Exception as e:
                self.log_error("Failed to extract amounts from collect event",
                              log_index=log.index,
                              error=str(e),
                              amount0_raw=log.attributes.get("amount0"),
                              amount1_raw=log.attributes.get("amount1"),
                              transformer_name=self.name)
                raise
            
            self.log_debug("Collect attributes extracted successfully",
                          log_index=log.index,
                          token_id=token_id,
                          recipient=recipient,
                          amount0=amount0,
                          amount1=amount1,
                          transformer_name=self.name)
            
            return token_id, recipient, amount0, amount1
            
        except Exception as e:
            self.log_error("Exception in collect attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise
    
    def _get_liquidity_attributes(self, log: DecodedLog) -> Tuple[int, str, str, str]:
        """Extract liquidity attributes from NFP liquidity events"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for liquidity extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for liquidity extraction")
            
            # Extract and validate token ID
            try:
                token_id = int(log.attributes.get("tokenId", 0))
                if token_id <= 0:
                    self.log_error("Invalid or missing tokenId in liquidity event",
                                  log_index=log.index,
                                  token_id=token_id,
                                  transformer_name=self.name)
                    raise ValueError("Valid tokenId is required for liquidity events")
            except (ValueError, TypeError) as e:
                self.log_error("Failed to parse tokenId in liquidity event",
                              log_index=log.index,
                              token_id_raw=log.attributes.get("tokenId"),
                              error=str(e),
                              transformer_name=self.name)
                raise
            
            # Extract amounts with error handling
            try:
                liquidity = amount_to_str(log.attributes.get("liquidity", 0))
                amount0 = amount_to_str(log.attributes.get("amount0", 0))
                amount1 = amount_to_str(log.attributes.get("amount1", 0))
            except Exception as e:
                self.log_error("Failed to extract amounts from liquidity event",
                              log_index=log.index,
                              error=str(e),
                              liquidity_raw=log.attributes.get("liquidity"),
                              amount0_raw=log.attributes.get("amount0"),
                              amount1_raw=log.attributes.get("amount1"),
                              transformer_name=self.name)
                raise
            
            self.log_debug("Liquidity attributes extracted successfully",
                          log_index=log.index,
                          token_id=token_id,
                          liquidity=liquidity,
                          amount0=amount0,
                          amount1=amount1,
                          transformer_name=self.name)
            
            return token_id, liquidity, amount0, amount1
            
        except Exception as e:
            self.log_error("Exception in liquidity attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise
    
    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[int, str, str]:
        """Extract transfer attributes from NFP transfer events"""
        try:
            if not log.attributes:
                self.log_error("Log has no attributes for transfer extraction",
                              log_index=log.index,
                              log_name=log.name,
                              transformer_name=self.name)
                raise ValueError("Log attributes required for transfer extraction")
            
            # Extract and validate token ID
            try:
                token_id = int(log.attributes.get("tokenId", 0))
                if token_id <= 0:
                    self.log_error("Invalid or missing tokenId in transfer event",
                                  log_index=log.index,
                                  token_id=token_id,
                                  transformer_name=self.name)
                    raise ValueError("Valid tokenId is required for transfer events")
            except (ValueError, TypeError) as e:
                self.log_error("Failed to parse tokenId in transfer event",
                              log_index=log.index,
                              token_id_raw=log.attributes.get("tokenId"),
                              error=str(e),
                              transformer_name=self.name)
                raise
            
            from_addr = str(log.attributes.get("from", ""))
            to_addr = str(log.attributes.get("to", ""))
            
            # Validate addresses are present
            if not from_addr or not to_addr:
                self.log_error("Missing required addresses in transfer event",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              transformer_name=self.name)
                raise ValueError("Both from and to addresses are required for transfer events")
            
            self.log_debug("Transfer attributes extracted successfully",
                          log_index=log.index,
                          token_id=token_id,
                          from_addr=from_addr,
                          to_addr=to_addr,
                          transformer_name=self.name)
            
            return token_id, from_addr, to_addr
            
        except Exception as e:
            self.log_error("Exception in transfer attribute extraction",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            raise
    
    def _validate_collect_data(self, log: DecodedLog, collect: Tuple[int, str, str, str], errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate collect event data with comprehensive checks"""
        try:
            token_id, recipient, amount0, amount1 = collect
            
            # Basic null validation
            if not self._validate_null_attr([token_id, recipient, amount0, amount1], log.index, errors):
                return False
            
            # Validate token ID is positive
            if token_id <= 0:
                self.log_error("Invalid tokenId in collect validation",
                              log_index=log.index,
                              token_id=token_id,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate recipient address if provided
            if recipient and not self._validate_addresses(recipient):
                self.log_error("Invalid recipient address in collect event",
                              log_index=log.index,
                              recipient=recipient,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Check if both amounts are zero (might be valid for some cases)
            if is_zero(amount0) and is_zero(amount1):
                self.log_warning("Both collect amounts are zero - may be valid",
                                log_index=log.index,
                                token_id=token_id,
                                transformer_name=self.name)
            
            self.log_debug("Collect data validation passed",
                          log_index=log.index,
                          token_id=token_id,
                          transformer_name=self.name)
            return True
            
        except Exception as e:
            self.log_error("Exception during collect validation",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            self._create_attr_error(log.index, errors)
            return False

    def _validate_liquidity_data(self, log: DecodedLog, liq: Tuple[int, str, str, str], errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate liquidity event data with comprehensive checks"""
        try:
            token_id, liquidity, amount0, amount1 = liq
            
            # Basic null validation
            if not self._validate_null_attr([token_id, liquidity, amount0, amount1], log.index, errors):
                return False
            
            # Validate token ID is positive
            if token_id <= 0:
                self.log_error("Invalid tokenId in liquidity validation",
                              log_index=log.index,
                              token_id=token_id,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False

            # Validate that not all amounts are zero
            if is_zero(liquidity) and is_zero(amount0) and is_zero(amount1):
                self.log_error("All liquidity amounts are zero - invalid state",
                              log_index=log.index,
                              token_id=token_id,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Warn if liquidity delta is zero but amounts are not
            if is_zero(liquidity) and (not is_zero(amount0) or not is_zero(amount1)):
                self.log_warning("Liquidity delta is zero but token amounts are not",
                                log_index=log.index,
                                token_id=token_id,
                                amount0=amount0,
                                amount1=amount1,
                                transformer_name=self.name)
            
            self.log_debug("Liquidity data validation passed",
                          log_index=log.index,
                          token_id=token_id,
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

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[int, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate transfer event data with comprehensive checks"""
        try:
            token_id, from_addr, to_addr = trf
            
            # Basic null validation
            if not self._validate_null_attr([token_id, from_addr, to_addr], log.index, errors):
                return False
            
            # Validate token ID is positive
            if token_id <= 0:
                self.log_error("Invalid tokenId in transfer validation",
                              log_index=log.index,
                              token_id=token_id,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            # Validate addresses
            if not self._validate_addresses(from_addr, to_addr):
                self.log_error("Invalid addresses in transfer event",
                              log_index=log.index,
                              from_addr=from_addr,
                              to_addr=to_addr,
                              transformer_name=self.name)
                self._create_attr_error(log.index, errors)
                return False
            
            self.log_debug("Transfer data validation passed",
                          log_index=log.index,
                          token_id=token_id,
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
    
    def _handle_collect(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle NFP collect events with comprehensive error handling"""
        self.log_debug("Handling NFP Collect log",
                      log_index=log.index,
                      log_name=log.name,
                      transformer_name=self.name)

        try:
            collect = self._get_collect_attributes(log)
            if not self._validate_collect_data(log, collect, errors):
                self.log_warning("Collect data validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            token_id, recipient, amount0, amount1 = collect
            
            signal = NfpCollectSignal(
                log_index=log.index,
                pattern="Info",
                contract=self.contract_address,
                token_id=token_id,
                recipient=EvmAddress(recipient.lower()),
                amount0=amount0,
                amount1=amount1
            )
            
            signals[log.index] = signal
            
            self.log_info("NFP Collect signal created successfully",
                         log_index=log.index,
                         token_id=token_id,
                         recipient=recipient,
                         amount0=amount0,
                         amount1=amount1,
                         transformer_name=self.name)
            
        except Exception as e:
            self.log_error("Exception while handling collect log",
                          log_index=log.index,
                          error=str(e),
                          exception_type=type(e).__name__,
                          transformer_name=self.name)
            
            error = self._create_transform_error(
                error_type="collect_handler_exception",
                message=f"Exception in collect handler: {str(e)}",
                log_index=log.index
            )
            errors[error.error_id] = error

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle NFP mint (increase liquidity) events with comprehensive error handling"""
        self.log_debug("Handling NFP Mint log",
                      log_index=log.index,
                      log_name=log.name,
                      transformer_name=self.name)

        try:
            liq = self._get_liquidity_attributes(log)
            if not self._validate_liquidity_data(log, liq, errors):
                self.log_warning("Liquidity data validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            token_id, liquidity, amount0, amount1 = liq
            
            signal = NfpLiquiditySignal(
                log_index=log.index,
                pattern="Info",
                contract=self.contract_address,
                token_id=token_id,
                liquidity=liquidity,
                amount0=amount0,
                amount1=amount1,
                action="add",
            )
            
            signals[log.index] = signal
            
            self.log_info("NFP Mint signal created successfully",
                         log_index=log.index,
                         token_id=token_id,
                         liquidity=liquidity,
                         amount0=amount0,
                         amount1=amount1,
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
        """Handle NFP burn (decrease liquidity) events with comprehensive error handling"""
        self.log_debug("Handling NFP Burn log",
                      log_index=log.index,
                      log_name=log.name,
                      transformer_name=self.name)

        try:
            liq = self._get_liquidity_attributes(log)
            if not self._validate_liquidity_data(log, liq, errors):
                self.log_warning("Liquidity data validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            token_id, liquidity, amount0, amount1 = liq
            
            # Make amounts negative for burn operations
            negative_liquidity = f"-{liquidity}" if not liquidity.startswith('-') else liquidity
            negative_amount0 = f"-{amount0}" if not amount0.startswith('-') else amount0
            negative_amount1 = f"-{amount1}" if not amount1.startswith('-') else amount1
            
            signal = NfpLiquiditySignal(
                log_index=log.index,
                pattern="Info",
                contract=self.contract_address,
                token_id=token_id,
                liquidity=negative_liquidity,
                amount0=negative_amount0,
                amount1=negative_amount1,
                action="remove",
            )
            
            signals[log.index] = signal
            
            self.log_info("NFP Burn signal created successfully",
                         log_index=log.index,
                         token_id=token_id,
                         liquidity=negative_liquidity,
                         amount0=negative_amount0,
                         amount1=negative_amount1,
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

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle NFP transfer events with comprehensive error handling"""
        self.log_debug("Handling NFP transfer log",
                      log_index=log.index,
                      log_name=log.name,
                      transformer_name=self.name)

        try:
            trf = self._get_transfer_attributes(log)
            if not self._validate_transfer_data(log, trf, errors):
                self.log_warning("Transfer data validation failed - skipping signal creation",
                                log_index=log.index,
                                transformer_name=self.name)
                return
            
            token_id, from_addr, to_addr = trf
            
            signal = TransferSignal(
                log_index=log.index,
                pattern="Transfer",
                token=self.contract_address,
                from_address=EvmAddress(from_addr.lower()),
                to_address=EvmAddress(to_addr.lower()),
                amount=amount_to_str(1),  # NFTs have amount of 1
                token_id=token_id
            )
            
            signals[log.index] = signal
            
            self.log_info("NFP Transfer signal created successfully",
                         log_index=log.index,
                         token_id=token_id,
                         from_address=from_addr,
                         to_address=to_addr,
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