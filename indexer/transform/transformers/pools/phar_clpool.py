# indexer/transform/transformers/pools/phar_clpool.py

from typing import Dict, Tuple

from .pool_base import PoolTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
    ProcessingError,
    Signal,
    SwapSignal,
    LiquiditySignal,
    CollectSignal,
    ErrorId,
)
from ....utils.amounts import amount_to_str, is_zero


class PharClPoolTransformer(PoolTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, 
                 base_token: EvmAddress):
        super().__init__(contract, token0, token1, base_token)
        self.handler_map = {
            "Swap": self._handle_swap,
            "Mint": self._handle_mint,
            "Burn": self._handle_burn,
            "Collect": self._handle_collect,
            "CollectProtocol": self._handle_collect
        }
        
        self.log_info(
            "PharClPoolTransformer initialized",
            contract=contract,
            token0=token0,
            token1=token1,
            base_token=base_token,
            handler_count=len(self.handler_map)
        )

    def _get_cl_swap_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        """Extract CL swap attributes with error handling"""
        try:
            self.log_debug(
                "Extracting CL swap attributes",
                log_index=log.index,
                log_name=log.name,
                available_attributes=list(log.attributes.keys())
            )
            
            base_amount, quote_amount = self._get_amounts(log)
            recipient = str(log.attributes.get("recipient", ""))
            sender = str(log.attributes.get("sender", ""))
            
            self.log_debug(
                "CL swap attributes extracted successfully",
                log_index=log.index,
                base_amount=base_amount,
                quote_amount=quote_amount,
                recipient=recipient[:10] + "..." if recipient else "None",
                sender=sender[:10] + "..." if sender else "None"
            )
            
            return base_amount, quote_amount, recipient, sender
            
        except Exception as e:
            self.log_error(
                "Failed to extract CL swap attributes",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                available_attributes=list(log.attributes.keys()) if hasattr(log, 'attributes') else None
            )
            raise

    def _get_cl_liquidity_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str]:
        """Extract CL liquidity attributes with error handling"""
        try:
            self.log_debug(
                "Extracting CL liquidity attributes",
                log_index=log.index,
                log_name=log.name,
                available_attributes=list(log.attributes.keys())
            )
            
            base_amount, quote_amount = self._get_amounts(log)
            owner = str(log.attributes.get("owner", ""))
            sender = str(log.attributes.get("sender", ""))
            receipt_amount = amount_to_str(log.attributes.get("amount", 0))
            
            self.log_debug(
                "CL liquidity attributes extracted successfully",
                log_index=log.index,
                base_amount=base_amount,
                quote_amount=quote_amount,
                receipt_amount=receipt_amount,
                owner=owner[:10] + "..." if owner else "None",
                sender=sender[:10] + "..." if sender else "None"
            )
            
            return base_amount, quote_amount, owner, sender, receipt_amount
            
        except Exception as e:
            self.log_error(
                "Failed to extract CL liquidity attributes",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                available_attributes=list(log.attributes.keys()) if hasattr(log, 'attributes') else None
            )
            raise

    def _get_cl_collect_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str]:
        """Extract CL collect attributes with error handling"""
        try:
            self.log_debug(
                "Extracting CL collect attributes",
                log_index=log.index,
                log_name=log.name,
                available_attributes=list(log.attributes.keys())
            )
            
            base_amount, quote_amount = self._get_amounts(log)
            recipient = str(log.attributes.get("recipient", ""))
            owner = str(log.attributes.get("owner", ""))
            sender = str(log.attributes.get("sender", ""))
            
            self.log_debug(
                "CL collect attributes extracted successfully",
                log_index=log.index,
                base_amount=base_amount,
                quote_amount=quote_amount,
                recipient=recipient[:10] + "..." if recipient else "None",
                owner=owner[:10] + "..." if owner else "None",
                sender=sender[:10] + "..." if sender else "None"
            )
            
            return base_amount, quote_amount, recipient, owner, sender
            
        except Exception as e:
            self.log_error(
                "Failed to extract CL collect attributes",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                available_attributes=list(log.attributes.keys()) if hasattr(log, 'attributes') else None
            )
            raise

    def _validate_cl_liquidity_data(self, log: DecodedLog, liq: Tuple[str, str, str, str, str],
                                   errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate CL liquidity data with enhanced error reporting"""
        try:
            self.log_debug(
                "Validating CL liquidity data",
                log_index=log.index,
                base_amount=liq[0],
                quote_amount=liq[1],
                receipt_amount=liq[4]
            )
            
            if not self._validate_null_attr(liq[:4], log.index, errors): 
                self.log_warning(
                    "CL liquidity validation failed - null attributes",
                    log_index=log.index,
                    liquidity_data=liq[:4]
                )
                return False
            
            if is_zero(liq[0]) and is_zero(liq[1]):
                if not is_zero(liq[4]):    
                    self.log_warning(
                        "CL liquidity validation failed - both amounts zero but receipt non-zero",
                        log_index=log.index,
                        base_amount=liq[0],
                        quote_amount=liq[1],
                        receipt_amount=liq[4]
                    )
                    self._create_attr_error(log.index, errors)
                    return False
            
            if liq[2] and not self._validate_addresses(liq[2]):
                self.log_warning(
                    "CL liquidity validation failed - invalid owner address",
                    log_index=log.index,
                    owner_address=liq[2]
                )
                self._create_attr_error(log.index, errors)
                return False
                
            if liq[3] and not self._validate_addresses(liq[3]):
                self.log_warning(
                    "CL liquidity validation failed - invalid sender address",
                    log_index=log.index,
                    sender_address=liq[3]
                )
                self._create_attr_error(log.index, errors)
                return False

            self.log_debug(
                "CL liquidity data validation passed",
                log_index=log.index
            )
            return True
            
        except Exception as e:
            self.log_error(
                "Exception during CL liquidity validation",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                liquidity_data=liq
            )
            self._create_log_exception(e, log.index, errors)
            return False

    def _validate_collect_data(self, log: DecodedLog, collect: Tuple[str, str, str, str, str],
                              errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate collect data with enhanced error reporting"""
        try:
            self.log_debug(
                "Validating CL collect data",
                log_index=log.index,
                base_amount=collect[0],
                quote_amount=collect[1],
                recipient=collect[2][:10] + "..." if collect[2] else "None"
            )
            
            if not self._validate_null_attr(collect[:3], log.index, errors):
                self.log_warning(
                    "CL collect validation failed - null attributes",
                    log_index=log.index,
                    collect_data=collect[:3]
                )
                return False
            
            if collect[2] and not self._validate_addresses(collect[2]):
                self.log_warning(
                    "CL collect validation failed - invalid recipient address",
                    log_index=log.index,
                    recipient_address=collect[2]
                )
                self._create_attr_error(log.index, errors)
                return False

            self.log_debug(
                "CL collect data validation passed",
                log_index=log.index
            )
            return True
            
        except Exception as e:
            self.log_error(
                "Exception during CL collect validation",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                collect_data=collect
            )
            self._create_log_exception(e, log.index, errors)
            return False

    def _handle_swap(self, log: DecodedLog, signals: Dict[int, Signal], 
                    errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle CL swap log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing CL swap log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )
            
            swap = self._get_cl_swap_attributes(log)
            if not self._validate_swap_data(log, swap, errors):
                self.log_warning(
                    "CL swap validation failed, skipping signal creation",
                    log_index=log.index,
                    swap_data=swap
                )
                return
            
            signal = SwapSignal(
                log_index=log.index,
                pattern="Swap_A",
                pool=self.contract_address,
                base_amount=swap[0],
                base_token=self.base_token,
                quote_amount=swap[1], 
                quote_token=self.quote_token,
                to=EvmAddress(swap[2].lower()),
                sender=EvmAddress(swap[3].lower()) if swap[3] else None
            )
            
            signals[log.index] = signal
            
            self.log_info(
                "CL swap signal created successfully",
                log_index=log.index,
                pool=self.contract_address,
                base_amount=swap[0],
                quote_amount=swap[1],
                taker=swap[2][:10] + "..." if swap[2] else "None"
            )
            
        except Exception as e:
            self.log_error(
                "Failed to handle CL swap log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal],
                    errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle CL mint log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing CL mint log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )
            
            liq = self._get_cl_liquidity_attributes(log)
            if not self._validate_cl_liquidity_data(log, liq, errors):
                self.log_warning(
                    "CL mint validation failed, skipping signal creation",
                    log_index=log.index,
                    liquidity_data=liq
                )
                return
            
            signal = LiquiditySignal(
                log_index=log.index,
                pattern="Mint_A",
                pool=self.contract_address,
                base_amount=liq[0],
                base_token=self.base_token,
                quote_amount=liq[1],
                quote_token=self.quote_token,
                action="add",
                receipt_amount=liq[4],  # liquidity amount
                sender=EvmAddress(liq[3].lower()) if liq[3] else None,
                owner=EvmAddress(liq[2].lower()) if liq[2] else None
            )
            
            signals[log.index] = signal
            
            self.log_info(
                "CL mint signal created successfully",
                log_index=log.index,
                pool=self.contract_address,
                base_amount=liq[0],
                quote_amount=liq[1],
                receipt_amount=liq[4],
                owner=liq[2][:10] + "..." if liq[2] else "None"
            )
            
        except Exception as e:
            self.log_error(
                "Failed to handle CL mint log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal],
                    errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle CL burn log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing CL burn log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )
            
            liq = self._get_cl_liquidity_attributes(log)
            if not self._validate_cl_liquidity_data(log, liq, errors):
                self.log_warning(
                    "CL burn validation failed, skipping signal creation",
                    log_index=log.index,
                    liquidity_data=liq
                )
                return
            
            # Ensure negative amounts for burn operation
            base_amount = f"-{liq[0]}" if not liq[0].startswith('-') else liq[0]
            quote_amount = f"-{liq[1]}" if not liq[1].startswith('-') else liq[1]
            receipt_amount = f"-{liq[4]}" if not liq[4].startswith('-') else liq[4]
            
            signal = LiquiditySignal(
                log_index=log.index,
                pattern="Burn_A",
                pool=self.contract_address,
                base_amount=base_amount,
                base_token=self.base_token,
                quote_amount=quote_amount,
                quote_token=self.quote_token,
                action="remove",
                receipt_amount=receipt_amount,
                sender=EvmAddress(liq[3].lower()) if liq[3] else None,
                owner=EvmAddress(liq[2].lower()) if liq[2] else None
            )
            
            signals[log.index] = signal
            
            self.log_info(
                "CL burn signal created successfully",
                log_index=log.index,
                pool=self.contract_address,
                base_amount=base_amount,
                quote_amount=quote_amount,
                receipt_amount=receipt_amount,
                owner=liq[2][:10] + "..." if liq[2] else "None"
            )
            
        except Exception as e:
            self.log_error(
                "Failed to handle CL burn log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)

    def _handle_collect(self, log: DecodedLog, signals: Dict[int, Signal],
                       errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle CL collect log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing CL collect log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )
            
            collect = self._get_cl_collect_attributes(log)
            if not self._validate_collect_data(log, collect, errors):
                self.log_warning(
                    "CL collect validation failed, skipping signal creation",
                    log_index=log.index,
                    collect_data=collect
                )
                return
            
            # Only create signal if amounts are non-zero
            if not (is_zero(collect[0]) and is_zero(collect[1])):
                signal = CollectSignal(
                    log_index=log.index,
                    pattern="Info",
                    contract=self.contract_address,
                    recipient=EvmAddress(collect[2].lower()),
                    base_amount=collect[0],
                    base_token=self.base_token,
                    quote_amount=collect[1],
                    quote_token=self.quote_token,
                    owner=EvmAddress(collect[3].lower()) if collect[3] else None,
                    sender=EvmAddress(collect[4].lower()) if collect[4] else None,
                )
                
                signals[log.index] = signal
                
                self.log_info(
                    "CL collect signal created successfully",
                    log_index=log.index,
                    contract=self.contract_address,
                    base_amount=collect[0],
                    quote_amount=collect[1],
                    recipient=collect[2][:10] + "..." if collect[2] else "None"
                )
            else:
                self.log_debug(
                    "CL collect amounts are zero, skipping signal creation",
                    log_index=log.index,
                    base_amount=collect[0],
                    quote_amount=collect[1]
                )
            
        except Exception as e:
            self.log_error(
                "Failed to handle CL collect log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)