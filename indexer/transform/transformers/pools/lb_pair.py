# indexer/transform/transformers/pools/lb_pair.py

from typing import Dict, Tuple, Optional

from .pool_base import PoolTransformer
from ....types import (
    DecodedLog,
    EvmAddress,
    ProcessingError,
    Signal,
    SwapBatchSignal,
    TransferSignal,
    LiquiditySignal,
    ErrorId,
)
from ....utils.amounts import amount_to_str, is_zero, add_amounts


class LbPairTransformer(PoolTransformer):
    def __init__(self, contract: EvmAddress, token_x: EvmAddress, token_y: EvmAddress, base_token: EvmAddress):
        super().__init__(contract, token_x, token_y, base_token, fee_collector=None)
        self.handler_map = {
            "Swap": self._handle_swap,
            "DepositedToBins": self._handle_mint,
            "WithdrawnFromBins": self._handle_burn,
            "TransferBatch": self._handle_transfer
        }
        
        self.log_info(
            "LbPairTransformer initialized",
            contract=contract,
            token_x=token_x,
            token_y=token_y,
            base_token=base_token,
            handler_count=len(self.handler_map)
        )

    @property
    def token_x(self):
        return self.token0
    
    @property 
    def token_y(self):
        return self.token1

    def _unpack_amounts(self, amounts: str) -> Tuple[str, str]:
        """Unpack LB pair amounts with comprehensive error handling"""
        try:
            self.log_debug(
                "Starting amount unpacking",
                amounts_type=type(amounts).__name__,
                amounts_value=str(amounts)[:100],  # Truncate for readability
                amounts_startswith_0x=amounts.startswith("0x") if isinstance(amounts, str) else False
            )
            
            if amounts.startswith("0x"):
                hex_str = amounts[2:]
            else:
                hex_str = amounts

            packed_amounts = bytes.fromhex(hex_str)

            amounts_x = int.from_bytes(packed_amounts, byteorder="big") & (2 ** 128 - 1)
            amounts_y = int.from_bytes(packed_amounts, byteorder="big") >> 128

            if self.token0 == self.base_token:
                result = amounts_x, amounts_y
            else:
                result = amounts_y, amounts_x
            
            self.log_debug(
                "Amount unpacking successful",
                amounts_x=amounts_x,
                amounts_y=amounts_y,
                base_amount=result[0],
                quote_amount=result[1]
            )
            
            return result
        
        except ValueError as e:
            self.log_error(
                "Amount unpacking failed - invalid hex format",
                amounts_type=type(amounts).__name__,
                amounts_value=str(amounts)[:50],
                error=str(e),
                exception_type=type(e).__name__
            )
            raise
        except Exception as e:
            self.log_error(
                "Amount unpacking failed - unexpected error",
                amounts_type=type(amounts).__name__,
                amounts_value=str(amounts)[:50],
                error=str(e),
                exception_type=type(e).__name__
            )
            raise

    def _get_in_out_amounts(self, log: DecodedLog) -> Tuple[str, str]:
        """Get swap in/out amounts with error handling"""
        try:
            self.log_debug(
                "Extracting swap in/out amounts",
                log_index=log.index,
                available_attributes=list(log.attributes.keys())
            )
            
            amounts_in = log.attributes.get("amountsIn")
            amounts_out = log.attributes.get("amountsOut")
            
            if not amounts_in or not amounts_out:
                self.log_error(
                    "Missing required amounts attributes",
                    log_index=log.index,
                    has_amounts_in=bool(amounts_in),
                    has_amounts_out=bool(amounts_out)
                )
                raise ValueError("Missing amountsIn or amountsOut attributes")
            
            base_amount_in, quote_amount_in = self._unpack_amounts(amounts_in)
            base_amount_out, quote_amount_out = self._unpack_amounts(amounts_out)
            
            base_amount = base_amount_out - base_amount_in
            quote_amount = quote_amount_out - quote_amount_in

            result = amount_to_str(base_amount), amount_to_str(quote_amount)
            
            self.log_debug(
                "Swap amounts calculated successfully",
                log_index=log.index,
                base_amount=result[0],
                quote_amount=result[1],
                base_in=base_amount_in,
                base_out=base_amount_out,
                quote_in=quote_amount_in,
                quote_out=quote_amount_out
            )
            
            return result
            
        except Exception as e:
            self.log_error(
                "Failed to extract swap amounts",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            raise

    def _prepare_bins_and_amounts(self, log: DecodedLog) -> Optional[Tuple[Dict[str, str], str]]:
        """Prepare bins and amounts with comprehensive error handling"""
        try:
            self.log_debug(
                "Preparing bins and amounts",
                log_index=log.index,
                available_attributes=list(log.attributes.keys())
            )
            
            ids = log.attributes.get("ids")
            amounts = log.attributes.get("amounts")
            
            if not ids or not amounts:
                self.log_error(
                    "Missing required ids or amounts attributes",
                    log_index=log.index,
                    has_ids=bool(ids),
                    has_amounts=bool(amounts)
                )
                return None
            
            bins = [str(bin_id) for bin_id in ids]
            amounts_str = [str(amt) for amt in amounts]
            total_sum = add_amounts(amounts_str)

            if len(bins) != len(amounts_str):
                self.log_error(
                    "Bins and amounts length mismatch",
                    log_index=log.index,
                    bins_length=len(bins),
                    amounts_length=len(amounts_str),
                    bins_sample=bins[:5],
                    amounts_sample=amounts_str[:5]
                )
                return None

            bins_amounts = dict(zip(bins, amounts_str))

            self.log_debug(
                "Bins and amounts prepared successfully",
                log_index=log.index,
                bin_count=len(bins),
                total_amount=total_sum,
                first_bin=bins[0] if bins else None
            )

            return bins_amounts, total_sum

        except Exception as e:
            self.log_error(
                "Failed to prepare bins and amounts",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            return None

    def _prepare_bins_and_packed_amounts(self, log: DecodedLog, negative: bool) -> Optional[Tuple[Dict[str, Dict[str, str]], str, str]]:
        """Prepare bins and packed amounts with comprehensive error handling"""
        try:
            self.log_debug(
                "Preparing bins and packed amounts",
                log_index=log.index,
                negative=negative,
                available_attributes=list(log.attributes.keys())
            )
            
            multiplier = -1 if negative else 1

            ids = log.attributes.get("ids")
            amounts_raw = log.attributes.get("amounts", [])
            
            if not ids or not amounts_raw:
                self.log_error(
                    "Missing required ids or amounts attributes",
                    log_index=log.index,
                    has_ids=bool(ids),
                    has_amounts=bool(amounts_raw)
                )
                return None

            bins = [str(bin_id) for bin_id in ids]
            amounts = []

            for i, amts in enumerate(amounts_raw):
                try:
                    base_amount, quote_amount = self._unpack_amounts(amts)
                    amounts.append({
                        "base": str(base_amount * multiplier), 
                        "quote": str(quote_amount * multiplier)
                    })
                except Exception as e:
                    self.log_error(
                        "Failed to unpack amount at index",
                        log_index=log.index,
                        amount_index=i,
                        amount_value=str(amts)[:50],
                        error=str(e)
                    )
                    return None
            
            sum_base = add_amounts([amt["base"] for amt in amounts])
            sum_quote = add_amounts([amt["quote"] for amt in amounts])

            if len(bins) != len(amounts):
                self.log_error(
                    "Bins and packed amounts length mismatch",
                    log_index=log.index,
                    bins_length=len(bins),
                    amounts_length=len(amounts)
                )
                return None

            bins_amounts = dict(zip(bins, amounts))

            self.log_debug(
                "Bins and packed amounts prepared successfully",
                log_index=log.index,
                bin_count=len(bins),
                sum_base=sum_base,
                sum_quote=sum_quote,
                negative=negative
            )

            return bins_amounts, sum_base, sum_quote

        except Exception as e:
            self.log_error(
                "Failed to prepare bins and packed amounts",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                negative=negative
            )
            return None

    def _get_swap_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str]:
        """Extract swap attributes with error handling"""
        try:
            self.log_debug(
                "Extracting swap attributes",
                log_index=log.index,
                available_attributes=list(log.attributes.keys())
            )
            
            id_val = int(log.attributes.get("id", 0))
            base_amount, quote_amount = self._get_in_out_amounts(log)
            sender = str(log.attributes.get("sender", ""))
            to = str(log.attributes.get("to", ""))

            result = id_val, base_amount, quote_amount, to, sender
            
            self.log_debug(
                "Swap attributes extracted successfully",
                log_index=log.index,
                id=id_val,
                base_amount=base_amount,
                quote_amount=quote_amount,
                to=to[:10] + "..." if to else "None",
                sender=sender[:10] + "..." if sender else "None"
            )
            
            return result
            
        except Exception as e:
            self.log_error(
                "Failed to extract swap attributes",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            raise
    
    def _get_batch_transfer_attributes(self, log: DecodedLog) -> Optional[Tuple[str, str, Dict[str, str], str, str]]:
        """Extract batch transfer attributes with error handling"""
        try:
            self.log_debug(
                "Extracting batch transfer attributes",
                log_index=log.index,
                available_attributes=list(log.attributes.keys())
            )
            
            from_addr = str(log.attributes.get("from", ""))
            to_addr = str(log.attributes.get("to", ""))
            sender = str(log.attributes.get("sender", ""))
            
            if not from_addr or not to_addr:
                self.log_error(
                    "Missing required transfer addresses",
                    log_index=log.index,
                    has_from=bool(from_addr),
                    has_to=bool(to_addr)
                )
                return None
            
            result = self._prepare_bins_and_amounts(log)
            if result is None:
                self.log_error(
                    "Failed to prepare bins and amounts for transfer",
                    log_index=log.index
                )
                return None
            
            bins_amounts, total_sum = result

            transfer_result = from_addr, to_addr, bins_amounts, sender, total_sum
            
            self.log_debug(
                "Batch transfer attributes extracted successfully",
                log_index=log.index,
                from_addr=from_addr[:10] + "..." if from_addr else "None",
                to_addr=to_addr[:10] + "..." if to_addr else "None",
                total_amount=total_sum,
                bin_count=len(bins_amounts)
            )
            
            return transfer_result
            
        except Exception as e:
            self.log_error(
                "Failed to extract batch transfer attributes",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            return None
    
    def _get_liquidity_attributes(self, log: DecodedLog, negative: bool = False) -> Optional[Tuple[str, str, str, str, Dict[str, Dict[str, str]]]]:
        """Extract liquidity attributes with error handling"""
        try:
            self.log_debug(
                "Extracting liquidity attributes",
                log_index=log.index,
                negative=negative,
                amounts_attr_type=type(log.attributes.get("amounts")).__name__,
                amounts_length=len(log.attributes.get("amounts", [])),
                ids_type=type(log.attributes.get("ids")).__name__
            )
    
            result = self._prepare_bins_and_packed_amounts(log, negative)
            if result is None:
                self.log_error(
                    "Failed to prepare bins and packed amounts for liquidity",
                    log_index=log.index,
                    negative=negative
                )
                return None
        
            bins_amounts, base_amount, quote_amount = result
            sender = str(log.attributes.get("sender", ""))
            to = str(log.attributes.get("to", ""))

            liquidity_result = base_amount, quote_amount, sender, to, bins_amounts
            
            self.log_debug(
                "Liquidity attributes extracted successfully",
                log_index=log.index,
                negative=negative,
                base_amount=base_amount,
                quote_amount=quote_amount,
                sender=sender[:10] + "..." if sender else "None",
                to=to[:10] + "..." if to else "None",
                bin_count=len(bins_amounts)
            )
            
            return liquidity_result
            
        except Exception as e:
            self.log_error(
                "Failed to extract liquidity attributes",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                negative=negative
            )
            return None
    
    def _validate_swap_data(self, log: DecodedLog, swap: Tuple[str, str, str, str, str],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate swap data with enhanced error reporting"""
        try:
            self.log_debug(
                "Validating swap data",
                log_index=log.index,
                id=swap[0],
                base_amount=swap[1],
                quote_amount=swap[2]
            )
            
            if not self._validate_null_attr(swap, log.index, errors):
                self.log_warning(
                    "Swap validation failed - null attributes",
                    log_index=log.index,
                    swap_data=swap
                )
                return False
                
            if is_zero(swap[0]):
                self.log_warning(
                    "Swap validation failed - bin ID is zero",
                    log_index=log.index,
                    bin_id=swap[0]
                )
                self._create_attr_error(log.index, errors)
                return False
                
            if is_zero(swap[1]) or is_zero(swap[2]):
                self.log_warning(
                    "Swap validation failed - amounts are zero",
                    log_index=log.index,
                    base_amount=swap[1],
                    quote_amount=swap[2]
                )
                self._create_attr_error(log.index, errors)
                return False
                
            if not self._validate_addresses(swap[3], swap[4]):
                self.log_warning(
                    "Swap validation failed - invalid addresses",
                    log_index=log.index,
                    to_address=swap[3],
                    sender_address=swap[4]
                )
                self._create_attr_error(log.index, errors)
                return False
                
            self.log_debug(
                "Swap data validation passed",
                log_index=log.index
            )
            return True
            
        except Exception as e:
            self.log_error(
                "Exception during swap validation",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            self._create_log_exception(e, log.index, errors)
            return False

    def _validate_batch_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, Dict[str, str], str, str], 
                                    errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate batch transfer data with enhanced error reporting"""
        try:
            self.log_debug(
                "Validating batch transfer data",
                log_index=log.index,
                from_addr=trf[0][:10] + "..." if trf[0] else "None",
                to_addr=trf[1][:10] + "..." if trf[1] else "None",
                total_amount=trf[4]
            )
            
            if not self._validate_null_attr(trf, log.index, errors):
                self.log_warning(
                    "Batch transfer validation failed - null attributes",
                    log_index=log.index
                )
                return False
                
            self.log_debug(
                "Batch transfer data validation passed",
                log_index=log.index
            )
            return True
            
        except Exception as e:
            self.log_error(
                "Exception during batch transfer validation",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            self._create_log_exception(e, log.index, errors)
            return False

    def _validate_liquidity_data(self, log: DecodedLog, liq: Tuple[str, str, str, str, Dict[str, Dict[str, str]]],
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        """Validate liquidity data with enhanced error reporting"""
        try:
            self.log_debug(
                "Validating liquidity data",
                log_index=log.index,
                base_amount=liq[0],
                quote_amount=liq[1],
                bin_count=len(liq[4]) if liq[4] else 0
            )
            
            if not self._validate_null_attr(liq[:4], log.index, errors):
                self.log_warning(
                    "Liquidity validation failed - null attributes",
                    log_index=log.index
                )
                return False
        
            if is_zero(liq[0]) and is_zero(liq[1]):
                self.log_warning(
                    "Liquidity validation failed - both amounts are zero",
                    log_index=log.index,
                    base_amount=liq[0],
                    quote_amount=liq[1]
                )
                self._create_attr_error(log.index, errors)
                return False
        
            if liq[2] and not self._validate_addresses(liq[2]):
                self.log_warning(
                    "Liquidity validation failed - invalid sender address",
                    log_index=log.index,
                    sender_address=liq[2]
                )
                self._create_attr_error(log.index, errors)
                return False
            
            if liq[3] and not self._validate_addresses(liq[3]):
                self.log_warning(
                    "Liquidity validation failed - invalid to address",
                    log_index=log.index,
                    to_address=liq[3]
                )
                self._create_attr_error(log.index, errors)
                return False

            self.log_debug(
                "Liquidity data validation passed",
                log_index=log.index
            )
            return True
            
        except Exception as e:
            self.log_error(
                "Exception during liquidity validation",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            self._create_log_exception(e, log.index, errors)
            return False

    def _handle_swap(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle LB swap log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing LB swap log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )

            swap = self._get_swap_attributes(log)
            if not self._validate_swap_data(log, swap, errors):
                self.log_warning(
                    "LB swap validation failed, skipping signal creation",
                    log_index=log.index
                )
                return
        
            signal = SwapBatchSignal(
                log_index=log.index,
                pattern="Swap_B",
                pool=self.contract_address,
                to=EvmAddress(swap[3].lower()),
                id=swap[0],
                base_amount=swap[1],
                quote_amount=swap[2],
                sender=EvmAddress(swap[4].lower()) if swap[4] else None,
            )
            
            signals[log.index] = signal
            
            self.log_info(
                "LB swap signal created successfully",
                log_index=log.index,
                pool=self.contract_address,
                bin_id=swap[0],
                base_amount=swap[1],
                quote_amount=swap[2],
                taker=swap[3][:10] + "..." if swap[3] else "None"
            )
            
        except Exception as e:
            self.log_error(
                "Failed to handle LB swap log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle LB transfer log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing LB transfer log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )

            trf = self._get_batch_transfer_attributes(log)
            if trf is None:
                self.log_error(
                    "Failed to extract transfer attributes, creating error",
                    log_index=log.index
                )
                self._create_attr_error(log.index, errors)
                return
        
            if not self._validate_batch_transfer_data(log, trf, errors):
                self.log_warning(
                    "LB transfer validation failed, skipping signal creation",
                    log_index=log.index
                )
                return

            signal = TransferSignal(
                log_index=log.index,
                pattern="Transfer",
                token=self.contract_address,
                from_address=EvmAddress(trf[0].lower()),
                to_address=EvmAddress(trf[1].lower()),
                amount=trf[4],
                batch=trf[2],
                sender=EvmAddress(trf[3].lower()) if trf[3] else None,
            )
            
            signals[log.index] = signal
            
            self.log_info(
                "LB transfer signal created successfully",
                log_index=log.index,
                token=self.contract_address,
                from_address=trf[0][:10] + "..." if trf[0] else "None",
                to_address=trf[1][:10] + "..." if trf[1] else "None",
                amount=trf[4],
                bin_count=len(trf[2])
            )
            
        except Exception as e:
            self.log_error(
                "Failed to handle LB transfer log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle LB mint log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing LB mint log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )

            liq = self._get_liquidity_attributes(log)
            if liq is None:
                self.log_error(
                    "Failed to extract liquidity attributes, creating error",
                    log_index=log.index
                )
                self._create_attr_error(log.index, errors)
                return
                
            if not self._validate_liquidity_data(log, liq, errors):
                self.log_warning(
                    "LB mint validation failed, skipping signal creation",
                    log_index=log.index
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
                sender=EvmAddress(liq[2].lower()) if liq[2] else None,
                owner=EvmAddress(liq[3].lower()) if liq[3] else None,
                batch=liq[4] if isinstance(liq[4], dict) else None
            )
            
            signals[log.index] = signal
            
            self.log_info(
                "LB mint signal created successfully",
                log_index=log.index,
                pool=self.contract_address,
                base_amount=liq[0],
                quote_amount=liq[1],
                bin_count=len(liq[4]) if liq[4] else 0,
                owner=liq[3][:10] + "..." if liq[3] else "None"
            )
            
        except Exception as e:
            self.log_error(
                "Failed to handle LB mint log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        """Handle LB burn log with comprehensive error handling"""
        try:
            self.log_debug(
                "Processing LB burn log",
                log_index=log.index,
                contract=self.contract_address,
                log_name=log.name
            )

            liq = self._get_liquidity_attributes(log, negative=True)
            if liq is None:
                self.log_error(
                    "Failed to extract burn liquidity attributes, creating error",
                    log_index=log.index
                )
                self._create_attr_error(log.index, errors)
                return
                
            if not self._validate_liquidity_data(log, liq, errors):
                self.log_warning(
                    "LB burn validation failed, skipping signal creation",
                    log_index=log.index
                )
                return
        
            signal = LiquiditySignal(
                log_index=log.index,
                pattern="Burn_A",
                pool=self.contract_address,
                base_amount=liq[0],
                base_token=self.base_token,
                quote_amount=liq[1],
                quote_token=self.quote_token,
                action="remove",
                sender=EvmAddress(liq[2].lower()) if liq[2] else None,
                owner=EvmAddress(liq[3].lower()) if liq[3] else None,
                batch=liq[4] if isinstance(liq[4], dict) else None
            )
            
            signals[log.index] = signal
            
            self.log_info(
                "LB burn signal created successfully",
                log_index=log.index,
                pool=self.contract_address,
                base_amount=liq[0],
                quote_amount=liq[1],
                bin_count=len(liq[4]) if liq[4] else 0,
                owner=liq[3][:10] + "..." if liq[3] else "None"
            )
            
        except Exception as e:
            self.log_error(
                "Failed to handle LB burn log",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__,
                contract=self.contract_address
            )
            self._create_log_exception(e, log.index, errors)