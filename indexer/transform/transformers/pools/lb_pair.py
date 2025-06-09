# indexer/transform/transformers/pools/lb_pair.py

from typing import Dict, Tuple, Optional
import ast
import codecs

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
        super().__init__(contract,token_x,token_y,base_token,fee_collector=None)
        self.handler_map = {
            "Swap": self._handle_swap,
            "DepositedToBins": self._handle_mint,
            "WithdrawnFromBins": self._handle_burn,
            "TransferBatch": self._handle_transfer
        }

    @property
    def token_x(self):
        return self.token0
    
    @property 
    def token_y(self):
        return self.token1

    def _unpack_amounts(self, amounts: str) -> Tuple[str, str]:
        self.log_debug("Unpacking amounts", 
                    amounts_type=type(amounts).__name__,
                    amounts_value=str(amounts)[:100],  # Truncate for readability
                    amounts_startswith_b=amounts.startswith("b'") if isinstance(amounts, str) else False)
        try:
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
            
            self.log_debug("Amount unpacking successful",amounts_x=amounts_x,amounts_y=amounts_y)
            
            return result
        
        except Exception as e:
            self.log_error("Amount unpacking failed", 
                        amounts_type=type(amounts).__name__,
                        amounts_value=str(amounts)[:50],
                        error=str(e))
            return 0, 0

    def _get_in_out_amounts(self, log: DecodedLog) -> Tuple[str, str]:         
        base_amount_in, quote_amount_in = self._unpack_amounts(log.attributes.get("amountsIn"))
        base_amount_out, quote_amount_out = self._unpack_amounts(log.attributes.get("amountsOut"))
        
        base_amount = base_amount_in - base_amount_out
        quote_amount = quote_amount_in - quote_amount_out

        return amount_to_str(base_amount), amount_to_str(quote_amount)

    def _prepare_bins_and_amounts(self, log: DecodedLog) -> Optional[Tuple[Dict[str, str],str]]:
        try:    
            bins = [str(bin_id) for bin_id in log.attributes.get("ids")]
            amounts = [str(amt) for amt in log.attributes.get("amounts")]
            sum = add_amounts(amounts)

            if len(bins) != len(amounts):
                self.log_warning(
                    "Bins and amounts length mismatch", 
                    log_index=log.index,
                    bins_length=len(bins),
                    amounts_length=len(amounts)
                )
                return None

            bins_amounts = dict(zip(bins, amounts))

            self.log_debug("Prepared bins and amounts",log_index=log.index)

            return bins_amounts, sum

        except Exception as e:
            self.log_error(
                "Failed to prepare bins and amounts",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            return None        

    def _prepare_bins_and_packed_amounts(self, log: DecodedLog, negative: bool) -> Optional[Tuple[Dict[str, Dict[str,str]],str, str]]:
        try:    
            multiplier = -1 if negative else 1

            bins = [str(bin_id) for bin_id in log.attributes.get("ids")]
            amounts = [self._unpack_amounts(amt) for amt in log.attributes.get("amounts")]
            amounts_raw = log.attributes.get("amounts",[])
            amounts = []

            for amts in amounts_raw:
                base_amount, quote_amount = self._unpack_amounts(amts)
                amounts.append({
                    "base": str(base_amount * multiplier), 
                    "quote": str(quote_amount * multiplier)
                })
            
            sum_base = add_amounts([amt["base"] for amt in amounts])
            sum_quote = add_amounts([amt["quote"] for amt in amounts])

            if len(bins) != len(amounts):
                self.log_warning(
                    "Bins and amounts length mismatch", 
                    log_index=log.index,
                    bins_length=len(bins),
                    amounts_length=len(amounts)
                )
                return None

            bins_amounts = dict(zip(bins, amounts))

            self.log_debug("Prepared bins and amounts",log_index=log.index)

            return bins_amounts, sum_base, sum_quote

        except Exception as e:
            self.log_error(
                "Failed to prepare bins and amounts",
                log_index=log.index,
                error=str(e),
                exception_type=type(e).__name__
            )
            return None  

    def _get_swap_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str]:
        id = int(log.attributes.get("id", 0))
        base_amount, quote_amount = self._get_in_out_amounts(log)
        sender = str(log.attributes.get("sender", ""))
        to = str(log.attributes.get("to", ""))

        return id, base_amount, quote_amount, to, sender
    
    def _get_batch_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, Dict[str,str], str, str]:
        from_addr = str(log.attributes.get("from"))
        to_addr = str(log.attributes.get("to"))
        result = self._prepare_bins_and_amounts(log)
        
        if result is None:
            return None
        
        bins_amounts, sum = result
        sender = str(log.attributes.get("sender", ""))

        return from_addr, to_addr, bins_amounts, sender, sum
    
    def _get_liquidity_attributes(self, log: DecodedLog, negative: bool = False) -> Tuple[str, str, str, str,Dict[str, Dict[str,str]]]:
        
        self.log_debug("Processing liquidity attributes",
                    log_index=log.index,
                    amounts_attr_type=type(log.attributes.get("amounts")).__name__,
                    amounts_length=len(log.attributes.get("amounts", [])),
                    ids_type=type(log.attributes.get("ids")).__name__)
    
        result = self._prepare_bins_and_packed_amounts(log, negative)
        if result is None:
            return None
        
        bins_amounts, base_amount, quote_amount = result
        sender = str(log.attributes.get("sender", ""))
        to = str(log.attributes.get("to", ""))

        return base_amount, quote_amount, sender, to, bins_amounts
    
    def _validate_swap_data(self, log: DecodedLog, swap: Tuple[str, str, str, str, str],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(swap, log.index, errors):
            return False
        if is_zero(swap[0]):
            self.log_warning("BinId is zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        if is_zero(swap[1])or is_zero(swap[2]):
            self.log_warning("Swap amounts are zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        if not self._validate_addresses(swap[3], swap[4]):
            self.log_warning("Swap addresses are invalid", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        return True

    def _validate_batch_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, Dict[str,str],str,str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(trf, log.index, errors):
            return False
        return True

    def _validate_liquidity_data(self, log: DecodedLog, liq: Tuple[str, str, str, str, Dict[str, Dict[str, str]]],
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(liq[:4], log.index, errors):
            return False
        
        if is_zero(liq[0]) and is_zero(liq[1]):
            self.log_warning("Both liquidity amounts are zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        if liq[2] and not self._validate_addresses(liq[2]):
            self.log_warning("Invalid sender address", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
            
        if liq[3] and not self._validate_addresses(liq[3]):
            self.log_warning("Invalid to address", log_index=log.index) 
            self._create_attr_error(log.index, errors)
            return False

        return True

    def _handle_swap(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling swap log", log_index=log.index)

        swap = self._get_swap_attributes(log)
        if not self._validate_swap_data(log, swap, errors):
            return
        
        signals[log.index] = SwapBatchSignal(
            log_index=log.index,
            pool=self.contract_address,
            to=EvmAddress(swap[3].lower()),
            id=swap[0],
            base_amount=swap[1],
            quote_amount=swap[2],
            sender=EvmAddress(swap[4].lower()) if swap[4] else None,
        )

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling transfer log", log_index=log.index)

        trf = self._get_batch_transfer_attributes(log)
        if trf is None:
            self._create_attr_error(log.index, errors)
            return
        
        if not self._validate_batch_transfer_data(log, trf, errors):
            return

        signals[log.index] = TransferSignal(
            log_index=log.index,
            token=self.contract_address,
            from_address=EvmAddress(trf[0].lower()),
            to_address=EvmAddress(trf[1].lower()),
            amount = trf[4],
            batch = trf[2],
            sender=EvmAddress(trf[3].lower()) if trf[3] else None,
        )
        self.log_debug("Transfer signal created", log_index=log.index)

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling mint log", log_index=log.index)

        liq = self._get_liquidity_attributes(log)
        if liq is None:
            self._create_attr_error(log.index, errors)
            return
        if not self._validate_liquidity_data(log, liq, errors):
            return
        
        signals[log.index] = LiquiditySignal(
            log_index=log.index,
            pool=self.contract_address,
            base_amount=liq[0],
            base_token=self.base_token,
            quote_amount=liq[1],
            quote_token=self.quote_token,
            sender=EvmAddress(liq[2].lower()) if liq[2] else None,
            to=EvmAddress(liq[3].lower()) if liq[3] else None,
            batch=liq[4] if isinstance(liq[4], dict) else None
        )
        self.log_debug("Mint signal created", log_index=log.index)

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling burn log", log_index=log.index)

        liq = self._get_liquidity_attributes(log, negative=True)
        if not self._validate_liquidity_data(log, liq, errors):
            return
        
        signals[log.index] = LiquiditySignal(
            log_index=log.index,
            pool=self.contract_address,
            base_amount=liq[0],
            base_token=self.base_token,
            quote_amount=liq[1],
            quote_token=self.quote_token,
            sender=EvmAddress(liq[2].lower()) if liq[2] else None,
            to=EvmAddress(liq[3].lower()) if liq[3] else None,
            batch=liq[4] if isinstance(liq[4], dict) else None
        )
        self.log_debug("Burn signal created", log_index=log.index)