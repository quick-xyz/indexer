# indexer/transform/transformers/pools/pool_base.py

from typing import Optional, Dict, Tuple

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
from ....utils.amounts import amount_to_int, amount_to_str, is_zero


class PoolTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, 
                 base_token: EvmAddress, fee_collector: Optional[EvmAddress] = None):
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
        
        if self.base_token not in [self.token0, self.token1]:
            raise ValueError(f"Base token {self.base_token} must be one of the pool tokens")

    def _get_in_out_amounts(self, log: DecodedLog) -> Tuple[str, str]:         
        amount0_in = amount_to_int(log.attributes.get("amount0In", 0))
        amount0_out = amount_to_int(log.attributes.get("amount0Out", 0))
        amount1_in = amount_to_int(log.attributes.get("amount1In", 0))
        amount1_out = amount_to_int(log.attributes.get("amount1Out", 0))
        
        amount0 = amount_to_str(amount0_in - amount0_out)
        amount1 = amount_to_str(amount1_in - amount1_out)

        if self.token0 == self.base_token:
            return amount0, amount1
        else:
            return amount1, amount0

    def _get_amounts(self, log: DecodedLog) -> Tuple[str, str]:
        amount0 = amount_to_str(log.attributes.get("amount0", 0))
        amount1 = amount_to_str(log.attributes.get("amount1", 0))

        if self.token0 == self.base_token:
            return amount0, amount1
        else:
            return amount1, amount0

    def _get_swap_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        base_amount, quote_amount = self._get_in_out_amounts(log)
        to = str(log.attributes.get("to", ""))
        sender = str(log.attributes.get("sender", ""))
        
        return base_amount, quote_amount, to, sender

    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        from_addr = str(log.attributes.get("from", ""))
        to_addr = str(log.attributes.get("to", ""))
        value = amount_to_str(log.attributes.get("value", 0))
        sender = str(log.attributes.get("sender", ""))
        
        return from_addr, to_addr, value, sender
    
    def _get_liquidity_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        base_amount, quote_amount = self._get_amounts(log)
        sender = str(log.attributes.get("sender", ""))
        to = str(log.attributes.get("to", ""))
        
        return base_amount, quote_amount, sender, to
    
    def _validate_swap_data(self, log: DecodedLog, swap: Tuple[str, str, str, str],
                            errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(swap, log.index, errors):
            return False
        if is_zero(swap[0]) or is_zero(swap[1]):
            self.log_warning("Swap amounts are zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        if not self._validate_addresses(swap[2], swap[3]):
            self.log_warning("Swap addresses are invalid", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        return True

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(trf, log.index, errors):
            return False
        if is_zero(trf[2]):
            self.log_warning("Transfer amount is zero",log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        return True

    def _validate_liquidity_data(self, log: DecodedLog, liq: Tuple[str, str, str, str],
                                 errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(liq, log.index, errors):
            return False
        if is_zero(liq[0]) or is_zero(liq[1]):
            self.log_warning("A liquidity amount is zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False

        return True

    def _handle_swap(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling swap log", log_index=log.index)
        
        swap = self._get_swap_attributes(log)
        if not self._validate_swap_data(log, swap, errors):
            return
        
        signals[log.index] = SwapSignal(
            log_index=log.index,
            pool=self.contract_address,
            base_amount=swap[0],
            base_token=self.base_token,
            quote_amount=swap[1],
            quote_token=self.quote_token,
            to=EvmAddress(swap[2].lower()),
            sender=EvmAddress(swap[3].lower()) if swap[3] else None,
        )
        self.log_debug("Swap signal created", log_index=log.index)

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling transfer log", log_index=log.index)

        trf = self._get_transfer_attributes(log)
        if not self._validate_transfer_data(log, trf, errors):
            return
        
        signals[log.index] = TransferSignal(
            log_index=log.index,
            token=self.contract_address,
            from_address=EvmAddress(trf[0].lower()),
            to_address=EvmAddress(trf[1].lower()),
            amount=trf[2],
            sender=EvmAddress(trf[3].lower()) if trf[3] else None
        )
        self.log_debug("Transfer signal created", log_index=log.index)

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling mint log", log_index=log.index)

        liq = self._get_liquidity_attributes(log)
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
            owner=EvmAddress(liq[3].lower()) if liq[3] else None
        )
        self.log_debug("Mint signal created", log_index=log.index)

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling burn log", log_index=log.index)

        liq = self._get_liquidity_attributes(log)
        if not self._validate_liquidity_data(log, liq, errors):
            return
        
        signals[log.index] = LiquiditySignal(
            log_index=log.index,
            pool=self.contract_address,
            base_amount=f"-{liq[0]}" if not liq[0].startswith('-') else liq[0],
            base_token=self.base_token,
            quote_amount=f"-{liq[1]}" if not liq[1].startswith('-') else liq[1],
            quote_token=self.quote_token,
            sender=EvmAddress(liq[2].lower()) if liq[2] else None,
            owner=EvmAddress(liq[3].lower()) if liq[3] else None
        )
        self.log_debug("Burn signal created", log_index=log.index)