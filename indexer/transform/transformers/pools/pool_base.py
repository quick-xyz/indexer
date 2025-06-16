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
        
        amount0 = amount_to_str(amount0_out - amount0_in)
        amount1 = amount_to_str(amount1_out - amount1_in)

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
        # Check for None values
        if any(v is None for v in swap):
            self.log_warning("Swap has null attributes", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        # Check amounts (can be int or string, but not zero)
        base_amount, quote_amount = swap[0], swap[1]
        
        if self._is_zero_amount(base_amount) or self._is_zero_amount(quote_amount):
            self.log_warning("Swap amounts are zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        # Validate addresses (not empty strings)
        if not self._validate_addresses(swap[2], swap[3]):
            self.log_warning("Swap addresses are invalid", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        return True

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        # Check for None values
        if any(v is None for v in trf):
            self.log_warning("Transfer has null attributes", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        # Check amount (can be int or string, but not zero)
        if self._is_zero_amount(trf[2]):
            self.log_warning("Transfer amount is zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        return True

    def _validate_liquidity_data(self, log: DecodedLog, liq: Tuple[str, str, str, str],
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        # Check for None values
        if any(v is None for v in liq):
            self.log_warning("Liquidity has null attributes", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        # Check amounts (can be int or string, but not both zero)
        if self._is_zero_amount(liq[0]) and self._is_zero_amount(liq[1]):
            self.log_warning("Both liquidity amounts are zero", log_index=log.index)
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
            pattern="Swap_A",
            pool=self.contract_address,
            base_amount=str(swap[0]),  # Convert to string here
            base_token=self.base_token,
            quote_amount=str(swap[1]),  # Convert to string here
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
            pattern="Transfer",
            token=self.contract_address,
            from_address=EvmAddress(trf[0].lower()),
            to_address=EvmAddress(trf[1].lower()),
            amount=str(trf[2]),  # Convert to string here
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
            pattern="Mint_A",
            pool=self.contract_address,
            base_amount=str(liq[0]),   # Convert to string here
            base_token=self.base_token,
            quote_amount=str(liq[1]),  # Convert to string here
            quote_token=self.quote_token,
            action="add",
            sender=EvmAddress(liq[2].lower()) if liq[2] else None,
            owner=EvmAddress(liq[3].lower()) if liq[3] else None
        )
        self.log_debug("Mint signal created", log_index=log.index)

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling burn log", log_index=log.index)

        liq = self._get_liquidity_attributes(log)
        if not self._validate_liquidity_data(log, liq, errors):
            return
        
        # Convert to string and make negative
        base_amount = str(liq[0])
        quote_amount = str(liq[1])
        
        signals[log.index] = LiquiditySignal(
            log_index=log.index,
            pattern="Burn_A",
            pool=self.contract_address,
            base_amount=f"-{base_amount}" if not base_amount.startswith('-') else base_amount,
            base_token=self.base_token,
            quote_amount=f"-{quote_amount}" if not quote_amount.startswith('-') else quote_amount,
            quote_token=self.quote_token,
            action="remove",
            sender=EvmAddress(liq[2].lower()) if liq[2] else None,
            owner=EvmAddress(liq[3].lower()) if liq[3] else None
        )
        self.log_debug("Burn signal created", log_index=log.index)

    def _is_zero_amount(self, amount: Any) -> bool:
        """Check if amount is zero (handles both int and string)"""
        if amount is None:
            return True
        
        if isinstance(amount, (int, float)):
            return amount == 0
        
        if isinstance(amount, str):
            return amount == "" or amount == "0"
        
        return True  # Unknown type, treat as zero