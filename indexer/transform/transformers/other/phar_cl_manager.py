# indexer/transform/transformers/routers/phar_cl_manager.py

from typing import Optional, Dict, Tuple

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
from ....utils.amounts import amount_to_int, amount_to_str, is_zero

class PharNfpTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress):
        super().__init__(contract_address=contract)
        self.handler_map = {
            "Collect": self._handle_collect,
            "IncreaseLiquidity": self._handle_mint,
            "DecreaseLiquidity": self._handle_burn,
            "Transfer": self._handle_transfer
        }
    
    def _get_collect_attributes(self, log: DecodedLog) -> Tuple[int, str, str, str]:
        token_id = int(log.attributes.get("tokenId", 0))
        recipient = str(log.attributes.get("recipient", ""))
        amount0 = amount_to_str(log.attributes.get("amount0", 0))
        amount1 = amount_to_str(log.attributes.get("amount1", 0))

        return token_id, recipient, amount0, amount1
    
    def _get_liquidity_attributes(self, log: DecodedLog) -> Tuple[int, str, str, str]:
        token_id = int(log.attributes.get("tokenId", 0))
        liquidity = amount_to_str(log.attributes.get("liquidity", 0))
        amount0 = amount_to_str(log.attributes.get("amount0", 0))
        amount1 = amount_to_str(log.attributes.get("amount1", 0))

        return token_id, liquidity, amount0, amount1
    
    def _get_transfer_attributes(self, log: DecodedLog) -> Tuple[int, str, str]:
        token_id = int(log.attributes.get("tokenId", 0))
        from_addr = str(log.attributes.get("from", ""))
        to_addr = str(log.attributes.get("to", ""))
        return token_id, from_addr, to_addr
    
    def _validate_collect_data(self, log: DecodedLog, collect: Tuple[int, str, str, str], errors: Dict[ErrorId, ProcessingError]) -> bool:       
        if not self._validate_null_attr(collect, log.index, errors):
            return False
        
        if collect[1] and not self._validate_addresses(collect[1]):
            self.log_warning("Invalid recipient address", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        return True

    def _validate_liquidity_data(self, log: DecodedLog, liq: Tuple[int, str, str, str], errors: Dict[ErrorId, ProcessingError]) -> bool:       
        if not self._validate_null_attr(liq, log.index, errors):
            return False

        if is_zero(liq[2]) and is_zero(liq[3]):
            self.log_warning("Both liquidity amounts are zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        if is_zero(liq[1]):
            self.log_warning("Liquidity receipt delta is zero", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        
        return True

    def _validate_transfer_data(self, log: DecodedLog, trf: Tuple[str, str, str], 
                                errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(trf, log.index, errors):
            return False
        if is_zero(trf[0]):
            self.log_warning("Transfer amount is zero",log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
        return True
    
    def _handle_collect(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling NFP Collect log", log_index=log.index)

        collect = self._get_collect_attributes(log)
        if not self._validate_collect_data(log, collect, errors):
            return
        
        signals[log.index] = NfpCollectSignal(
            log_index=log.index,
            pattern="Collect_D",
            contract=self.contract_address,
            token_id=collect[0],
            recipient=EvmAddress(collect[1].lower()),
            amount0=collect[2],
            amount1=collect[3]
        )
        self.log_debug("NFP Collect signal created", log_index=log.index)

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling NFP Mint log", log_index=log.index)

        liq = self._get_liquidity_attributes(log)
        if not self._validate_liquidity_data(log, liq, errors):
            return
        
        signals[log.index] = NfpLiquiditySignal(
            log_index=log.index,
            pattern="Mint_D",
            contract=self.contract_address,
            token_id=liq[0],
            liquidity=liq[1],
            amount0=liq[2],
            amount1=liq[3],
            action="add",
        )
        self.log_debug("NFP Mint signal created", log_index=log.index)

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling NFP Burn log", log_index=log.index)

        liq = self._get_liquidity_attributes(log)
        if not self._validate_liquidity_data(log, liq, errors):
            return
        
        signals[log.index] = NfpLiquiditySignal(
            log_index=log.index,
            pattern="Burn_D",
            contract=self.contract_address,
            token_id=liq[0],
            liquidity=f"-{liq[1]}" if not liq[1].startswith('-') else liq[1],
            amount0=f"-{liq[2]}" if not liq[2].startswith('-') else liq[2],
            amount1=f"-{liq[3]}" if not liq[3].startswith('-') else liq[3],
            action="remove",
        )
        self.log_debug("NFP Burn signal created", log_index=log.index)

    def _handle_transfer(self, log: DecodedLog, signals: Dict[int, Signal], errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling transfer log", log_index=log.index)

        trf = self._get_transfer_attributes(log)
        if not self._validate_transfer_data(log, trf, errors):
            return
        
        signals[log.index] = TransferSignal(
            log_index=log.index,
            pattern="Transfer",
            token=self.contract_address,
            from_address=EvmAddress(trf[1].lower()),
            to_address=EvmAddress(trf[2].lower()),
            amount=amount_to_str(1),
            token_id=trf[0]
        )
        self.log_debug("Transfer signal created", log_index=log.index)