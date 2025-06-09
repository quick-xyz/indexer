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
                 base_token: EvmAddress, fee_collector: EvmAddress, nfp_manager: EvmAddress):
        super().__init__(contract, token0, token1, base_token, fee_collector)
        self.nfp_manager = nfp_manager.lower()
        self.handler_map = {
            "Swap": self._handle_swap,
            "Mint": self._handle_mint,
            "Burn": self._handle_burn,
            "Collect": self._handle_collect,
            "CollectProtocol": self._handle_collect
        }

    def _get_cl_swap_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str]:
        base_amount, quote_amount = self._get_amounts(log)
        recipient = str(log.attributes.get("recipient", ""))
        sender = str(log.attributes.get("sender", ""))
        
        return base_amount, quote_amount, recipient, sender

    def _get_cl_liquidity_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str]:
        base_amount, quote_amount = self._get_amounts(log)
        owner = str(log.attributes.get("owner", ""))
        sender = str(log.attributes.get("sender", ""))
        receipt_amount = amount_to_str(log.attributes.get("amount", 0))
        
        return base_amount, quote_amount, owner, sender, receipt_amount

    def _get_cl_collect_attributes(self, log: DecodedLog) -> Tuple[str, str, str, str, str]:
        base_amount, quote_amount = self._get_amounts(log)
        recipient = str(log.attributes.get("recipient", ""))
        owner = str(log.attributes.get("owner", ""))
        sender = str(log.attributes.get("sender", ""))
        
        return base_amount, quote_amount, recipient, owner, sender

    def _validate_cl_liquidity_data(self, log: DecodedLog, liq: Tuple[str, str, str, str, str],
                                   errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(liq[:4], log.index, errors): 
            return False
        
        if is_zero(liq[0]) and is_zero(liq[1]):
            if not is_zero(liq[4]):    
                self.log_warning("Both liquidity amounts are zero", log_index=log.index)
                self._create_attr_error(log.index, errors)
                return False
        
        if liq[2] and not self._validate_addresses(liq[2]):
            self.log_warning("Invalid owner address", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False
            
        if liq[3] and not self._validate_addresses(liq[3]):
            self.log_warning("Invalid sender address", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False

        return True

    def _validate_collect_data(self, log: DecodedLog, collect: Tuple[str, str, str, str, str],
                              errors: Dict[ErrorId, ProcessingError]) -> bool:
        if not self._validate_null_attr(collect[:3], log.index, errors):
            return False
        
        if collect[2] and not self._validate_addresses(collect[2]):
            self.log_warning("Invalid recipient address", log_index=log.index)
            self._create_attr_error(log.index, errors)
            return False

        return True

    def _handle_swap(self, log: DecodedLog, signals: Dict[int, Signal], 
                    errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling CL swap log", log_index=log.index)
        
        swap = self._get_cl_swap_attributes(log)
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
            sender=EvmAddress(swap[3].lower()) if swap[3] else None
        )
        self.log_debug("CL swap signal created", log_index=log.index)

    def _handle_mint(self, log: DecodedLog, signals: Dict[int, Signal],
                    errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling CL mint log", log_index=log.index)
        
        liq = self._get_cl_liquidity_attributes(log)
        if not self._validate_cl_liquidity_data(log, liq, errors):
            return
        
        signals[log.index] = LiquiditySignal(
            log_index=log.index,
            pool=self.contract_address,
            base_amount=liq[0],
            base_token=self.base_token,
            quote_amount=liq[1],
            quote_token=self.quote_token,
            receipt_amount=liq[4],  # liquidity amount
            sender=EvmAddress(liq[3].lower()) if liq[3] else None,
            owner=EvmAddress(liq[2].lower()) if liq[2] else None
        )
        self.log_debug("CL mint signal created", log_index=log.index)

    def _handle_burn(self, log: DecodedLog, signals: Dict[int, Signal],
                    errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling CL burn log", log_index=log.index)
        
        liq = self._get_cl_liquidity_attributes(log)
        if not self._validate_cl_liquidity_data(log, liq, errors):
            return
        
        signals[log.index] = LiquiditySignal(
            log_index=log.index,
            pool=self.contract_address,
            base_amount=f"-{liq[0]}" if not liq[0].startswith('-') else liq[0],
            base_token=self.base_token,
            quote_amount=f"-{liq[1]}" if not liq[1].startswith('-') else liq[1],
            quote_token=self.quote_token,
            receipt_amount=f"-{liq[4]}" if not liq[4].startswith('-') else liq[4],
            sender=EvmAddress(liq[3].lower()) if liq[3] else None,
            owner=EvmAddress(liq[2].lower()) if liq[2] else None
        )
        self.log_debug("CL burn signal created", log_index=log.index)

    def _handle_collect(self, log: DecodedLog, signals: Dict[int, Signal],
                       errors: Dict[ErrorId, ProcessingError]) -> None:
        self.log_debug("Handling CL collect log", log_index=log.index)
        
        collect = self._get_cl_collect_attributes(log)
        if not self._validate_collect_data(log, collect, errors):
            return
        
        if not (is_zero(collect[0]) and is_zero(collect[1])):
            signals[log.index] = CollectSignal(
                log_index=log.index,
                contract=self.contract_address,
                recipient=EvmAddress(collect[2].lower()),
                base_amount= collect[0],
                base_token= self.base_token,
                quote_amount= collect[1],
                quote_token= self.quote_token,
                owner=EvmAddress(collect[3].lower()) if collect[2] else None,
                sender=EvmAddress(collect[4].lower()) if collect[3] else None,
            )

        self.log_debug("CL collect signals created", log_index=log.index)