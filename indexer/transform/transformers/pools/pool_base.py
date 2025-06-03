
from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict, Tuple
import msgspec

from ..base import BaseTransformer
from ....types import (
    ZERO_ADDRESS,
    DecodedLog,
    Transaction,
    EvmAddress,
    DomainEvent,
    ProcessingError,
    Transfer,
    UnmatchedTransfer,
    MatchedTransfer,
    DomainEventId,
    ErrorId,
    create_transform_error,
    EvmHash,
)


class PoolTransformer(BaseTransformer):
    def __init__(self, contract: EvmAddress, token0: EvmAddress, token1: EvmAddress, 
                 base_token: EvmAddress, fee_collector: Optional[EvmAddress] = None):
        super().__init__(contract_address=contract)
        self.token0 = EvmAddress(str(token0).lower())
        self.token1 = EvmAddress(str(token1).lower())
        self.base_token = EvmAddress(str(base_token).lower())
        self.quote_token = self.token1 if self.token0 == self.base_token else self.token0
        self.fee_collector = EvmAddress(str(fee_collector).lower()) if fee_collector else None
        
        if self.base_token not in [self.token0, self.token1]:
            raise ValueError(f"Base token {self.base_token} must be one of the pool tokens")

    def get_amounts(self, log: DecodedLog) -> Tuple[Optional[int], Optional[int]]:
        try:
            amount0 = int(log.attributes.get("amount0"))
            amount1 = int(log.attributes.get("amount1"))

            if amount0 is None or amount1 is None:
                return None, None

            return self._get_base_quote_amounts(amount0, amount1, self.token0, self.token1, self.base_token)
        except Exception:
            return None, None

    def get_in_out_amounts(self, log: DecodedLog) -> Tuple[Optional[int], Optional[int]]:
        try:            
            amount0 = int(log.attributes.get("amount0In", 0)) - int(log.attributes.get("amount0Out", 0))
            amount1 = int(log.attributes.get("amount1In", 0)) - int(log.attributes.get("amount1Out", 0))
            
            if self.token0 == self.base_token:
                return amount0, amount1
            else:
                return amount1, amount0
            
        except Exception:
            return None, None

    def _get_liquidity_transfers(self, unmatched_transfers: Dict[DomainEventId, Transfer]) -> Dict[str, Dict[DomainEventId, Transfer]]:
        liq_transfers = {
            "mints": {},
            "burns": {},
            "deposits": {},
            "withdrawals": {},
            "underlying_transfers": {},
            "receipt_transfers": {}
        }

        for key, transfer in unmatched_transfers.items():
            if transfer.token == self.contract_address:
                if transfer.from_address == ZERO_ADDRESS:
                    liq_transfers["mints"][key] = transfer
                elif transfer.to_address == ZERO_ADDRESS:
                    liq_transfers["burns"][key] = transfer
                else:
                    liq_transfers["receipt_transfers"][key] = transfer

            elif transfer.token in [self.base_token, self.quote_token]:
                if transfer.to_address == self.contract_address:
                    liq_transfers["deposits"][key] = transfer
                elif transfer.from_address == self.contract_address:
                    liq_transfers["withdrawals"][key] = transfer
                else:
                    liq_transfers["underlying_transfers"][key] = transfer
                        
        return liq_transfers

    def _get_swap_transfers(self, unmatched_transfers: Dict[DomainEventId, Transfer]) -> Dict[str, Dict[DomainEventId, Transfer]]:
        swap_transfers = {
            "base_swaps": {},
            "quote_swaps": {},
        }

        for key, transfer in unmatched_transfers.items():
            if not (transfer.from_address == self.contract_address or transfer.to_address == self.contract_address):
                continue
            
            if transfer.token == self.base_token:
                swap_transfers["base_swaps"][key] = transfer
            elif transfer.token == self.quote_token:
                swap_transfers["quote_swaps"][key] = transfer

        return swap_transfers