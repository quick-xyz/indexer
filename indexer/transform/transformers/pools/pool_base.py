# indexer/transform/transformers/pools/pool_base.py

from typing import List, Optional, Dict, Tuple

from ..base import BaseTransformer
from ....types import (
    ZERO_ADDRESS,
    DecodedLog,
    Transaction,
    EvmAddress,
    ProcessingError,
    Transfer,
    MatchedTransfer,
    DomainEventId,
    ErrorId,
    create_transform_error,
    EvmHash,
    Position,
)
from ....utils.amounts import amount_to_int, amount_to_str, abs_amount


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

    def get_amounts(self, log: DecodedLog) -> Tuple[Optional[str], Optional[str]]:
        try:
            amount0 = amount_to_str(log.attributes.get("amount0"))
            amount1 = amount_to_str(log.attributes.get("amount1"))

            if amount0 is None or amount1 is None:
                return None, None

            return self._get_base_quote_amounts(amount0, amount1, self.token0, self.token1, self.base_token)
        except Exception:
            return None, None

    def get_in_out_amounts(self, log: DecodedLog) -> Tuple[Optional[str], Optional[str]]:
        try:            
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
        swap_transfers = {"base_swaps": {},"quote_swaps": {}}

        for key, transfer in unmatched_transfers.items():
            if not (transfer.from_address == self.contract_address or transfer.to_address == self.contract_address):
                continue
            
            if transfer.token == self.base_token:
                swap_transfers["base_swaps"][key] = transfer
            elif transfer.token == self.quote_token:
                swap_transfers["quote_swaps"][key] = transfer

        return swap_transfers
    
    def _validate_bin_consistency(self, amounts: List, bins: List, transfer_bins: List, 
                                 tx_hash: EvmHash, log_index: int, error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        if len(amounts) != len(bins):
            error = create_transform_error(
                error_type="invalid_lb_transfer",
                message=f"LB: Expected amounts and bins to have the same length, got {len(amounts)} and {len(bins)}",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False

        if transfer_bins != bins:
            error = create_transform_error(
                error_type="invalid_lb_transfer",
                message="Transfer bins do not match event bins",
                tx_hash=tx_hash,
                log_index=log_index
            )
            error_dict[error.error_id] = error
            return False

        return True

    def _unpack_lb_amounts(self, amounts: bytes) -> Tuple[str, str]:
        try:
            if isinstance(amounts, str):
                amounts = amounts.replace("0x", "")
                packed_amounts = bytes.fromhex(amounts)
            else:
                packed_amounts = amounts
            
            amounts_x = int.from_bytes(packed_amounts, byteorder="big") & (2 ** 128 - 1)
            amounts_y = int.from_bytes(packed_amounts, byteorder="big") >> 128

            if self.token0 == self.base_token:
                return amount_to_str(amounts_x), amount_to_str(amounts_y)
            else:
                return amount_to_str(amounts_y), amount_to_str(amounts_x)
        except Exception:
            return None, None


    def _create_positions_from_bins(self, bins: List[int], amounts: List, bin_amounts: Dict, 
                                   tx: Transaction, log: DecodedLog, negative: bool = False) -> Tuple[Dict, str, str, str]:
        positions = {}
        sum_base, sum_quote, sum_receipt = 0, 0, 0
        multiplier = -1 if negative else 1

        for i, bin_id in enumerate(bins):
            base_amount_int, quote_amount_int = self._unpack_lb_amounts(amounts[i])
            base_amount_int = amount_to_int(base_amount_int)
            quote_amount_int = amount_to_int(quote_amount_int)
            receipt_amount = amount_to_int(bin_amounts[bin_id])

            position = Position(
                timestamp=tx.timestamp,
                tx_hash=tx.tx_hash,
                receipt_token=log.contract,
                receipt_id=bin_id,
                amount_base=amount_to_str(base_amount_int * multiplier),
                amount_quote=amount_to_str(quote_amount_int * multiplier),
                amount_receipt=amount_to_str(receipt_amount * multiplier),
            )
            positions[position.content_id] = position
            
            sum_base += base_amount_int
            sum_quote += quote_amount_int
            sum_receipt += receipt_amount

        return positions, amount_to_str(sum_base), amount_to_str(sum_quote), amount_to_str(sum_receipt)

    def _aggregate_swap_logs(self, swap_logs: List[DecodedLog]) -> Tuple[str, str, Dict]:
        net_base_amount, net_quote_amount = 0, 0
        swap_batch = {}

        for log in swap_logs:
            base_amount_in, quote_amount_in = self._unpack_lb_amounts(log.attributes.get("amountsIn"))
            base_amount_out, quote_amount_out = self._unpack_lb_amounts(log.attributes.get("amountsOut"))

            base_amount_in = amount_to_int(base_amount_in)
            quote_amount_in = amount_to_int(quote_amount_in)
            base_amount_out = amount_to_int(base_amount_out)
            quote_amount_out = amount_to_int(quote_amount_out)

            base_amount = base_amount_in - base_amount_out
            quote_amount = quote_amount_in - quote_amount_out
            bin_id = int(log.attributes.get("id"))
            
            swap_batch[bin_id] = {"base": amount_to_str(base_amount), "quote": amount_to_str(quote_amount)}
            net_base_amount += base_amount
            net_quote_amount += quote_amount

        return amount_to_str(net_base_amount), amount_to_str(net_quote_amount), swap_batch

    def _handle_batch_transfers(self, transfers_to_match: List[Transfer]) -> Dict[DomainEventId, MatchedTransfer]:
        return self._create_matched_transfers_dict(transfers_to_match)

    def _validate_transfer_counts_flexible(self, transfer_groups: List[Tuple], tx_hash: EvmHash, log_index: int, 
                                         error_type: str, error_dict: Dict[ErrorId, ProcessingError]) -> bool:
        for transfers, name, expected, is_max in transfer_groups:
            if is_max:
                if len(transfers) > expected:
                    error = create_transform_error(
                        error_type=error_type,
                        message=f"Expected at most {expected} {name}, found {len(transfers)}",
                        tx_hash=tx_hash, log_index=log_index
                    )
                    error_dict[error.error_id] = error
                    return False
            else:
                if not self._validate_transfer_count(transfers, name, expected, tx_hash, log_index, error_type, error_dict):
                    return False
        return True