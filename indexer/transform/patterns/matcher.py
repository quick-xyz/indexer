# indexer/transform/patterns/matcher.py
"""
Pattern matcher for validating transfers against patterns with amount inference
"""

from typing import Dict, Set, Optional, Union, Any, List
from collections import defaultdict
from msgspec import Struct

from ...types import EvmAddress, Signal, TransferSignal
from ...types.constants import ZERO_ADDRESS
from .base import TransferPattern, TransferLeg, AddressContext
from .registry import TransferPatternRegistry
from ...utils import safe_nested_get
from ..context import TransformerContext


class PatternMatchResult(Struct):
    pattern: TransferPattern
    matched_transfers: Set[int]
    address_context: AddressContext
    success: bool

class TransferPatternMatcher:
    def __init__(self, pattern_registry: TransferPatternRegistry):
        self.registry = pattern_registry
    
    def process_signal_with_pattern(self, signal: Signal, pattern: TransferPattern, context: TransformerContext) -> Optional[PatternMatchResult]:
        addresses, legs = pattern.generate_transfer_legs(signal, context)

        if addresses is None or legs is None:
            return None

        matched_transfers = set()
        
        for leg in legs:
            matched_transfers = self._match_transfer_leg(leg, context)
            all_matched_transfers.update(matched_transfers.keys())
        
        success = len(missing_legs) == 0
        
        return PatternMatchResult(
            pattern=pattern,
            matched_transfers=all_matched_transfers,
            address_context=address_context,
            success=success,
            missing_legs=missing_legs
        )
    
    def _match_transfer_leg(self, leg: TransferLeg, context: TransformerContext) -> Dict[int, TransferSignal]:
        if not leg.from_end or not leg.to_end:
            return {}
        
        if leg.amount:
            filtered_trf_dict = self._filter_transfers_by_token_and_amount(
                context.trf_dict, leg.token, leg.amount
            )
        else:
            inferred_amount = self._infer_transfer_amount(leg, context)
            if not inferred_amount:
                return {}  # Can't infer amount
            
            filtered_trf_dict = self._filter_transfers_by_token_and_amount(
                context.trf_dict, leg.token, inferred_amount
            )
        
        from_transfers = safe_nested_get(filtered_trf_dict, leg.token, "out", leg.from_end, default={})
        to_transfers = safe_nested_get(filtered_trf_dict, leg.token, "in", leg.to_end, default={})
        
        return self._process_leg_with_filtered_transfers(leg, from_transfers, to_transfers, filtered_trf_dict)
    
    def _infer_transfer_amount(self, leg: TransferLeg, context: TransformerContext) -> Optional[str]:
        from_transfers = safe_nested_get(context.trf_dict, leg.token, "out", leg.from_end, default={})
        
        direct_transfers = [
            transfer for transfer in from_transfers.values()
            if transfer.to_address == leg.to_end
        ]
        
        if direct_transfers:
            return direct_transfers[0].amount
        
        if from_transfers:
            return next(iter(from_transfers.values())).amount
        
        to_transfers = safe_nested_get(context.trf_dict, leg.token, "in", leg.to_end, default={})
        if to_transfers:
            return next(iter(to_transfers.values())).amount
        
        return None
    
    def _filter_transfers_by_token_and_amount(self, trf_dict: Dict, token: str, amount: str) -> Dict:        
        filtered_dict = {}
        
        if token not in trf_dict:
            return filtered_dict
        
        token_data = trf_dict[token]
        filtered_token_data = {"in": {}, "out": {}}
        
        for from_addr, transfers in token_data.get("out", {}).items():
            filtered_transfers = {
                idx: transfer for idx, transfer in transfers.items()
                if transfer.amount == amount
            }
            if filtered_transfers:
                filtered_token_data["out"][from_addr] = filtered_transfers
        
        for to_addr, transfers in token_data.get("in", {}).items():
            filtered_transfers = {
                idx: transfer for idx, transfer in transfers.items()
                if transfer.amount == amount
            }
            if filtered_transfers:
                filtered_token_data["in"][to_addr] = filtered_transfers
        
        if filtered_token_data["in"] or filtered_token_data["out"]:
            filtered_dict[token] = filtered_token_data
        
        return filtered_dict
    
    def _process_leg_with_filtered_transfers(self, leg: TransferLeg, from_transfers: Dict, to_transfers: Dict, filtered_trf_dict: Dict) -> Dict[int, TransferSignal]:
        direct_transfers = {
            idx: transfer for idx, transfer in from_transfers.items()
            if transfer.to_address == leg.to_end
        }
        
        if direct_transfers:
            return direct_transfers
        
        if from_transfers and to_transfers:
            multi_hop_transfers = self._find_multi_hop_path(leg, from_transfers, to_transfers, filtered_trf_dict)
            if multi_hop_transfers:
                return multi_hop_transfers
        
        return {}
    
    def _find_multi_hop_path(self, leg: TransferLeg, from_transfers: Dict, to_transfers: Dict, filtered_trf_dict: Dict) -> Dict[int, TransferSignal]:
        path_transfers = {}
        current_addresses = set()
        
        for idx, transfer in from_transfers.items():
            path_transfers[idx] = transfer
            current_addresses.add(transfer.to_address)
        
        max_hops = 5
        for _ in range(max_hops):
            if leg.to_end in current_addresses:
                break
                
            new_addresses = set()
            for addr in current_addresses:
                if addr == leg.from_end:
                    continue
                    
                intermediate_transfers = safe_nested_get(filtered_trf_dict, leg.token, "out", addr, default={})
                
                for idx, transfer in intermediate_transfers.items():
                    if idx not in path_transfers:  # Avoid cycles
                        path_transfers[idx] = transfer
                        new_addresses.add(transfer.to_address)
            
            if not new_addresses:
                break
                
            current_addresses.update(new_addresses)
        
        if leg.to_end in current_addresses:
            if self._validate_net_flow(leg, path_transfers):
                return path_transfers
        
        return {}
    
    def _validate_net_flow(self, leg: TransferLeg, path_transfers: Dict[int, TransferSignal]) -> bool:
        total_out = sum(
            int(transfer.amount) for transfer in path_transfers.values()
            if transfer.from_address == leg.from_end
        )
        
        total_in = sum(
            int(transfer.amount) for transfer in path_transfers.values()
            if transfer.to_address == leg.to_end
        )
        
        return total_out == total_in and total_out > 0

    def get_address_deltas_for_token(self, token: EvmAddress) -> Dict[EvmAddress, int]:
        deltas = defaultdict(int)
        
        for transfer in self.transfer_signals.values():
            if transfer.token != token:
                continue
                
            amount = int(transfer.amount)
            deltas[transfer.from_address] -= amount
            deltas[transfer.to_address] += amount
            
        return dict(deltas)
    
    def _get_token_trf():
        token_trfs = {}
        