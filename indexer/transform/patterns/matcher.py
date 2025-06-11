# indexer/transform/patterns/matcher.py

from typing import Dict, Set, Optional, Union, Any, List
from dataclasses import dataclass

from ...types import EvmAddress, Signal, TransferSignal
from ...types.constants import ZERO_ADDRESS
from .base import TransferPattern, TransferLeg, AddressPattern
from .registry import TransferPatternRegistry


@dataclass 
class PatternMatchResult:
    pattern: TransferPattern
    matched_transfers: Set[int]
    context_data: Dict[str, Any]
    success: bool
    missing_legs: List[str] = None
    
    def __post_init__(self):
        if self.missing_legs is None:
            self.missing_legs = []


class TransferPatternMatcher:
    def __init__(self, pattern_registry: TransferPatternRegistry):
        self.registry = pattern_registry
    
    def match_signal_to_pattern(self, signal: Signal, pattern: TransferPattern, context) -> Optional[PatternMatchResult]:
        context_data = pattern.extract_context_data(signal, context)
        
        if not self._validate_context_data(context_data):
            return PatternMatchResult(pattern, set(), context_data, False)
        
        legs = pattern.generate_legs(signal, context_data)
        
        all_matched_transfers = set()
        missing_legs = []
        
        for leg in legs:
            matched_transfers = self._match_transfer_leg(leg, context_data, context)
            
            if len(matched_transfers) < leg.min_transfers or len(matched_transfers) > leg.max_transfers:
                missing_legs.append(leg.description)
                continue
            
            all_matched_transfers.update(matched_transfers.keys())
        
        success = len(missing_legs) == 0
        
        return PatternMatchResult(
            pattern=pattern,
            matched_transfers=all_matched_transfers,
            context_data=context_data,
            success=success,
            missing_legs=missing_legs
        )
    
    def _validate_context_data(self, context_data: Dict[str, Any]) -> bool:
        for key in ["provider", "taker", "pool", "router"]:
            if key in context_data and context_data[key] is None:
                return False
        return True
    
    def _match_transfer_leg(self, leg: TransferLeg, context_data: Dict[str, Any], context) -> Dict[int, TransferSignal]:
        from_addr = self._resolve_address_pattern(leg.from_pattern, context_data)
        to_addr = self._resolve_address_pattern(leg.to_pattern, context_data)
        
        if not from_addr or not to_addr:
            return {}
        
        from_transfers = self._safe_nested_get(context.trf_dict, leg.token, "out", from_addr, default={})
        
        matching_transfers = {
            idx: transfer for idx, transfer in from_transfers.items()
            if transfer.to_address == to_addr
        }
        
        return matching_transfers
    
    def _resolve_address_pattern(self, pattern: Union[AddressPattern, EvmAddress], 
                                context_data: Dict[str, Any]) -> Optional[EvmAddress]:
        if isinstance(pattern, AddressPattern):
            if pattern == AddressPattern.PROVIDER:
                return context_data.get("provider")
            elif pattern == AddressPattern.POOL:
                return context_data.get("pool")
            elif pattern == AddressPattern.ZERO:
                return ZERO_ADDRESS
            elif pattern == AddressPattern.ROUTER:
                return context_data.get("router")
            elif pattern == AddressPattern.TAKER:
                return context_data.get("taker")
            elif pattern == AddressPattern.FEE_COLLECTOR:
                return context_data.get("fee_collector")
        else:
            return pattern
        
        return None
    
    def _safe_nested_get(self, d, *keys, default=None):
        for key in keys:
            if isinstance(d, dict) and key in d:
                d = d[key]
            else:
                return default
        return d