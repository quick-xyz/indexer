# indexer/transform/patterns/base.py

from typing import Dict, List, Set, Optional, Union, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum

from ...types import EvmAddress, Signal
from ..context import TransformerContext

class AddressPattern(Enum):
    PROVIDER = "provider"
    POOL = "pool" 
    ZERO = "zero"
    ROUTER = "router"
    TAKER = "taker"
    FEE_COLLECTOR = "fee_collector"

@dataclass
class TransferLeg:
    token: EvmAddress
    from_pattern: Union[AddressPattern, EvmAddress]
    to_pattern: Union[AddressPattern, EvmAddress]
    min_transfers: int = 1
    max_transfers: Optional[int] = 3

class TransferPattern(ABC):
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def generate_legs(self, signal: Signal, context_data: Dict[str, Any]) -> List[TransferLeg]:
        """Generate transfer legs for this pattern given a signal"""
        pass
    
    @abstractmethod
    def extract_context_data(self, signal: Signal, context: TransformerContext) -> Dict[str, Any]:
        """Extract addresses and other context needed for pattern matching"""
        pass