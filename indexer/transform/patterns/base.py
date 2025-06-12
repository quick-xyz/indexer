# indexer/transform/patterns/base.py

from typing import Dict, List, Set, Optional, Union, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
from msgspec import Struct

from ...types import EvmAddress, Signal
from ..context import TransformContext


class TransferLeg(Struct):
    token: EvmAddress
    from_end: EvmAddress
    to_end: EvmAddress
    min_transfers: int = 1
    max_transfers: int = 3
    amount: Optional[str] = None

class AddressContext(Struct):
    base: EvmAddress
    quote: Optional[EvmAddress] = None
    provider: Optional[EvmAddress] = None
    taker: Optional[EvmAddress] = None
    pool: Optional[EvmAddress] = None
    router: Optional[EvmAddress] = None
    fee_collector: Optional[EvmAddress] = None


class TransferPattern(ABC):
    def __init__(self, name: str):
        self.name = name
    
    @abstractmethod
    def generate_transfer_legs(self, signal: Signal, context: TransformContext) -> Optional[List[TransferLeg]]:
        pass
    
    @abstractmethod
    def extract_addresses(self, signal: Signal, context: TransformContext) -> Optional[AddressContext]:
        pass

    @abstractmethod
    def process_signal(self, signal: Signal, context: TransformContext) -> Optional[AddressContext]:
        pass
                
    def _update_context(self) -> None:
        pass

    def _reconcile_event_transfers(self) -> None:
        pass
