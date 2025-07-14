# indexer/types/model/parameters.py

from typing import Literal, Optional, Dict

from ..new import EvmAddress, EvmHash
from .base import DomainEvent, DomainEventId, Signal


class ParameterSignal(Signal, tag=True):
    contract: EvmAddress
    parameter: str
    value_type: Literal["address", "string", "int", "bool"]
    new_value: str
    old_value: Optional[str] = None

class ParameterChange(DomainEvent, tag=True):
    contract: EvmAddress
    parameter: str
    value_type: Literal["address", "string", "int", "bool"]
    new_value: str
    old_value: Optional[str] = None

    @classmethod
    def from_signal(cls, signal: ParameterSignal, timestamp: int, tx_hash: EvmHash):
        return cls(
            timestamp=timestamp,
            tx_hash=tx_hash,
            contract=signal.contract,
            parameter=signal.parameter,
            value_type=signal.value_type,
            old_value=signal.old_value,
            new_value=signal.new_value,
        )
    
    def _get_identifying_content(self):
        return {
            "event_type": "parameter_change",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "parameter": self.parameter,
            "new_value": self.new_value,
        }

class ParameterSetChange(DomainEvent, tag=True):
    contract: EvmAddress
    parameters: Dict[DomainEventId,ParameterChange]
    
    def _get_identifying_content(self):
        return {
            "event_type": "parameters",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "parameters": sorted(self.parameters.keys()),
        }