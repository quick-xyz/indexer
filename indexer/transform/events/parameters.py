from typing import Literal, Optional, List
from msgspec import Struct

from ...decode.model.evm import EvmAddress
from .base import DomainEvent

class Parameter(DomainEvent, tag=True):
    parameter: str
    value_type: Literal["address","string","int","bool"]
    new_value: Optional[any] = None
    old_value: Optional[any] = None

class Parameters(DomainEvent, tag=True):
    contract: EvmAddress
    parameters: List[Parameter]
    
    def _get_identifying_content(self):
        return {
            "event_type": "parameters",
            "tx_salt": self.tx_hash,
            "contract": self.contract,
            "parameters": [param.parameter for param in self.parameters],
        }