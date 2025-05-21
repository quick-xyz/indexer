from typing import Literal, Optional, List
from msgspec import Struct

from ...decode.model.evm import EvmAddress
from .base import DomainEvent

class Parameter(Struct, tag=True):
    parameter: EvmAddress
    value_type: Literal["address","string","int","bool"]
    new_value: Optional[any] = None
    old_value: Optional[any] = None

class Parameters(DomainEvent, tag=True):
    contract: EvmAddress
    parameters: List[Parameter]