# transformer/registry/transformer_registry.py

from typing import Dict, List, Type, Optional
from dataclasses import dataclass
from enum import Enum


class TransformationType(Enum):
    ONE_TO_ONE = "1:1"
    MANY_TO_ONE = "M:1"
    ONE_TO_MANY = "1:M"


@dataclass
class TransformationRule:
    source_events: List[str]  # Primary business event names (e.g., "Mint", "Swap")
    target_event: str  # Domain event name to create
    transformation_type: TransformationType
    contract_address: str = None  # None means applies to all contracts
    requires_transfers: bool = True  # Whether this rule needs transfer correlation
    transfer_validation: bool = True  # Whether to validate transfer amounts
    priority: int = 0


@dataclass
class ContractMapping:
    """Mapping for a contract - transformer + business event rules."""
    transformer_class: Type
    business_event_rules: List[TransformationRule]  # Rules for non-transfer events


class TransformerRegistry:
    """Registry focused on business events that correlate with transfers."""
    
    def __init__(self):
        self._contract_transformers: Dict[str, Type] = {}
        self._transformation_rules: List[TransformationRule] = []
        # Transfer events are handled specially - not part of contract-specific rules
        self._transfer_events = {"Transfer", "TransferBatch"}  # Common transfer event names
    
    def register_contract_transformer(self, contract_address: str, transformer_class: Type):
        """Register a transformer class for a contract."""
        self._contract_transformers[contract_address.lower()] = transformer_class
    
    def register_transformation_rule(self, rule: TransformationRule):
        """Register a transformation rule for business events."""
        self._transformation_rules.append(rule)
    
    def get_contract_mapping(self, contract_address: str) -> Optional[ContractMapping]:
        """Get mapping for business events (non-transfers) for a contract."""
        transformer_class = self._contract_transformers.get(contract_address.lower())
        if not transformer_class:
            return None
        
        # Get rules that apply to this contract and are for business events
        business_rules = [
            rule for rule in self._transformation_rules
            if (rule.contract_address is None or 
                rule.contract_address.lower() == contract_address.lower()) and
            not any(event in self._transfer_events for event in rule.source_events)
        ]
        
        return ContractMapping(transformer_class, business_rules)
    
    def is_transfer_event(self, event_name: str) -> bool:
        return event_name in self._transfer_events
    
    def add_transfer_event_type(self, event_name: str):
        self._transfer_events.add(event_name)


registry = TransformerRegistry()