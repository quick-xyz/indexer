
from typing import Dict, List, Type, Optional, Set
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class TransformationType(Enum):
    ONE_TO_ONE = "1:1"
    MANY_TO_ONE = "M:1"
    ONE_TO_MANY = "1:M"


@dataclass
class TransformationRule:
    """Defines how decoded events map to domain events."""
    source_events: List[str]  # Decoded event names that trigger this transformation
    target_event: str  # Domain event name to create
    transformation_type: TransformationType
    contract_address: Optional[str] = None  # None means applies to all contracts
    priority: int = 0  # Higher priority rules are processed first
    requires_all_sources: bool = True  # For M:1, whether all source events are required


class TransformerRegistry:
    """Registry for transformation rules and contract transformer mappings."""
    
    def __init__(self):
        self._contract_transformers: Dict[str, Type] = {}
        self._transformation_rules: List[TransformationRule] = []
        self._event_triggers: Dict[str, List[TransformationRule]] = {}
    
    def register_contract_transformer(self, contract_address: str, transformer_class: Type):
        """Register a transformer class for a specific contract."""
        self._contract_transformers[contract_address.lower()] = transformer_class
    
    def register_transformation_rule(self, rule: TransformationRule):
        """Register a transformation rule."""
        self._transformation_rules.append(rule)
        
        # Index by source events for quick lookup
        for source_event in rule.source_events:
            if source_event not in self._event_triggers:
                self._event_triggers[source_event] = []
            self._event_triggers[source_event].append(rule)
    
    def get_contract_transformer(self, contract_address: str) -> Optional[Type]:
        """Get transformer class for a contract address."""
        return self._contract_transformers.get(contract_address.lower())
    
    def get_triggered_rules(self, event_name: str) -> List[TransformationRule]:
        """Get all transformation rules triggered by an event."""
        return self._event_triggers.get(event_name, [])
    
    def get_rules_for_contract(self, contract_address: str) -> List[TransformationRule]:
        """Get all transformation rules applicable to a contract."""
        return [
            rule for rule in self._transformation_rules
            if rule.contract_address is None or rule.contract_address.lower() == contract_address.lower()
        ]
    
    def get_all_source_events(self) -> Set[str]:
        """Get all decoded event names that can trigger transformations."""
        return set(self._event_triggers.keys())


# Global registry instance
registry = TransformerRegistry()