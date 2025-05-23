# transformer/registry/transformer_registry.py

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
    """Simple transformation rule."""
    source_events: List[str]  # Event names that trigger this transformation
    target_event: str  # Domain event name to create
    transformation_type: TransformationType
    contract_address: str = None  # None means applies to all contracts
    priority: int = 0


class TransformerRegistry:
    def __init__(self):
        self._contract_transformers: Dict[str, Type] = {}
        self._transformation_rules: List[TransformationRule] = []
        self._event_triggers: Dict[str, List[TransformationRule]] = {}
    
    def register_contract_transformer(self, contract_address: str, transformer_class: Type):
        """Register a transformer class for a contract."""
        self._contract_transformers[contract_address.lower()] = transformer_class
    
    def register_transformation_rule(self, rule: TransformationRule):
        """Register a transformation rule."""
        self._transformation_rules.append(rule)
        
        # Index by event names
        for source_event in rule.source_events:
            if source_event not in self._event_triggers:
                self._event_triggers[source_event] = []
            self._event_triggers[source_event].append(rule)
    
    def get_contract_transformer(self, contract_address: str) -> Type:
        """Get transformer class for a contract."""
        return self._contract_transformers.get(contract_address.lower())
    
    def get_triggered_rules(self, event_name: str) -> List[TransformationRule]:
        """Get transformation rules triggered by an event."""
        return self._event_triggers.get(event_name, [])


# Global registry instance
registry = TransformerRegistry()