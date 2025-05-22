"""
Registry components for transformation configuration.
"""

from .transformer_registry import registry, TransformationRule, TransformationType
from .event_mappings import setup_registry

__all__ = ['registry', 'TransformationRule', 'TransformationType', 'setup_registry']

