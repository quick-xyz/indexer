"""
Transformer module for converting decoded blockchain events to domain events.
"""

from .manager import TransformationManager
from .registry.event_mappings import setup_registry

# Initialize the registry when module is imported
setup_registry()

__all__ = ['TransformationManager']