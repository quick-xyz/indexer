"""
Transformer module for converting decoded blockchain events to domain events.
"""

from .transformation_manager import TransformationManager
from .transformer_registry import registry

# Auto-initialize the registry when module is imported
registry.setup()

__all__ = ['TransformationManager', 'registry']