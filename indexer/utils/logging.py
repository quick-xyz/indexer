"""
Legacy logging utilities module.

This module provides backward compatibility with the old logging system.
New code should use the utils.logger module directly.
"""
import logging
import os
from datetime import datetime

from .logger import get_logger, setup_logger

# Re-export the logger creation function for backward compatibility
__all__ = ['setup_logger', 'get_logger']