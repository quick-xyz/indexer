# indexer/transform/patterns/registry.py
"""
Simple registry for transfer patterns
"""

from typing import Dict, Optional, List

from .base import TransferPattern
from .liquidity import LiquidityAddPattern, LiquidityRemovePattern
from .trading import DirectSwapPattern, RoutedSwapPattern, RoutedSwapWithWrapPattern


class TransferPatternRegistry:
    def __init__(self):
        self.patterns: Dict[str, TransferPattern] = {}
        self._register_default_patterns()
    
    def _register_default_patterns(self):
        """Register built-in patterns"""
        self.patterns.update({
            "liquidity_add": LiquidityAddPattern(),
            "liquidity_remove": LiquidityRemovePattern(),
            "direct_swap": DirectSwapPattern(),
            "routed_swap": RoutedSwapPattern(),
            "routed_swap_with_wrap": RoutedSwapWithWrapPattern()
        })
    
    def get_pattern(self, name: str) -> Optional[TransferPattern]:
        """Get specific pattern by name"""
        return self.patterns.get(name)
    
    def register_pattern(self, name: str, pattern: TransferPattern):
        """Register a new pattern"""
        self.patterns[name] = pattern
    
    def list_patterns(self) -> List[str]:
        """List all available pattern names"""
        return list(self.patterns.keys())