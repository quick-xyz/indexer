from typing import Any, Dict, List, Optional


class ComponentRegistry:
    
    _instance = None
    
    def __init__(self):
        """Initialize an empty registry."""
        self.components: Dict[str, Any] = {}
    
    def register(self, name: str, component: Any) -> None:
        self.components[name] = component
    
    def get(self, name: str) -> Optional[Any]:
        return self.components.get(name)
    
    def clear(self, names: Optional[List[str]] = None) -> None:
        if names is None:
            self.components.clear()
        else:
            for name in names:
                if name in self.components:
                    del self.components[name]
    
    def has(self, name: str) -> bool:
        return name in self.components

registry = ComponentRegistry()