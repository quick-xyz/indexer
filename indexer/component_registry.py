from typing import Any, List, Optional


class ComponentRegistry:
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.components = {}
        return cls._instance
    
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