from typing import TypeVar, Type, Callable
import inspect

T = TypeVar('T')

class IndexerContainer:
    def __init__(self, config):
        self._config = config
        self._services = {}  # service_type -> (implementation, factory, is_singleton)
        self._instances = {}  # service_type -> instance (for singletons)
        
    def register_singleton(self, interface: Type[T], implementation: Type[T]) -> 'IndexerContainer':
        """Register a service that gets created once and reused"""
        self._services[interface] = (implementation, None, True)
        return self
        
    def register_transient(self, interface: Type[T], implementation: Type[T]) -> 'IndexerContainer':
        """Register a service that gets created fresh each time"""
        self._services[interface] = (implementation, None, False)
        return self
        
    def register_factory(self, interface: Type[T], factory_func: Callable[['IndexerContainer'], T]) -> 'IndexerContainer':
        """Register a factory function (treated as singleton)"""
        self._services[interface] = (None, factory_func, True)
        return self
        
    def get(self, service_type: Type[T]) -> T:
        """Get service instance, creating if necessary"""
        if service_type not in self._services:
            raise ValueError(f"Service {service_type.__name__} not registered")
            
        implementation, factory, is_singleton = self._services[service_type]
        
        # Return cached instance if singleton
        if is_singleton and service_type in self._instances:
            return self._instances[service_type]
            
        # Create new instance
        if factory:
            instance = factory(self)
        else:
            instance = self._create_instance(implementation)
            
        # Cache if singleton
        if is_singleton:
            self._instances[service_type] = instance
            
        return instance
        
    def _create_instance(self, implementation_type: Type):
        """Create instance with dependency injection"""
        # Get constructor signature
        sig = inspect.signature(implementation_type.__init__)
        kwargs = {}
        
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
                
            # Try to resolve dependency from container
            param_type = param.annotation
            if param_type != inspect.Parameter.empty and param_type in self._services:
                kwargs[param_name] = self.get(param_type)
            elif param_name == 'config':
                # Special case for config
                kwargs[param_name] = self._config
                
        return implementation_type(**kwargs)
        
    def has_service(self, service_type: Type) -> bool:
        return service_type in self._services