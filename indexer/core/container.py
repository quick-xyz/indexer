# Update indexer/core/container.py - Add circular dependency detection

from typing import TypeVar, Type, Callable, Set
import inspect
import logging

from .logging_config import IndexerLogger, log_with_context

T = TypeVar('T')

class IndexerContainer:
    def __init__(self, config):
        self._config = config
        self._services = {}  # service_type -> (implementation, factory, is_singleton)
        self._instances = {}  # service_type -> instance (for singletons)
        self._resolution_stack: Set[Type] = set()  # Track currently resolving services
        
        # Get logger for container operations
        self._logger = IndexerLogger.get_logger('core.container')
        self._logger.debug("IndexerContainer initialized")
        
    def register_singleton(self, interface: Type[T], implementation: Type[T]) -> 'IndexerContainer':
        """Register a service that gets created once and reused"""
        log_with_context(self._logger, logging.DEBUG, "Registering singleton service",
                        interface=interface.__name__,
                        implementation=implementation.__name__)
        
        self._services[interface] = (implementation, None, True)
        return self
        
    def register_transient(self, interface: Type[T], implementation: Type[T]) -> 'IndexerContainer':
        """Register a service that gets created fresh each time"""
        log_with_context(self._logger, logging.DEBUG, "Registering transient service",
                        interface=interface.__name__,
                        implementation=implementation.__name__)
        
        self._services[interface] = (implementation, None, False)
        return self
        
    def register_factory(self, interface: Type[T], factory_func: Callable[['IndexerContainer'], T]) -> 'IndexerContainer':
        """Register a factory function (treated as singleton)"""
        log_with_context(self._logger, logging.DEBUG, "Registering factory service",
                        interface=interface.__name__,
                        factory_func=factory_func.__name__)
        
        self._services[interface] = (None, factory_func, True)
        return self
        
    def get(self, service_type: Type[T]) -> T:
        """Get service instance, creating if necessary"""
        service_name = service_type.__name__
        
        # Check for circular dependency
        if service_type in self._resolution_stack:
            circular_path = " -> ".join([t.__name__ for t in self._resolution_stack]) + f" -> {service_name}"
            log_with_context(self._logger, logging.ERROR, "Circular dependency detected",
                           service_type=service_name,
                           circular_path=circular_path)
            raise ValueError(f"Circular dependency detected: {circular_path}")
        
        if service_type not in self._services:
            log_with_context(self._logger, logging.ERROR, "Service not registered",
                           service_type=service_name)
            raise ValueError(f"Service {service_name} not registered")
            
        implementation, factory, is_singleton = self._services[service_type]
        
        # Return cached instance if singleton
        if is_singleton and service_type in self._instances:
            log_with_context(self._logger, logging.DEBUG, "Returning cached singleton instance",
                           service_type=service_name)
            return self._instances[service_type]
            
        # Add to resolution stack for circular dependency detection
        self._resolution_stack.add(service_type)
        
        try:
            # Create new instance
            log_with_context(self._logger, logging.DEBUG, "Creating new service instance",
                            service_type=service_name,
                            is_singleton=is_singleton,
                            has_factory=bool(factory))
            
            if factory:
                log_with_context(self._logger, logging.DEBUG, "Using factory function",
                               service_type=service_name,
                               factory_func=factory.__name__)
                instance = factory(self)
            else:
                log_with_context(self._logger, logging.DEBUG, "Using dependency injection",
                                service_type=service_name,
                                implementation=implementation.__name__)
                instance = self._create_instance(implementation)
                
            # Cache if singleton
            if is_singleton:
                log_with_context(self._logger, logging.DEBUG, "Caching singleton instance",
                               service_type=service_name)
                self._instances[service_type] = instance
            
            log_with_context(self._logger, logging.INFO, "Service instance created successfully",
                           service_type=service_name,
                           instance_type=type(instance).__name__)
            
            return instance
            
        except Exception as e:
            log_with_context(self._logger, logging.ERROR, "Failed to create service instance",
                           service_type=service_name,
                           error=str(e),
                           exception_type=type(e).__name__)
            raise
        finally:
            # Always remove from resolution stack
            self._resolution_stack.discard(service_type)
        
    def _create_instance(self, implementation_type: Type):
        """Create instance with dependency injection"""
        service_name = implementation_type.__name__
        
        log_with_context(self._logger, logging.DEBUG, "Starting dependency injection",
                        implementation=service_name)
        
        # Get constructor signature
        sig = inspect.signature(implementation_type.__init__)
        kwargs = {}
        resolved_dependencies = []
        
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
                
            # Try to resolve dependency from container
            param_type = param.annotation
            if param_type != inspect.Parameter.empty and param_type in self._services:
                log_with_context(self._logger, logging.DEBUG, "Resolving dependency from container",
                               implementation=service_name,
                               dependency=param_name,
                               dependency_type=param_type.__name__)
                kwargs[param_name] = self.get(param_type)
                resolved_dependencies.append(f"{param_name}:{param_type.__name__}")
            elif param_name == 'config':
                # Special case for config
                log_with_context(self._logger, logging.DEBUG, "Injecting config dependency",
                               implementation=service_name)
                kwargs[param_name] = self._config
                resolved_dependencies.append("config:IndexerConfig")
            else:
                log_with_context(self._logger, logging.DEBUG, "Skipping unresolvable parameter",
                               implementation=service_name,
                               parameter=param_name,
                               parameter_type=str(param_type))
        
        log_with_context(self._logger, logging.DEBUG, "Dependency injection completed",
                        implementation=service_name,
                        resolved_count=len(resolved_dependencies),
                        dependencies=resolved_dependencies)
        
        try:
            instance = implementation_type(**kwargs)
            log_with_context(self._logger, logging.DEBUG, "Instance created via dependency injection",
                           implementation=service_name,
                           instance_type=type(instance).__name__)
            return instance
        except Exception as e:
            log_with_context(self._logger, logging.ERROR, "Dependency injection failed",
                           implementation=service_name,
                           error=str(e),
                           exception_type=type(e).__name__,
                           provided_kwargs=list(kwargs.keys()))
            raise
        
    def has_service(self, service_type: Type) -> bool:
        """Check if a service is registered"""
        has_service = service_type in self._services
        log_with_context(self._logger, logging.DEBUG, "Service registration check",
                        service_type=service_type.__name__,
                        is_registered=has_service)
        return has_service
    
    def get_service_info(self) -> dict:
        """Get information about registered services"""
        info = {
            'registered_services': len(self._services),
            'cached_instances': len(self._instances),
            'services': {}
        }
        
        for service_type, (implementation, factory, is_singleton) in self._services.items():
            info['services'][service_type.__name__] = {
                'implementation': implementation.__name__ if implementation else 'factory',
                'factory': factory.__name__ if factory else None,
                'is_singleton': is_singleton,
                'is_cached': service_type in self._instances
            }
        
        log_with_context(self._logger, logging.DEBUG, "Service info requested",
                        **info)
        
        return info