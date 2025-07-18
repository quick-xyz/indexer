# indexer/core/container.py

from typing import TypeVar, Type, Callable, Set, Dict, Any, Optional
import inspect
import logging

from .logging import IndexerLogger, log_with_context

T = TypeVar('T')

class ContainerError(Exception):
    pass

class ServiceNotRegisteredException(ContainerError):
    pass

class CircularDependencyException(ContainerError):
    pass

class IndexerContainer:
    def __init__(self, config):
        self._config = config
        self._services: Dict[Type, tuple] = {}  # service_type -> (implementation, factory, is_singleton)
        self._instances: Dict[Type, Any] = {}  # service_type -> instance (for singletons)
        self._resolution_stack: Set[Type] = set()  # Track currently resolving services
        
        self._logger = IndexerLogger.get_logger('core.container')
        self._logger.debug("IndexerContainer initialized")
        
    def register_singleton(self, interface: Type[T], implementation: Type[T]) -> 'IndexerContainer':
        """Register a service that gets created once and reused"""
        self._validate_registration(interface, implementation)
        
        log_with_context(self._logger, logging.DEBUG, "Registering singleton service",
                        interface=interface.__name__,
                        implementation=implementation.__name__)
        
        self._services[interface] = (implementation, None, True)
        return self
        
    def register_transient(self, interface: Type[T], implementation: Type[T]) -> 'IndexerContainer':
        """Register a service that gets created fresh each time"""
        self._validate_registration(interface, implementation)
        
        log_with_context(self._logger, logging.DEBUG, "Registering transient service",
                        interface=interface.__name__,
                        implementation=implementation.__name__)
        
        self._services[interface] = (implementation, None, False)
        return self
        
    def register_factory(self, interface: Type[T], factory_func: Callable[['IndexerContainer'], T]) -> 'IndexerContainer':
        """Register a factory function (treated as singleton)"""
        if not callable(factory_func):
            raise ContainerError(f"Factory function for {interface.__name__} must be callable")
        
        log_with_context(self._logger, logging.DEBUG, "Registering factory service",
                        interface=interface.__name__,
                        factory_func=factory_func.__name__)
        
        self._services[interface] = (None, factory_func, True)
        return self

    def register_instance(self, interface: Type[T], instance: T) -> 'IndexerContainer':
        """Register an already-created instance"""
        if instance is None:
            raise ContainerError(f"Cannot register None instance for {interface.__name__}")
            
        log_with_context(self._logger, logging.DEBUG, "Registering instance",
                        interface=interface.__name__,
                        instance_type=type(instance).__name__)
        
        self._instances[interface] = instance
        self._services[interface] = (None, None, True)
        return self
        
    def get(self, service_type: Type[T]) -> T:
        """Get service instance, creating if necessary"""
        service_name = service_type.__name__
        
        if service_type in self._resolution_stack:
            circular_path = " -> ".join([t.__name__ for t in self._resolution_stack]) + f" -> {service_name}"
            log_with_context(self._logger, logging.ERROR, "Circular dependency detected",
                           service_type=service_name,
                           circular_path=circular_path)
            raise CircularDependencyException(f"Circular dependency detected: {circular_path}")
        
        if service_type not in self._services:
            log_with_context(self._logger, logging.ERROR, "Service not registered",
                           service_type=service_name)
            raise ServiceNotRegisteredException(f"Service {service_name} not registered")
            
        implementation, factory, is_singleton = self._services[service_type]
        
        if is_singleton and service_type in self._instances:
            log_with_context(self._logger, logging.DEBUG, "Returning cached singleton instance",
                           service_type=service_name)
            return self._instances[service_type]
            
        self._resolution_stack.add(service_type)
        
        try:
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
        
        try:
            sig = inspect.signature(implementation_type.__init__)
        except (ValueError, TypeError) as e:
            raise ContainerError(f"Cannot inspect constructor of {service_name}: {e}")
            
        kwargs = {}
        resolved_dependencies = []
        
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
                
            param_type = param.annotation
            
            if param_type == inspect.Parameter.empty:
                if param.default == inspect.Parameter.empty:
                    log_with_context(self._logger, logging.WARNING, 
                                   "Parameter without type annotation and no default value",
                                   implementation=service_name,
                                   parameter=param_name)
                continue
            
            try:
                dependency = self.get(param_type)
                kwargs[param_name] = dependency
                resolved_dependencies.append(param_name)
                
            except ServiceNotRegisteredException:
                if param.default != inspect.Parameter.empty:
                    log_with_context(self._logger, logging.DEBUG, 
                                   "Using default value for unregistered dependency",
                                   implementation=service_name,
                                   parameter=param_name)
                    continue
                else:
                    log_with_context(self._logger, logging.ERROR, 
                                   "Required dependency not registered",
                                   implementation=service_name,
                                   parameter=param_name,
                                   parameter_type=param_type.__name__)
                    raise
        
        log_with_context(self._logger, logging.DEBUG, "Dependencies resolved",
                        implementation=service_name,
                        resolved_dependencies=resolved_dependencies)
        
        try:
            instance = implementation_type(**kwargs)
            log_with_context(self._logger, logging.DEBUG, "Instance created successfully",
                           implementation=service_name,
                           instance_type=type(instance).__name__)
            return instance
            
        except Exception as e:
            log_with_context(self._logger, logging.ERROR, "Instance creation failed",
                           implementation=service_name,
                           error=str(e),
                           exception_type=type(e).__name__,
                           provided_kwargs=list(kwargs.keys()))
            raise

    def _validate_registration(self, interface: Type, implementation: Type):
        if interface is None:
            raise ContainerError("Interface type cannot be None")
        if implementation is None:
            raise ContainerError("Implementation type cannot be None")
        if not isinstance(interface, type):
            raise ContainerError(f"Interface must be a type, got {type(interface)}")
        if not isinstance(implementation, type):
            raise ContainerError(f"Implementation must be a type, got {type(implementation)}")

    def has_service(self, service_type: Type) -> bool:
        has_service = service_type in self._services
        log_with_context(self._logger, logging.DEBUG, "Service registration check",
                        service_type=service_type.__name__,
                        is_registered=has_service)
        return has_service
    
    def clear_cache(self, service_type: Optional[Type] = None):
        """Clear cached instances (useful for testing)"""
        if service_type:
            if service_type in self._instances:
                del self._instances[service_type]
                log_with_context(self._logger, logging.DEBUG, "Cleared cached instance",
                               service_type=service_type.__name__)
        else:
            cleared_count = len(self._instances)
            self._instances.clear()
            log_with_context(self._logger, logging.DEBUG, "Cleared all cached instances",
                           cleared_count=cleared_count)
    
    def get_service_info(self) -> dict:
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
        
        log_with_context(self._logger, logging.DEBUG, "Service info requested", **info)
        return info