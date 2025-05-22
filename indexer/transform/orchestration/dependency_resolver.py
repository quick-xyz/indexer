
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict, deque
import logging

logger = logging.getLogger(__name__)


@dataclass
class TransformationDependency:
    """Represents a dependency between transformations."""
    source_transformation: str  # The transformation that must complete first
    target_transformation: str  # The transformation that depends on the source
    dependency_type: str = "requires"  # Type of dependency


class DependencyResolver:
    """Resolves dependencies between transformations and determines execution order."""
    
    def __init__(self):
        self._dependencies: Dict[str, List[TransformationDependency]] = defaultdict(list)
        self._reverse_dependencies: Dict[str, List[str]] = defaultdict(list)
    
    def add_dependency(self, dependency: TransformationDependency):
        """Add a dependency relationship between transformations."""
        source = dependency.source_transformation
        target = dependency.target_transformation
        
        self._dependencies[source].append(dependency)
        self._reverse_dependencies[target].append(source)
        
        # Validate no circular dependencies
        if self._has_circular_dependency(target, source):
            raise ValueError(f"Circular dependency detected: {source} -> {target}")
    
    def add_transformation_dependency(self, source: str, target: str, dependency_type: str = "requires"):
        """Convenience method to add a simple dependency."""
        dependency = TransformationDependency(
            source_transformation=source,
            target_transformation=target,
            dependency_type=dependency_type
        )
        self.add_dependency(dependency)
    
    def get_execution_order(self, transformations: List[str]) -> List[List[str]]:
        """
        Get the execution order for transformations, respecting dependencies.
        Returns a list of batches that can be executed in parallel.
        """
        # Filter to only include transformations we have
        available_transformations = set(transformations)
        
        # Build dependency graph for available transformations
        graph = {}
        in_degree = {}
        
        for transformation in available_transformations:
            graph[transformation] = []
            in_degree[transformation] = 0
        
        # Add edges from dependencies
        for transformation in available_transformations:
            dependencies = self._reverse_dependencies.get(transformation, [])
            for dep in dependencies:
                if dep in available_transformations:
                    graph[dep].append(transformation)
                    in_degree[transformation] += 1
        
        # Topological sort with level detection
        execution_batches = []
        queue = deque([t for t in available_transformations if in_degree[t] == 0])
        
        while queue:
            # Current batch - all transform