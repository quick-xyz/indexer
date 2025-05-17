"""
Environment utilities for blockchain indexer.

This module provides utilities for interacting with the environment,
including path management, environment variables, and component caching.
"""
import os
import re
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Type, TypeVar, List, Set

# Try to import dotenv, with fallback
try:
    from dotenv import load_dotenv
    has_dotenv = True
except ImportError:
    has_dotenv = False

T = TypeVar('T')

class IndexerEnvironment:
    """
    Environment manager for blockchain indexer.
    
    Handles:
    - Path resolution
    - Component caching
    - Environment detection
    - .env file loading
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, 
                project_root: Optional[Path] = None,
                indexer_root: Optional[Path] = None,
                env_file: Optional[str] = None,
                env_prefix: str = "INDEXER_"):
        """
        Initialize environment manager.
        
        Args:
            project_root: Path to project root directory
            indexer_root: Path to indexer root directory
            env_file: Path to .env file
            env_prefix: Prefix for environment variables
        """
        # Only initialize once
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.env_prefix = env_prefix
        self.logger = logging.getLogger("indexer.env")
        
        # Initialize paths
        self._init_paths(project_root, indexer_root)
        
        # Load environment variables
        self._load_env_vars(env_file)
        
        # Initialize component registry
        self._components = {}
        
        self._initialized = True
    
    def _init_paths(self, project_root=None, indexer_root=None):
        """Initialize directory paths."""
        # Resolve directories
        if indexer_root:
            self.indexer_root = Path(indexer_root).resolve()
        else:
            # Try to find indexer root based on this file's location
            self.current_dir = Path(__file__).resolve()
            self.indexer_root = self.current_dir.parents[2]  # indexer/utils/env.py -> indexer
        
        if project_root:
            self.project_root = Path(project_root).resolve()
        else:
            # Default to parent of indexer root
            self.project_root = self.indexer_root.parent
        
        # Set up paths dictionary
        self.paths = {
            'project_root': self.project_root,
            'indexer_root': self.indexer_root,
            'data_dir': self.project_root / 'data',
            'config_dir': self.indexer_root / 'config',
            'storage_dir': self.project_root / 'storage',
            'cache_dir': self.project_root / 'cache',
            'tmp_dir': self.project_root / 'tmp',
            'log_dir': self.project_root / 'logs'
        }
        
        # Create directories if they don't exist
        for name, path in self.paths.items():
            if name not in ('project_root', 'indexer_root'):  # Don't create root dirs
                path.mkdir(parents=True, exist_ok=True)
    
    def _load_env_vars(self, env_file=None):
        """Load environment variables from .env file."""
        if has_dotenv:
            if env_file and os.path.exists(env_file):
                load_dotenv(env_file)
                self.logger.info(f"Loaded environment variables from {env_file}")
            else:
                # Try to find .env file in standard locations
                env_paths = [
                    self.project_root / '.env',
                    self.indexer_root / '.env',
                    Path('.env')
                ]
                
                for path in env_paths:
                    if path.exists():
                        load_dotenv(path)
                        self.logger.info(f"Loaded environment variables from {path}")
                        break
        else:
            self.logger.debug("python-dotenv not installed, skipping .env file loading")
    
    def get_env(self, name: str, default: Any = None) -> Any:
        """
        Get environment variable with prefix.
        
        Args:
            name: Variable name (without prefix)
            default: Default value if not found
            
        Returns:
            Environment variable value or default
        """
        return os.getenv(f"{self.env_prefix}{name}", default)
    
    def get_path(self, name: str) -> Optional[Path]:
        """
        Get path by name.
        
        Args:
            name: Path name
            
        Returns:
            Path or None if not found
        """
        return self.paths.get(name)
    
    def is_development(self) -> bool:
        """
        Check if running in development environment.
        
        Returns:
            True if in development environment, False otherwise
        """
        return self.get_env("ENVIRONMENT", "").lower() == "development"
    
    def is_production(self) -> bool:
        """
        Check if running in production environment.
        
        Returns:
            True if in production environment, False otherwise
        """
        return self.get_env("ENVIRONMENT", "").lower() == "production"
    
    def is_test(self) -> bool:
        """
        Check if running in test environment.
        
        Returns:
            True if in test environment, False otherwise
        """
        return self.get_env("ENVIRONMENT", "").lower() == "test"
        
    def register_component(self, key: str, component: Any) -> None:
        """
        Register a component in the registry.
        
        Args:
            key: Component key
            component: Component instance
        """
        self._components[key] = component
        
    def get_component(self, key: str) -> Optional[Any]:
        """
        Get a component from the registry.
        
        Args:
            key: Component key
            
        Returns:
            Component instance or None if not found
        """
        return self._components.get(key)
    
    def get_components_by_type(self, component_type: Type[T]) -> List[T]:
        """
        Get all components of a specific type.
        
        Args:
            component_type: Component type
            
        Returns:
            List of components matching the type
        """
        return [c for c in self._components.values() if isinstance(c, component_type)]
    
    def clear_components(self, keys: Optional[Set[str]] = None) -> None:
        """
        Clear components from the registry.
        
        Args:
            keys: Set of keys to clear (None for all)
        """
        if keys is None:
            self._components.clear()
        else:
            for key in keys:
                if key in self._components:
                    del self._components[key]
    
    def extract_block_number(self, path: str) -> int:
        """
        Extract block number from a block path.
        
        Args:
            path: Path to block file
            
        Returns:
            Block number
            
        Raises:
            ValueError: If block number cannot be extracted
        """
        try:
            # Get filename
            filename = Path(path).name
            
            # Try common patterns in order of specificity
            
            # Pattern 1: Standard block_{number}.json
            match = re.search(r"block_(\d+)\.json", filename)
            if match:
                return int(match.group(1))
                
            # Pattern 2: Just the number itself (for simpler formats)
            match = re.search(r"(\d+)\.json$", filename)
            if match:
                return int(match.group(1))
                
            # Last resort: Try to find any sequence of digits in the filename
            match = re.search(r"(\d+)", filename)
            if match:
                return int(match.group(1))
                
        except Exception as e:
            self.logger.error(f"Failed to extract block number from {path}: {e}")
        
        raise ValueError(f"Could not extract block number from path: {path}")

# Create singleton instance
env = IndexerEnvironment()