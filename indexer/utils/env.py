"""
Environment configuration for blockchain indexer.
"""
import os
import re
import logging
from pathlib import Path
from typing import Optional


# Try to import dotenv, with fallback
try:
    from dotenv import load_dotenv
    has_dotenv = True
except ImportError:
    has_dotenv = False

class IndexerEnvironment:
    """
    Environment manager for blockchain indexer.
    
    Handles loading environment variables and path resolution.
    """
    
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
        self.env_prefix = env_prefix
        self.logger = logging.getLogger("indexer.env")
        

        self._init_paths(project_root, indexer_root)
        
        # Load environment variables
        self._load_env_vars(env_file)
        
        # Validate environment
        self._validate_env()
        
        # Initialize database connection
        self.db_engine = None
        if self._validate_db_config():
            self._init_db_connection()
    
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
            'config_dir': self.indexer_root / 'config'
        }
        
        # Create directories if they don't exist
        for name, path in self.paths.items():
            path.mkdir(parents=True, exist_ok=True)
    
    def _load_env_vars(self, env_file=None):
        """Load environment variables from .env files."""
        if not has_dotenv:
            self.logger.warning("python-dotenv not installed, skipping .env file loading")
            return
            
        # Load from specific file if provided
        if env_file and Path(env_file).exists():
            load_dotenv(env_file)
            self.logger.info(f"Loaded environment variables from {env_file}")
            return
            
        # Otherwise try default locations
        env_files = [
            self.project_root / '.env',
            self.indexer_root / '.env'
        ]
        
        for file in env_files:
            if file.exists():
                load_dotenv(file)
                self.logger.info(f"Loaded environment variables from {file}")
    
    def _validate_env(self):
        """Validate required environment variables."""
        required_vars = [
            "STORAGE_TYPE",
            "DB_TYPE"
        ]
        
        missing = [var for var in required_vars if not self.get_env(var)]
        if missing:
            self.logger.warning(f"Missing recommended environment variables: {', '.join(missing)}")
    
    def _validate_db_config(self):
        """Validate database configuration."""
        db_type = self.get_env("DB_TYPE", "sqlite").lower()
        
        if db_type == "sqlite":
            self.logger.info("Using SQLite database")
            return True
        
        elif db_type == "postgresql":
            # Check PostgreSQL config
            db_vars = ['DB_USER', 'DB_PASS', 'DB_NAME', 'DB_HOST']
            db_present = [var for var in db_vars if self.get_env(var)]
            
            if len(db_present) < len(db_vars):
                # Incomplete PostgreSQL config
                missing_db = [var for var in db_vars if not self.get_env(var)]
                self.logger.warning(f"Incomplete PostgreSQL configuration. Missing: {', '.join(missing_db)}")
                self.logger.warning("Falling back to SQLite database")
                return True
            
            # Complete PostgreSQL config
            self.logger.info("Using PostgreSQL database")
            return True
        
        else:
            self.logger.warning(f"Unsupported database type: {db_type}")
            self.logger.warning("Falling back to SQLite database")
            return True
    
    def _init_db_connection(self):
        """Initialize database connection and verify it works."""
        # Only import SQLAlchemy if needed
        try:
            from sqlalchemy import create_engine, text
            from sqlalchemy.exc import SQLAlchemyError
        except ImportError:
            self.logger.warning("SQLAlchemy not installed, skipping database initialization")
            return False
            
        db_url = self.get_db_url()
        # Mask password in logs
        masked_url = db_url
        if ":" in db_url and "@" in db_url:
            parts = db_url.split(":")
            if len(parts) > 2:
                masked_url = f"{parts[0]}:{parts[1]}:****@{db_url.split('@')[1]}"
        
        self.logger.info(f"Initializing database connection to {masked_url}")
        
        try:
            # Create engine with reasonable defaults
            self.db_engine = create_engine(
                db_url,
                pool_pre_ping=True,  # Verify connection before using
                pool_recycle=300,    # Recycle connections after 5 minutes
                pool_size=5,         # Maintain a pool of 5 connections
                max_overflow=10      # Allow up to 10 additional connections
            )
            
            # Test connection
            with self.db_engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                if result == 1:
                    self.logger.info("Database connection verified")
                    return True
                else:
                    self.logger.warning("Database connection test returned unexpected result")
                    return False
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            self.logger.warning("Application may not function correctly without database access")
            self.db_engine = None
            return False
    
    def get_env(self, name, default=None):
        """
        Get environment variable with prefix.
        
        Args:
            name: Variable name (without prefix)
            default: Default value if not found
            
        Returns:
            Environment variable value or default
        """
        return os.getenv(f"{self.env_prefix}{name}", default)
    
    def get_path(self, name):
        """Get path by name."""
        return self.paths.get(name)
    
    def is_development(self):
        """Check if running in development environment."""
        return self.get_env("ENVIRONMENT", "").lower() == "development"
    
    def get_log_level(self):
        """Get configured log level."""
        level = self.get_env("LOG_LEVEL", "INFO").upper()
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        return level if level in valid_levels else "INFO"
    
    def get_db_url(self):
        """Get database connection URL from environment."""
        db_type = self.get_env("DB_TYPE", "sqlite").lower()
        
        if db_type == "sqlite":
            # Use SQLite for local development
            data_dir = self.paths['data_dir']
            sqlite_path = self.get_env("DB_SQLITE_PATH", str(data_dir / "indexer.db"))
            return f"sqlite:///{sqlite_path}"
        
        elif db_type == "postgresql":
            # PostgreSQL connection
            db_user = self.get_env("DB_USER")
            db_pass = self.get_env("DB_PASS")
            db_name = self.get_env("DB_NAME")
            db_host = self.get_env("DB_HOST")
            db_port = self.get_env("DB_PORT", "5432")
            
            if not all([db_user, db_pass, db_name, db_host]):
                # Development fallback - SQLite
                data_dir = self.paths['data_dir']
                return f"sqlite:///{data_dir}/indexer.db"
            
            return f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        
        else:
            # Default fallback
            data_dir = self.paths['data_dir']
            return f"sqlite:///{data_dir}/indexer.db"
    
    def get_storage_type(self):
        """Get storage type."""
        return self.get_env("STORAGE_TYPE", "local")
    
    def get_storage_config(self):
        """Get storage configuration."""
        storage_type = self.get_storage_type()
        
        if storage_type == "gcs":
            return {
                "storage_type": "gcs",
                "bucket_name": self.get_env("GCS_BUCKET_NAME"),
                "credentials_path": self.get_env("GCS_CREDENTIALS_PATH"),
                "raw_prefix": self.get_env("GCS_RAW_PREFIX", "raw/"),
                "decoded_prefix": self.get_env("GCS_DECODED_PREFIX", "decoded/")
            }
        else:
            # Default to local
            return {
                "storage_type": "local",
                "local_dir": str(self.paths['data_dir']),
                "raw_prefix": self.get_env("LOCAL_RAW_PREFIX", "raw/"),
                "decoded_prefix": self.get_env("LOCAL_DECODED_PREFIX", "decoded/")
            }
    
    def extract_block_number(self, path):
        """Extract block number from a block path."""
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
    
    def register_component(self, name, component):
        """Register a component for shared use."""
        self._components[name] = component
        
    def get_component(self, name):
        """Get a registered component."""
        return self._components.get(name)
    
    def verify_database(self):
        """
        Verify database connection and schema.
        Returns True if database is ready, False otherwise.
        """
        try:
            # Only import SQLAlchemy if needed
            from sqlalchemy import text
        except ImportError:
            self.logger.warning("SQLAlchemy not installed, skipping database verification")
            return False
            
        if not self.db_engine:
            success = self._init_db_connection()
            if not success:
                return False
        
        # Just check if we can execute a simple query
        try:
            with self.db_engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                return result == 1
        except Exception as e:
            self.logger.error(f"Database verification failed: {e}")
            return False

# Create singleton instance
env = IndexerEnvironment()