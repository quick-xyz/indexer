# api/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from pathlib import Path

from indexer import create_indexer
from indexer.database.repository import RepositoryManager
from indexer.core.logging_config import IndexerLogger, log_with_context

from .routers import trades, liquidity, positions
from .dependencies import set_dependencies

# Global variables for dependency injection
indexer_container = None
repository_manager = None
logger = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global indexer_container, repository_manager, logger
    
    # Startup
    try:
        # Create indexer container using existing config
        config_path = Path(__file__).parent.parent / "config" / "config.json"
        indexer_container = create_indexer(config_path=str(config_path))
        
        # Get repository manager from container
        repository_manager = indexer_container.get(RepositoryManager)
        
        # Set up dependencies for routers
        set_dependencies(repository_manager)
        
        logger = IndexerLogger.get_logger('api.main')
        
        log_with_context(logger, logging.INFO, "API startup completed",
                        config_path=str(config_path))
        
    except Exception as e:
        print(f"Failed to initialize API: {e}")
        raise
    
    # Application runs here
    yield
    
    # Shutdown
    if logger:
        logger.info("API shutting down")

app = FastAPI(
    title="Progekt Indexer API",
    description="REST API for blockchain indexer data",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trades.router, prefix="/trades", tags=["trades"])
app.include_router(liquidity.router, prefix="/liquidity", tags=["liquidity"])
app.include_router(positions.router, prefix="/positions", tags=["positions"])

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "message": "Indexer API is running",
        "database_connected": repository_manager is not None
    }

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Indexer API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "trades": "/trades",
            "liquidity": "/liquidity", 
            "positions": "/positions",
            "docs": "/docs"
        }
    }