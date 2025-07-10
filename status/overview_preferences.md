# Blockchain Indexer Project - Overview & Preferences

## Project Overview

This is a comprehensive blockchain token indexer with a modular architecture designed to handle token-specific economic activity. The system follows a pricing and valuation architecture for web application support.

### Core Architecture

**Indexing Pipeline:**
- RPC → Decode → Transform → Storage (GCS + PostgreSQL)
- Signal-based transformation system converting decoded logs into domain events
- Multi-worker coordination with database job queues

**Service Architecture:**
- **Pricing Service**: Canonical price calculation (1-minute schedule) + Direct pricing for swaps/trades
- **Calculation Service**: Materialized views for valuations (5-minute schedule) 
- **Aggregation Service**: Summary metrics and time-series (15-minute schedule)

**Dual Database Strategy:**
- **Shared Database** (`indexer_shared`): Chain-level data and configuration shared across all indexers
  - Configuration: models, contracts, tokens, sources, addresses
  - Chain-level pricing: block_prices, periods, pool_pricing_configs
- **Indexer Database** (per model, e.g., `blub_test`): Model-specific indexing data
  - Processing state: transaction_processing, block_processing, processing_jobs
  - Domain events: trades, pool_swaps, positions, transfers, liquidity, rewards
  - Pricing details: pool_swap_details, trade_details, event_details
- Database-driven configuration system with dependency injection
- Read replicas for API layer

**Dependency Injection Pattern:**
- All services use dependency injection container
- Services receive dependencies via constructor injection
- Configuration loaded from shared database
- Clear separation between infrastructure services (use shared DB) and indexer services (use model DB)
- Container manages database connections, repositories, and service lifecycles

### Key Components

1. **Pipeline System**: IndexingPipeline (individual blocks) + BatchPipeline (orchestrator)
2. **Transform System**: Signal-based architecture with transformer registry
3. **Database Module**: Dual database with appropriate table placement
4. **Configuration System**: Database-driven with dependency injection
5. **Storage**: GCS for stateful JSON + PostgreSQL for queryable events
6. **Pricing System**: Block-level AVAX prices + configurable pool pricing strategies + direct pricing
7. **Migration System**: Alembic for shared database + templates for model databases

### Technology Stack
- **Language**: Python with msgspec for data structures
- **Database**: PostgreSQL with SQLAlchemy (dual database architecture)
- **Storage**: Google Cloud Storage
- **RPC**: QuickNode for Avalanche mainnet
- **Architecture**: Dependency injection container pattern
- **Migrations**: Alembic with custom type support

## Recent Major Enhancements

### **Complete Migration System Implementation**
- **Dual Database Migrations**: Separate strategies for shared (Alembic) vs model (templates) databases
- **Custom Type Support**: Automatic handling of `EvmAddressType`, `EvmHashType`, `DomainEventIdType`
- **Development Workflow**: Easy reset, recreation, and status checking
- **CLI Integration**: Complete management interface for all migration operations
- **Troubleshooting**: Comprehensive documentation and fallback procedures

### **Complete Direct Pricing Implementation**
- **Detail Tables**: Separate pricing tables (`pool_swap_details`, `trade_details`, `event_details`) 
- **Dual Denominations**: Every event gets both USD and AVAX valuations
- **Method Tracking**: DIRECT_AVAX, DIRECT_USD, GLOBAL, ERROR pricing methods
- **Volume Weighting**: Trade pricing aggregates from constituent swaps
- **Comprehensive CLI**: Full management interface for all pricing operations

### **Enhanced Configuration System**
- **Separated Configuration**: Split into shared (`shared_v1_0.yaml`) vs model (`blub_test_v1_0.yaml`) files
- **Global Defaults + Overrides**: Contract pricing defaults with model-specific pool configurations
- **Import/Export CLI**: Complete configuration management with validation
- **Database Integration**: Configuration successfully imported and validated

### **Enhanced Service Architecture**
- **PricingService**: Now handles swap and trade direct pricing + existing period/block price functionality
- **Repository Layer**: Enhanced with bulk operations, eligibility checks, method statistics
- **CLI Interface**: Complete pricing management with monitoring and validation
- **Error Handling**: Graceful fallback to global pricing for complex cases

## Current System Status

### **✅ FULLY OPERATIONAL SYSTEMS**

1. **Migration System**: 
   - Dual database migrations with custom type support
   - Development utilities (reset, status, schema generation)
   - Comprehensive documentation and troubleshooting guides

2. **Configuration Management**:
   - Separated shared/model configuration files
   - Import/export with validation
   - Successfully loaded into databases

3. **Database Architecture**:
   - Shared database with infrastructure tables
   - Model database with event/processing tables
   - Proper table separation and relationships

4. **Pricing System**:
   - Direct pricing implementation complete
   - Pool pricing configuration system
   - CLI management and monitoring

## Chat Interaction Preferences

### Communication Style
- **Conversational development**: Keep interactions natural and collaborative
- **Incremental progress**: Small changes in chat, complete files only when confirmed
- **One file at a time**: Don't generate multiple files without confirmation
- **Confirm before generating**: Always ask before creating files, especially large ones

### Development Approach
- **Step-by-step methodology**: Break complex tasks into manageable items
- **One step per message**: Working incrementally means only one step per message
- **Item-by-item walkthroughs**: Go through tasks systematically (very helpful)
- **Hands-on collaboration**: I prefer to be involved in design decisions
- **Practical focus**: Avoid sweeping changes, prefer targeted improvements

### Code Preferences  
- **Small targeted changes**: Update specific methods rather than rewriting entire classes
- **Clear explanations**: Explain design decisions and trade-offs
- **Repository patterns**: I like the repository pattern for database access
- **Error handling**: Graceful failure handling, log warnings but continue processing
- **No migrations**: Don't generate migration files unless explicitly requested - I prefer to delete and recreate databases during development
- **Dependency injection**: All new development must use DI patterns implemented in the indexer init
- **Configuration pattern**: All environment variables must use IndexerConfig, following existing patterns

### Database Architecture Preferences
- **Clear separation**: Shared vs indexer database tables must be clearly distinguished
- **Dependency injection**: All database connections managed through DI container
- **Repository pattern**: Clean query interfaces, business logic in services
- **Infrastructure vs Model clarity**: Chain-level data in shared DB, indexer-specific data in model DB

### Migration System Preferences
- **Alembic for shared database**: Traditional migrations for infrastructure schema evolution
- **Templates for model databases**: Recreation rather than migration for rapid development
- **Custom type handling**: Automatic import generation for `EvmAddressType`, etc.
- **Development workflow**: Easy reset and recreation during development iterations

### Response Format
- **Structured responses**: Use headings and bullet points for clarity
- **Code context**: Explain where changes fit in the overall architecture
- **Examples**: Provide usage examples for new functionality
- **Options**: Present alternatives when there are multiple approaches
- **Complete replacement files**: When providing artifacts, provide complete replacement files
- **Partial replacement clarity**: If a file is too long or only a single part is being updated, make it clear that it is a partial replacement and provide either a complete class replacement or complete method replacement

### What Works Well
- **Conversational tone**: Natural back-and-forth discussion
- **Incremental building**: Building features piece by piece
- **Clear documentation**: Well-commented code and explanations
- **Practical examples**: CLI usage examples and cron job setups

## Development Workflow

### **Migration Workflow**
```bash
# Development setup
python -m indexer.cli migrate dev setup blub_test

# Configuration import
python -m indexer.cli config import-shared config/shared_db/shared_v1_0.yaml
python -m indexer.cli config import-model config/model_db/blub_test_v1_0.yaml

# Status checking
python -m indexer.cli migrate status
python db_inspector.py
```

### **Development Iteration**
```bash
# When making model schema changes
python -m indexer.cli migrate model recreate blub_test

# When making shared schema changes
python -m indexer.cli migrate shared create "Description of changes"
python -m indexer.cli migrate shared upgrade
```

### **Pricing Management**
```bash
# Pricing operations
python -m indexer.cli pricing status blub_test
python -m indexer.cli pricing update-all blub_test
```

## Current Development Phase

### **✅ COMPLETED: Core Infrastructure**
- Migration system with custom type support
- Dual database architecture
- Configuration management system
- Direct pricing implementation
- CLI interface and documentation

### **🎯 NEXT: Testing Module Overhaul**

The testing module needs to be rebuilt from scratch to focus on:

1. **Clean up legacy files**: Remove outdated diagnostic and test files created for specific issues
2. **End-to-end tests**: 1-2 comprehensive tests for core functionality
3. **Infrastructure diagnostics**: Health checks for indexer containers, cloud services, database
4. **Development-focused**: Testing suitable for ongoing development rather than comprehensive test suite

The system is now ready for comprehensive testing and validation before moving to production indexing operations.

## Key Design Principles

### **Modularity**
- Clear separation between infrastructure and domain logic
- Dependency injection for testability and flexibility
- Repository pattern for data access abstraction

### **Configuration-Driven**
- Database-driven configuration with YAML import/export
- Environment-specific overrides
- Global defaults with model-specific customization

### **Development Efficiency** 
- Template-based model database recreation for rapid iteration
- Comprehensive CLI interface for all operations
- Clear documentation and troubleshooting guides

### **Production Readiness**
- Proper error handling and logging
- Health monitoring and diagnostics
- Scalable architecture with read replicas
- Pricing accuracy and method tracking

The indexer is now architecturally complete and ready for the final testing phase before production deployment.