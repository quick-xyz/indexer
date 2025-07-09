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

### Technology Stack
- **Language**: Python with msgspec for data structures
- **Database**: PostgreSQL with SQLAlchemy (dual database architecture)
- **Storage**: Google Cloud Storage
- **RPC**: QuickNode for Avalanche mainnet
- **Architecture**: Dependency injection container pattern

## Recent Major Enhancements

### **Complete Direct Pricing Implementation**
- **Detail Tables**: Separate pricing tables (`pool_swap_details`, `trade_details`, `event_details`) 
- **Dual Denominations**: Every event gets both USD and AVAX valuations
- **Method Tracking**: DIRECT_AVAX, DIRECT_USD, GLOBAL, ERROR pricing methods
- **Volume Weighting**: Trade pricing aggregates from constituent swaps
- **Comprehensive CLI**: Full management interface for all pricing operations

### **Enhanced Service Architecture**
- **PricingService**: Now handles swap and trade direct pricing + existing period/block price functionality
- **Repository Layer**: Enhanced with bulk operations, eligibility checks, method statistics
- **CLI Interface**: Complete pricing management with monitoring and validation
- **Error Handling**: Graceful fallback to global pricing for complex cases

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

## Critical Next Steps

### **🚨 Priority Issues to Address**
1. **Processing Pipeline Review**: End-to-end single block processing has been failing
2. **Enum Consistency**: Capital vs lowercase issues causing processing errors
3. **Database Migration**: Need fresh initial migration with all current tables
4. **Migration Process Review**: Previous migrations have been problematic

### **Pipeline Integration Notes**
- **Pricing runs separately**: NOT part of indexing pipeline to avoid clogging
- **Independent services**: Pricing service processes events after they're indexed
- **Batch processing**: Pricing handles thousands of events efficiently in separate process
- **Error isolation**: Pricing issues don't affect core indexing functionality