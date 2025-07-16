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
- **Calculation Service**: Event valuations and analytics aggregation (5-minute schedule) 
- **Aggregation Service**: User metrics and portfolio summaries (15-minute schedule)

**Dual Database Strategy:**
- **Shared Database** (`indexer_shared_v2`): Chain-level data and configuration shared across all indexers
  - Configuration: models, contracts, tokens, sources, addresses
  - Chain-level pricing: block_prices, periods, pool_pricing_configs
- **Indexer Database** (per model, e.g., `blub_test_v2`): Model-specific indexing data
  - Processing state: transaction_processing, processing_jobs
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
7. **Migration System**: Standalone scripts for schema evolution and data preservation

### Technology Stack
- **Language**: Python with msgspec for data structures
- **Database**: PostgreSQL with SQLAlchemy (dual database architecture)
- **Storage**: Google Cloud Storage
- **RPC**: QuickNode for Avalanche mainnet
- **Architecture**: Dependency injection container pattern
- **Migrations**: Standalone data migration scripts with comprehensive validation

## Recent Major Accomplishments ✅

### **✅ COMPLETED: Database Migration (July 16, 2025)**
- **Complete Migration**: 440,817 rows across 8 tables migrated successfully
- **100% Success Rate**: Perfect validation across all tables
- **Schema Evolution**: Successfully handled reserved keywords, JSONB conversion, field drops
- **Production Ready**: V2 databases (`indexer_shared_v2`, `blub_test_v2`) fully operational
- **Data Integrity**: Complete blockchain activity preserved with all relationships intact

**Migration Details:**
- liquidity: 46 rows (direct mapping)
- pool_swaps: 32,365 rows (direct mapping)
- positions: 256,624 rows (reserved keyword handling)
- processing_jobs: 356 rows (JSONB conversion)
- rewards: 44 rows (direct mapping)
- trades: 32,295 rows (direct mapping)
- transaction_processing: 54,310 rows (schema evolution)
- transfers: 64,421 rows (direct mapping)

### **✅ COMPLETED: Core Infrastructure**
- Migration system with custom type support
- Dual database architecture
- Configuration management system
- Direct pricing implementation
- CLI interface and documentation

### **✅ COMPLETED: Direct Pricing Implementation**
- **Detail Tables**: Separate pricing tables (`pool_swap_details`, `trade_details`, `event_details`) 
- **Dual Denominations**: Every event gets both USD and AVAX valuations
- **Method Tracking**: DIRECT_AVAX, DIRECT_USD, GLOBAL, ERROR pricing methods
- **Volume Weighting**: Trade pricing aggregates from constituent swaps
- **Comprehensive CLI**: Full management interface for all pricing operations

## Chat Interaction Preferences

### Communication Style
- **Conversational development**: Keep interactions natural and collaborative
- **Incremental progress**: Small changes in chat, complete files only when confirmed
- **One file at a time**: Don't generate multiple files without confirmation
- **Confirm before generating**: Always ask before creating files, especially large ones

### Development Approach - ⭐ **Enhanced from Migration Experience**
- **Step-by-step methodology**: Break complex tasks into manageable items
- **One step per message**: Working incrementally means only one step per message
- **Item-by-item walkthroughs**: Go through tasks systematically (very helpful)
- **Hands-on collaboration**: I prefer to be involved in design decisions
- **Practical focus**: Avoid sweeping changes, prefer targeted improvements

**From Migration Success:**
- **Ask before developing**: Don't jump straight into fixes without understanding the actual problem
- **Verify current state**: Check actual files/tables instead of inferring from documentation
- **Small targeted changes**: Update specific methods rather than rewriting entire classes
- **Question mismatches**: When two things don't match, ask which should be changed rather than deciding

### Code Preferences  
- **Small targeted changes**: Update specific methods rather than rewriting entire classes
- **Clear explanations**: Explain design decisions and trade-offs
- **Repository patterns**: I like the repository pattern for database access
- **Error handling**: Graceful failure handling, log warnings but continue processing
- **No migrations**: Don't generate migration files unless explicitly requested - I prefer to delete and recreate databases during development
- **Dependency injection**: All new development must use DI patterns implemented in the indexer init
- **Configuration pattern**: All environment variables must use IndexerConfig, following existing patterns

**From Migration Success:**
- **Repository patterns**: Use established repository patterns for database access
- **Error handling**: Graceful failure handling, log warnings but continue processing
- **No assumptions**: Don't generate code for cases that aren't confirmed to be true
- **Dependency injection**: All new development must use DI patterns

### Database Architecture Preferences
- **Clear separation**: Shared vs indexer database tables must be clearly distinguished
- **Dependency injection**: All database connections managed through DI container
- **Repository pattern**: Clean query interfaces, business logic in services
- **Infrastructure vs Model clarity**: Chain-level data in shared DB, indexer-specific data in model DB

### What Works Well - ⭐ **Proven Patterns**
- **Conversational tone**: Natural back-and-forth discussion
- **Incremental building**: Building features piece by piece
- **Clear documentation**: Well-commented code and explanations
- **Practical examples**: CLI usage examples and cron job setups

**From Migration Success:**
- **Standalone scripts**: Independent, self-contained migration scripts work better than inheritance
- **Comprehensive validation**: 5-6 validation checks per operation catch all issues
- **Transaction safety**: Rollback capability essential for complex operations
- **Copy-and-modify pattern**: Easier than complex base classes for similar operations

### What Doesn't Work - ⚠️ **Learned from Experience**
- **Jumping to solutions**: Don't immediately start developing without understanding the problem
- **Making assumptions**: Don't infer problems exist without checking actual code
- **Multiple versions**: Don't generate 9 versions of fixes without dialogue
- **Ignoring guidance**: Don't proceed when told a different approach is needed
- **Complex inheritance**: Simple standalone patterns often work better than abstract base classes

### Response Format
- **Structured responses**: Use headings and bullet points for clarity
- **Code context**: Explain where changes fit in the overall architecture
- **Examples**: Provide usage examples for new functionality
- **Options**: Present alternatives when there are multiple approaches
- **Complete replacement files**: When providing artifacts, provide complete replacement files
- **Partial replacement clarity**: If a file is too long or only a single part is being updated, make it clear that it is a partial replacement and provide either a complete class replacement or complete method replacement

## Development Workflow

### **V2 Database Operations** ✅
```bash
# V2 database operations (post-migration)
python -m indexer.cli migrate dev setup blub_test_v2

# Configuration import (V2 databases)
python -m indexer.cli config import-shared config/shared_db/shared_v1_0.yaml
python -m indexer.cli config import-model config/model_db/blub_test_v1_0.yaml

# Status checking
python -m indexer.cli migrate status
```

### **Pricing Management** ✅
```bash
# Pricing operations (ready for testing)
python -m indexer.cli pricing status blub_test_v2
python -m indexer.cli pricing update-all blub_test_v2
```

### **Migration Operations** ✅ **COMPLETED**
```bash
# Migration scripts (successfully completed)
python scripts/data_migration/migrate_liquidity.py
python scripts/data_migration/migrate_pool_swaps.py
python scripts/data_migration/migrate_positions.py
python scripts/data_migration/migrate_processing_jobs.py
python scripts/data_migration/migrate_rewards.py
python scripts/data_migration/migrate_trades.py
python scripts/data_migration/migrate_transaction_processing.py
python scripts/data_migration/migrate_transfers.py
```

## Current Development Phase

### **✅ COMPLETED: Database Migration & Core Infrastructure**
- ✅ **Migration System**: 440,817 rows migrated with 100% validation success
- ✅ **Dual Database Architecture**: V2 databases fully operational
- ✅ **Configuration Management**: Successfully loaded into databases
- ✅ **Direct Pricing System**: Complete implementation with CLI management
- ✅ **Repository Layer**: Enhanced with bulk operations and validation
- ✅ **Migration Scripts**: Standalone pattern proven across 8 tables

### **🎯 CURRENT FOCUS: Pricing Service Testing & Validation**

**Now that the core infrastructure is complete and 440K+ rows of real data are available:**

**Priority Development Plan:**
1. **🎯 Test Pricing Service**: Validate canonical VWAP pricing with real data
2. **🎯 Debug Pricing Issues**: Work out any calculation or configuration problems
3. **🎯 Calculation Service**: Event valuations, OHLC candles, protocol volume metrics
4. **🎯 Service Integration**: Test end-to-end pricing workflows
5. **🎯 Performance Validation**: Ensure pricing services handle production load
6. **🎯 CLI Testing**: Validate all pricing management commands
7. **🎯 Error Handling**: Test graceful fallback scenarios
8. **🎯 Data Quality**: Verify pricing accuracy against expected results

### **Architecture Ready for Pricing Work**
- ✅ **Real Data Available**: 440,817 rows across all event types for testing
- ✅ **Database Schema**: All pricing tables and relationships established
- ✅ **Service Foundation**: Repository patterns and DI container ready
- ✅ **CLI Interface**: Management commands available for testing
- ✅ **Configuration System**: Pool pricing configs ready for use

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
- Standalone scripts for complex operations (proven with migration)
- Comprehensive CLI interface for all operations
- Clear documentation and troubleshooting guides
- Copy-and-modify patterns for similar functionality

### **Production Readiness**
- Proper error handling and logging
- Health monitoring and diagnostics
- Scalable architecture with read replicas
- Pricing accuracy and method tracking
- Complete data validation and integrity checking

### **Testing & Validation**
- Comprehensive validation patterns (5-6 checks per operation)
- Transaction safety with rollback capability
- Real data testing with production-scale datasets
- CLI-driven testing and debugging workflows

## Data Migration Lessons Learned ⭐

### **Successful Patterns**
- **Standalone Scripts**: Self-contained operations easier than inheritance patterns
- **Comprehensive Validation**: Multiple validation checks catch all edge cases
- **Stable Ordering**: Use `ORDER BY id` not `ORDER BY created_at` for validation
- **Transaction Safety**: Rollback essential for complex data operations
- **Schema Evolution**: Handle reserved keywords, JSONB conversion, field drops gracefully

### **Migration Success Metrics**
- **100% Success Rate**: All 8 tables migrated perfectly
- **Zero Data Loss**: Complete preservation of blockchain activity
- **Perfect Validation**: All relationships and constraints maintained
- **Production Ready**: V2 databases operational immediately after migration

---

**Current Status**: Database migration complete, pricing infrastructure ready  
**Next Focus**: Pricing service testing and validation with real data  
**Data Available**: 440,817 rows across 8 tables for comprehensive testing  
**Architecture**: Production-ready with proven patterns and comprehensive validation