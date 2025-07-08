# Token Indexer: Pricing & Valuation Architecture Design

## Overview

This document outlines the architectural design for a blockchain token indexer with focus on pricing calculation and valuation propagation for a web application. The system is designed to handle token-specific economic activity with efficient pricing and user-facing valuations.

## Core Requirements

- **Token-Centric**: Index all activity for a specific token economy
- **Price Authority**: Use designated AMM pools as canonical price sources
- **Web Application**: Support many users viewing different data with acceptable latency
- **Valuation Accuracy**: Events/balances valued using prices from their specific timestamps
- **No Historical Versions**: Maintain only current best understanding of historical valuations
- **Latency Tolerance**: 1-5 minute lag acceptable for most use cases

## Service Architecture

### 1. Pricing Service
**Schedule**: Every 1 minute  
**Responsibility**: Canonical price calculation only

**Core Functions**:
- Generate 1-minute OHLCV candles from raw swap data
- Calculate 5-minute trailing volume-weighted average price (VWAP)
- Aggregate weighted prices across designated "main pools"
- Create canonical price table (primary source of truth for all valuations)
- Handle historical price amendments when late blockchain data arrives

**Design Principles**:
- Schedule-based execution (not event-driven)
- Idempotent processing (safe to re-run same time periods)
- Gap-aware (can detect and backfill missing periods)
- Amendment-friendly (can recalculate when new data arrives)

### 2. Calculation Service
**Schedule**: Every 5 minutes  
**Responsibility**: Manage all calculated/derived data in the system

**Core Functions**:
- Refresh materialized views for balance calculations (current balances, balance snapshots)
- Refresh materialized views for event/activity valuations  
- Refresh materialized views for LP position valuations
- Coordinate propagation of both price changes AND position changes
- Ensure all calculated data reflects current understanding of historical events

**Design Choice**: Uses PostgreSQL materialized views with `REFRESH MATERIALIZED VIEW CONCURRENTLY`
- Simpler implementation than calculated tables
- Less prone to bugs in update logic
- Non-blocking refresh (users can query during updates)
- Easy to modify calculation logic by changing view definitions
- Handles late-arriving position data automatically

### 3. Aggregation Service
**Schedule**: Every 15 minutes  
**Responsibility**: Generate summary metrics and time-series aggregations

**Core Functions**:
- Daily/hourly user portfolio summaries
- Pool statistics and TVL calculations
- User activity metrics
- Historical trend calculations

**Design Choice**: Uses calculated tables (not materialized views)
- Supports incremental updates
- Mixed update patterns (some metrics update with prices, others with new events)
- Better performance for time-series data
- Flexible retention policies

## Database Design Strategy

### Base Data Tables (Populated by Indexer)
```sql
-- Raw swap events from main pools
main_pool_swaps (block_number, transaction_hash, pool_address, amounts, timestamp)

-- User balance snapshots  
user_balances (user_address, pool_address, block_number, lp_balance)

-- LP pool share values
lp_share_values (block_number, pool_address, reserves, per_share_values)

-- All user activities/events
events (event_id, user_address, event_type, token_amount, event_timestamp)

-- Token position ledger (append-only)
positions (
    id, user_address, token_address, token_amount, 
    block_number, transaction_hash, position_type, timestamp
)
```

### Pricing Tables (Populated by Pricing Service)
```sql
-- 1-minute OHLCV candles with 5m trailing VWAP
price_candles_1m (candle_start, pool_address, OHLCV, vwap_5m_trailing)

-- Canonical prices (source of truth for all valuations)
canonical_prices (timestamp_minute, pair_token, price_5m_vwap_usd)
```

### Calculation Views/Tables (Populated by Calculation Service)
```sql
-- Current token balances (materialized view)
CREATE MATERIALIZED VIEW current_balances AS
SELECT user_address, token_address, SUM(token_amount) as balance,
       MAX(block_number) as last_position_block, COUNT(*) as position_count
FROM positions GROUP BY user_address, token_address
HAVING SUM(token_amount) != 0;

-- Historical balance snapshots (materialized view)  
CREATE MATERIALIZED VIEW balance_snapshots AS
SELECT user_address, token_address, snapshot_block, 
       SUM(token_amount) as balance, snapshot_timestamp
FROM positions p CROSS JOIN snapshot_intervals si
WHERE p.block_number <= si.snapshot_block
GROUP BY user_address, token_address, snapshot_block, snapshot_timestamp
HAVING SUM(token_amount) != 0;

-- Event/activity valuations (materialized views)
CREATE MATERIALIZED VIEW event_valuations AS
SELECT e.event_id, e.token_amount, 
       e.token_amount * cp.price_5m_vwap_usd as usd_value,
       cp.price_5m_vwap_usd as price_used
FROM events e
LEFT JOIN canonical_prices cp ON [price lookup at event timestamp];

CREATE MATERIALIZED VIEW balance_valuations AS
SELECT bs.*, bs.balance * cp.price_5m_vwap_usd as usd_value
FROM balance_snapshots bs
LEFT JOIN canonical_prices cp ON [price lookup at snapshot timestamp];

CREATE MATERIALIZED VIEW lp_position_valuations AS [similar pattern];
```

### Aggregation Tables (Populated by Aggregation Service)
```sql
-- Calculated tables for metrics
user_daily_metrics (user_address, date, total_portfolio_value_usd, active_pools_count)
pool_stats_daily (pool_address, date, total_tvl_usd, unique_users)
```

## Token Position & Balance Management

### Position Ledger Design

**Core Principle**: Maintain append-only ledger that handles late-arriving blockchain data

**Position Table Structure**:
```sql
CREATE TABLE positions (
    id BIGSERIAL PRIMARY KEY,
    user_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    token_amount NUMERIC(78,0) NOT NULL,  -- Can be negative (withdrawals)
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    position_type TEXT NOT NULL,  -- 'deposit', 'withdraw', 'reward', etc.
    timestamp BIGINT NOT NULL,
    indexed_at TIMESTAMP DEFAULT NOW()
);
```

### Balance Calculation Strategy

**Problem**: Need current balances + historical snapshots that handle late position arrivals

**Solution**: Materialized views that automatically recalculate when positions change

**Current Balances**:
```sql
CREATE MATERIALIZED VIEW current_balances AS
SELECT 
    user_address,
    token_address,
    SUM(token_amount) as balance,
    MAX(block_number) as last_position_block,
    COUNT(*) as position_count
FROM positions
GROUP BY user_address, token_address
HAVING SUM(token_amount) != 0;
```

**Historical Balance Snapshots**:
```sql
CREATE MATERIALIZED VIEW balance_snapshots AS
SELECT 
    user_address,
    token_address,
    snapshot_block,
    SUM(token_amount) as balance,
    snapshot_timestamp
FROM positions p
CROSS JOIN generate_series([block_intervals]) AS snapshot_block
WHERE p.block_number <= snapshot_block
GROUP BY user_address, token_address, snapshot_block, snapshot_timestamp
HAVING SUM(token_amount) != 0;
```

### Late Position Handling

**Philosophy**: Everything reflects current best understanding of historical events

**When Late Position Arrives**:
1. Added to append-only positions table
2. Calculation service refreshes current_balances materialized view
3. Calculation service refreshes balance_snapshots materialized view  
4. Calculation service refreshes balance_valuations (incorporates updated snapshots)
5. Aggregation service recalculates metrics for affected time periods

**Result**: All balance-dependent calculations automatically reflect the late position data

## Key Design Decisions

### Materialized Views vs Calculated Tables

**Event/Balance Valuations → Materialized Views**
- 1:1 transformation (each event gets one valuation)
- Simple refresh logic
- PostgreSQL handles complexity
- Easy to modify calculation logic
- Acceptable performance for valuation use case

**Aggregated Metrics → Calculated Tables**  
- Complex time-series aggregations
- Need incremental updates
- Mixed update triggers
- Custom retention policies
- Better performance control

### Price Amendment Strategy

**Challenge**: Blockchain data can arrive late, requiring historical price updates

**Solution**: 
- Events/balances are valued using canonical price from their timestamp
- When historical prices are amended, affected valuations are automatically recalculated
- No versioning - only current best understanding of historical values
- Materialized view refresh propagates all price changes automatically

### Service Coordination

**No Event-Driven Architecture**:
- Services run on predictable schedules
- Database-driven coordination (check completion timestamps)
- Simple monitoring and error handling
- Easier debugging and operational management

**Service Dependencies**:
```
Indexer → Raw Data Tables (including positions)
↓
Pricing Service → Canonical Prices  
↓
Calculation Service → All Materialized Views (balances + valuations)
↓  
Aggregation Service → Calculated Tables (metrics)
↓
REST API Layer → Read Replicas → Frontend
```

## API Architecture & Database Access Strategy

### PostgreSQL + REST API Approach

**Architecture**:
```
Master PostgreSQL → Read Replicas (2-3) → Load Balancer → REST API Layer → Frontend
```

**Why PostgreSQL + REST over GraphQL/ClickHouse**:
- **Relational data fits perfectly**: Events, balances, valuations, user relationships
- **Materialized view compatibility**: PostgreSQL's materialized views align with calculation service
- **Proven at Web3 scale**: Many successful Web3 projects use PostgreSQL
- **Simpler operations**: Single database system to maintain and debug
- **Cost effective**: No additional infrastructure complexity

### Database Abstraction Strategy

**REST API Benefits**:
- **Abstracts database schema**: Frontend doesn't see table structures, column names, or relationships
- **Security**: No direct database exposure to clients
- **Single query per endpoint**: Eliminates multiple round trips
- **Controlled access patterns**: API defines what data combinations are allowed
- **Schema evolution**: Can change database structure without breaking frontend

### Read Replica Strategy

**Master Database**: Handles all writes from services (pricing, calculation, aggregation)
**Read Replicas**: Serve REST API with pre-calculated data

**API Endpoint Examples**:
```javascript
// Single API call that abstracts complex joins
GET /api/users/{address}/activity?page=1&limit=50
// Returns unified activity feed from multiple tables

GET /api/users/{address}/portfolio/current
// Returns current portfolio value from materialized views

GET /api/users/{address}/metrics/daily?days=30
// Returns time-series metrics from aggregated tables
```

**Benefits**:
- Sub-second response times using pre-calculated data
- No complex calculations during user requests
- Scalable read capacity across geographic regions
- Database schema completely hidden from frontend
- Clean API contracts that can evolve independently

## Monitoring & Operations

**Key Metrics**:
- Price update lag (how far behind canonical pricing?)
- Valuation refresh duration (materialized view performance)
- Service completion timing (dependency coordination)
- Data freshness (user-facing staleness)

**Error Handling**:
- Services continue forward progress despite historical failures
- Background retry for failed time periods
- Clear separation of critical path (pricing) vs nice-to-have (metrics)

## Future Scalability Considerations

**When to Migrate from Materialized Views**:
- Refresh times become unacceptable (>2-3 minutes)
- Data volume makes full recalculation inefficient
- Need for more sophisticated update logic

**Migration Path**:
- Convert specific high-volume views to calculated tables
- Maintain hybrid approach (some views, some tables)
- Frontend queries remain unchanged due to consistent interface

## Implementation Benefits

1. **Simplicity**: Clear separation of concerns, minimal coordination complexity
2. **Accuracy**: Automatic price propagation ensures consistency  
3. **Performance**: Pre-calculated valuations serve user requests quickly
4. **Flexibility**: Easy to modify pricing/valuation logic
5. **Operational**: Predictable schedules, clear monitoring points
6. **Scalable**: Read replicas + pre-calculated data handles user growth