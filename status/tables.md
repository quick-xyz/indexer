# Database Schema Overview

## Database Architecture

### **Shared Database (`indexer_shared`)**
Chain-level infrastructure shared across all indexer models

### **Indexer Database (per model, e.g., `blub_test`)**  
Model-specific indexing and pricing data

---

## Table Definitions

### **SHARED DATABASE TABLES**

#### **Configuration Tables**

**`models`**
- `id` (Primary Key)
- `name` (Unique model identifier)
- `version` (Model version)
- `display_name` (Human-readable name)
- `description` (Model description)
- `database_name` (Associated indexer database)
- `status` (active/inactive)
- `created_at`, `updated_at`

**`contracts`** ⚠️ **NEEDS FIX: Missing project field in code**
- `id` (Primary Key)
- `address` (EVM contract address, Unique)
- `name` (Contract name)
- `project` (Project/protocol identifier) ← **Missing in Contract class**
- `type` (pool/token/router/factory/other)
- `description` (Contract description)
- `decode_config` (JSONB - ABI configuration)
- `transform_config` (JSONB - Transformer configuration)
- `pricing_strategy_default` (Global pricing default)
- `quote_token_address` (Default quote token for pricing)
- `pricing_start_block`, `pricing_end_block` (Default pricing block range)
- `status` (active/inactive)
- `created_at`, `updated_at`

**`tokens`**
- `id` (Primary Key)
- `address` (EVM token address, Unique)
- `name` (Token name)
- `symbol` (Token symbol)
- `decimals` (Token decimal places)
- `type` (erc20/native/other)
- `project` (Project/protocol identifier)
- `description` (Token description)
- `status` (active/inactive)
- `created_at`, `updated_at`

**`sources`**
- `id` (Primary Key)
- `name` (Source name)
- `path` (GCS path or identifier)
- `source_type` (gcs/rpc/other)
- `description` (Source description)
- `status` (active/inactive)
- `created_at`, `updated_at`

**`addresses`**
- `id` (Primary Key)
- `address` (EVM address)
- `name` (Address name/label)
- `type` (wallet/contract/other)
- `project` (Associated project)
- `grouping` (Address grouping for analytics)
- `description` (Address description)
- `status` (active/inactive)
- `created_at`, `updated_at`

#### **Association Tables**

**`model_contracts`**
- `id` (Primary Key)
- `model_id` (Foreign Key → models)
- `contract_id` (Foreign Key → contracts)
- `created_at`
- Unique constraint: (model_id, contract_id)

**`model_tokens`**
- `id` (Primary Key)
- `model_id` (Foreign Key → models)
- `token_id` (Foreign Key → tokens)
- `created_at`
- Unique constraint: (model_id, token_id)

**`model_sources`**
- `id` (Primary Key)
- `model_id` (Foreign Key → models)
- `source_id` (Foreign Key → sources)
- `created_at`
- Unique constraint: (model_id, source_id)

#### **Pricing Infrastructure Tables**

**`periods`**
- `id` (Primary Key)
- `period_type` (1min/5min/1hr/4hr/1day)
- `time_open` (Period start timestamp)
- `time_close` (Period end timestamp)
- `block_open` (Period start block)
- `block_close` (Period end block)
- `is_complete` (Boolean completion flag)
- `created_at`, `updated_at`

**`block_prices`**
- `id` (Primary Key)
- `block_number` (Block number, Unique)
- `timestamp` (Block timestamp)
- `price_usd` (AVAX-USD price from Chainlink)
- `fetched_at` (When price was fetched)
- `created_at`, `updated_at`

**`price_vwap`** ← **Canonical price authority**
- `id` (Primary Key)
- `timestamp_minute` (Minute timestamp)
- `asset_address` (Asset being priced)
- `denomination` (usd/avax)
- `base_volume` (Volume in base asset)
- `quote_volume` (Volume in quote asset)
- `price_period` (Price for this specific minute)
- `price_vwap_5m` (5-minute trailing VWAP)
- `periods_used` (JSONB - Array of period IDs used)
- `created_at`, `updated_at`

**`pool_pricing_configs`**
- `id` (Primary Key)
- `model_id` (Foreign Key → models)
- `contract_id` (Foreign Key → contracts)
- `start_block` (Configuration start block)
- `end_block` (Configuration end block, NULL = ongoing)
- `pricing_strategy` (direct_avax/direct_usd/global/use_global_default)
- `quote_token_address` (Quote token for direct pricing)
- `quote_token_type` (AVAX/USD)
- `pricing_pool` (Boolean - use for canonical pricing)
- `created_at`, `updated_at`

---

### **INDEXER DATABASE TABLES**

#### **Processing State Tables**

**`transaction_processing`**
- `id` (Primary Key)
- `transaction_hash` (EVM transaction hash, Unique)
- `block_number` (Block containing transaction)
- `status` (pending/processing/completed/failed)
- `started_at`, `completed_at`
- `error_message` (If processing failed)
- `retry_count` (Number of retry attempts)

**`block_processing`**
- `id` (Primary Key)
- `block_number` (Block number, Unique)
- `status` (pending/processing/completed/failed)
- `started_at`, `completed_at`
- `transaction_count` (Number of transactions in block)
- `events_created` (Number of events created)
- `error_message` (If processing failed)

**`processing_jobs`**
- `id` (Primary Key)
- `job_type` (Type of processing job)
- `status` (pending/processing/completed/failed)
- `block_list` (JSONB - Array of block numbers)
- `created_at`, `started_at`, `completed_at`
- `worker_id` (Worker processing this job)
- `error_message` (If job failed)

#### **Domain Event Tables**

**`trades`**
- `content_id` (Primary Key, Domain Event ID)
- `transaction_hash` (EVM transaction hash)
- `block_number` (Block number)
- `timestamp` (Block timestamp)
- `user_address` (Trader address)
- `base_token` (Base token address)
- `quote_token` (Quote token address)
- `base_amount` (Raw base token amount)
- `quote_amount` (Raw quote token amount)
- `trade_type` (buy/sell)
- `created_at`, `updated_at`

**`pool_swaps`**
- `content_id` (Primary Key, Domain Event ID)
- `transaction_hash` (EVM transaction hash)
- `block_number` (Block number)
- `timestamp` (Block timestamp)
- `trade_id` (Foreign Key → trades, if part of trade)
- `pool` (Pool contract address)
- `user_address` (Swapper address)
- `base_token` (Base token address)
- `quote_token` (Quote token address)
- `base_amount` (Raw base token amount)
- `quote_amount` (Raw quote token amount)
- `swap_type` (buy/sell)
- `created_at`, `updated_at`

**`transfers`**
- `content_id` (Primary Key, Domain Event ID)
- `transaction_hash` (EVM transaction hash)
- `block_number` (Block number)
- `timestamp` (Block timestamp)
- `token_address` (Token being transferred)
- `from_address` (Sender address)
- `to_address` (Recipient address)
- `amount` (Raw token amount)
- `transfer_type` (transfer/mint/burn)
- `created_at`, `updated_at`

**`positions`**
- `content_id` (Primary Key, Domain Event ID)
- `transaction_hash` (EVM transaction hash)
- `block_number` (Block number)
- `timestamp` (Block timestamp)
- `user_address` (User address)
- `token_address` (Token address)
- `amount_delta` (Raw amount change, can be negative)
- `position_type` (deposit/withdraw/reward/penalty)
- `created_at`, `updated_at`

**`liquidity`**
- `content_id` (Primary Key, Domain Event ID)
- `transaction_hash` (EVM transaction hash)
- `block_number` (Block number)
- `timestamp` (Block timestamp)
- `pool_address` (Pool contract address)
- `user_address` (Liquidity provider address)
- `token_a_address` (First token address)
- `token_b_address` (Second token address)
- `token_a_amount` (Raw amount of token A)
- `token_b_amount` (Raw amount of token B)
- `liquidity_amount` (Raw LP token amount)
- `action_type` (add/remove)
- `created_at`, `updated_at`

**`rewards`**
- `content_id` (Primary Key, Domain Event ID)
- `transaction_hash` (EVM transaction hash)
- `block_number` (Block number)
- `timestamp` (Block timestamp)
- `user_address` (Reward recipient)
- `token_address` (Reward token address)
- `amount` (Raw reward amount)
- `reward_type` (staking/liquidity/governance/other)
- `source_address` (Reward source contract)
- `created_at`, `updated_at`

#### **Pricing Detail Tables**

**`pool_swap_details`** ← **Dual denomination swap pricing**
- `id` (Primary Key)
- `content_id` (Foreign Key → pool_swaps)
- `denomination` (usd/avax)
- `value` (Human-readable swap value)
- `price` (Human-readable price per unit)
- `price_method` (direct_avax/direct_usd/global/error)
- `price_config_id` (Foreign Key → pool_pricing_configs, if direct)
- `created_at`, `updated_at`
- Unique constraint: (content_id, denomination)

**`trade_details`** ← **Aggregated trade pricing**
- `id` (Primary Key)
- `content_id` (Foreign Key → trades)
- `denomination` (usd/avax)
- `value` (Human-readable trade value)
- `price` (Human-readable price per unit)
- `pricing_method` (direct/global)
- `created_at`, `updated_at`
- Unique constraint: (content_id, denomination)

**`event_details`** ← **General event valuations**
- `id` (Primary Key)
- `content_id` (Domain Event ID - any event type)
- `denomination` (usd/avax)
- `value` (Human-readable event value)
- `created_at`, `updated_at`
- Unique constraint: (content_id, denomination)

#### **Analytics Aggregation Tables**

**`asset_price`** ← **OHLC candles**
- `id` (Primary Key)
- `period_id` (Foreign Key → periods)
- `asset_address` (Asset being tracked)
- `denomination` (usd/avax)
- `open` (Period opening price)
- `high` (Period high price)
- `low` (Period low price)
- `close` (Period closing price)
- `volume` (Period volume)
- `trade_count` (Number of trades in period)
- `created_at`, `updated_at`
- Unique constraint: (period_id, asset_address, denomination)

**`asset_volume`** ← **Protocol-level volume metrics**
- `id` (Primary Key)
- `period_id` (Foreign Key → periods)
- `asset_address` (Asset being tracked)
- `denomination` (usd/avax)
- `volume` (Total volume for period)
- `protocol` (Protocol name from contract.project)
- `pool_count` (Number of pools contributing)
- `swap_count` (Number of swaps contributing)
- `created_at`, `updated_at`
- Unique constraint: (period_id, asset_address, denomination, protocol)

---

## Key Relationships

### **Configuration Flow**
```
models → model_contracts → contracts (with global pricing defaults)
models → pool_pricing_configs → contracts (model-specific overrides)
```

### **Pricing Flow**
```
pool_swaps → pool_swap_details (direct pricing)
pool_swap_details (pricing_pool=true) → price_vwap (canonical pricing)
price_vwap → pool_swap_details (global pricing)
price_vwap → event_details (event valuations)
```

### **Analytics Flow**
```
trade_details → asset_price (OHLC aggregation)
pool_swap_details + contract.project → asset_volume (protocol metrics)
```

## Index Strategy

### **High-Performance Indexes**
- All timestamp fields: `(timestamp)`, `(block_number)`
- Price queries: `(asset_address, timestamp)`, `(asset_address, period_id)`
- Configuration lookups: `(model_id, contract_id, block_number)`
- Event relationships: `(transaction_hash)`, `(content_id)`

### **Composite Indexes**
- `pool_pricing_configs`: `(model_id, contract_id, start_block, end_block)`
- `price_vwap`: `(asset_address, timestamp_minute, denomination)`
- Detail tables: `(content_id, denomination)` (enforced by unique constraints)

## Critical Issues to Address

### **🚨 Contract.project Field Missing**
**Issue**: Migration shows `project` field in contracts table, but Contract class in code doesn't have this field

**Impact**: Protocol-level volume aggregation (`asset_volume`) cannot work without this field

**Fix Required**: Add `project` field to Contract class in `indexer/database/shared/tables/config.py`

### **Data Quality Dependencies**
- `tokens.decimals` must be populated for decimal conversion
- `contracts.project` must be populated for protocol-level volume metrics
- `pool_pricing_configs.pricing_pool` must be set for canonical pricing

This schema supports the complete pricing and calculation architecture while maintaining clear separation between infrastructure (shared) and model-specific (indexer) data.