TABLE: pool_pricing_config [shared]
- id
- model_id
- contract_id
- start_block
- end_block
- pricing_strategy
- pricing_pool (boolean)
- created_at
- updated_at


TABLE: block_prices [shared]
- block_number
- timestamp
- price_usd
- created_at
- updated_at
* note: block_prices is the primary AVAX-USD table
* note: block_prices table can also handle requirement 3. so every block in indexer plus every end block of every 1 minute


TABLE: periods [shared]
- period_type
- time_open
- time_close
- block_open
- block_close
- is_complete
- created_at
- updated_at


TABLE: price_vwap [shared]
- time (1 minute records)
- asset
- denom
- base_volume
- quote_volume
- price_period
- price_vwap
- created_at
- updated_at
* note: canonical price for a given asset, denominated in AVAX and USD


TABLE: asset_price [indexer]
- period_id
- asset
- denom
- open
- high
- low
- close
- created_at
- updated_at


TABLE: asset_volume [indexer]
- period_id
- asset
- denom
- volume
- protocol
- created_at
- updated_at


TABLE: trade_details [indexer]
- content_id
- denom
- value
- price
- price_method
- created_at
- updated_at


TABLE: pool_swap_details [indexer]
- content_id
- denom
- value
- price
- price_method
- price_config_id
- created_at
- updated_at


TABLE: event_details [indexer]
- content_id
- denom
- value
- created_at
- updated_at