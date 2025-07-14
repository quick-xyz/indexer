# Domain Events Export Report

**Model:** blub_test vv2
**Database:** blub_test
**Generated:** 2025-07-14 11:16:06
**Export Directory:** db_exporter/blub_test

## Domain Event Tables

### Trades
**Description:** Trade events - user trading activity

- **Rows exported:** 3
- **Columns:** 13
- **File:** `trades.csv`
- **Key columns:** taker, direction, base_token, base_amount, trade_type
- **Relationships:** Links to pool_swaps via content_id references
- **Time range:** 1745844478 to 1752259407

### Pool_Swaps
**Description:** Individual pool swap events within trades

- **Rows exported:** 0
- **Columns:** 14
- **File:** `pool_swaps.csv`
- **Key columns:** pool, taker, direction, base_token, quote_token
- **Relationships:** Can be linked to trades via trade_id

### Transfers
**Description:** Token transfer events

- **Rows exported:** 10
- **Columns:** 13
- **File:** `transfers.csv`
- **Key columns:** token, from_address, to_address, amount
- **Relationships:** Can link to parent events via parent_id/parent_type
- **Time range:** 1741708903 to 1752259407

### Liquidity
**Description:** Liquidity provision/removal events

- **Rows exported:** 2
- **Columns:** 13
- **File:** `liquidity.csv`
- **Key columns:** pool, provider, action, base_token, quote_token
- **Relationships:** References pools and tokens
- **Time range:** 1741207547 to 1741733435

### Rewards
**Description:** Reward distribution events (fees, farming rewards)

- **Rows exported:** 2
- **Columns:** 11
- **File:** `rewards.csv`
- **Key columns:** contract, recipient, token, amount, reward_type
- **Relationships:** Links to reward contracts and recipients
- **Time range:** 1741207547 to 1741733435

### Positions
**Description:** Position changes - deposits, withdrawals, balance updates

- **Rows exported:** 48
- **Columns:** 13
- **File:** `positions.csv`
- **Key columns:** user, token, amount, custodian
- **Relationships:** Can link to parent events via parent_id/parent_type
- **Time range:** 1741207547 to 1752259407

## Files Generated

- `analysis_processing_status.csv`
- `block_processing.csv`
- `liquidity.csv`
- `pool_swaps.csv`
- `positions.csv`
- `rewards.csv`
- `trades.csv`
- `transaction_processing.csv`
- `transfers.csv`

## Usage Notes

- All timestamps are Unix timestamps (seconds since epoch)
- Amounts are stored as strings to preserve precision
- Address fields use checksummed Ethereum addresses
- Enum fields (direction, action, etc.) use lowercase values
