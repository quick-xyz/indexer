# Domain Event Hunter - Usage Guide

## Overview
The Domain Event Hunter finds specific domain events in GCS complete blocks and compares exactly what made it to the database. Perfect for troubleshooting missing events like trades, pool swaps, etc.

## Location
```bash
scripts/domain_event_hunter.py
```

## Quick Start
```bash
# Hunt for 10 blocks with Trade events
python scripts/domain_event_hunter.py hunt Trade 10

# Hunt for 5 blocks with PoolSwap events
python scripts/domain_event_hunter.py hunt PoolSwap 5
```

## Commands

### Hunt for Single Event Type
```bash
python scripts/domain_event_hunter.py hunt <event_type> [count] [model_name]
```
- `event_type`: Trade, PoolSwap, Transfer, Liquidity, Reward, Position
- `count`: Number of blocks to find (default: 10)
- `model_name`: Optional model name (uses env var if not specified)

**Examples:**
```bash
python scripts/domain_event_hunter.py hunt Trade 10
python scripts/domain_event_hunter.py hunt PoolSwap 5 blub_test
python scripts/domain_event_hunter.py hunt Liquidity 15
```

### Hunt for Multiple Event Types (Comparative)
```bash
python scripts/domain_event_hunter.py multi <event_type1,event_type2,...> [count] [model_name]
```

**Examples:**
```bash
python scripts/domain_event_hunter.py multi Trade,PoolSwap 10
python scripts/domain_event_hunter.py multi Trade,PoolSwap,Transfer,Liquidity 10
```

## Output Files

All files are saved to: `db_exporter/block_compare/`

### 1. Main Report
**File:** `hunt_trade_20241211_143022.json`
- Complete GCS block data with target events
- Full database records for all transactions in those blocks
- Transaction-by-transaction comparison
- Detailed discrepancy analysis

### 2. Summary Report
**File:** `summary_trade_20241211_143022.json`
- High-level statistics and findings
- Persistence rates (GCS → database)
- Key patterns and common issues
- Quick overview for executives

### 3. Individual Block Files
**Directory:** `blocks_trade_20241211_143022/`
- One JSON file per block analyzed
- Detailed transaction-level data
- Perfect for debugging specific cases

### 4. Comparative Report (for multi command)
**File:** `comparative_hunt_20241211_143022.json`
- Side-by-side comparison of multiple event types
- Availability and persistence rates
- Identifies which event types are most problematic

## Common Use Cases

### Troubleshoot Missing Trades
```bash
python scripts/domain_event_hunter.py hunt Trade 10
```
**What it tells you:**
- Are Trade events being generated in GCS?
- Are they making it to the database?
- Which specific transactions are failing?

### Compare Event Type Issues
```bash
python scripts/domain_event_hunter.py multi Trade,PoolSwap,Transfer 10
```
**What it tells you:**
- Which event types are working vs broken?
- Are some events persisting while others aren't?
- Is this a transform issue or database issue?

### Deep Dive on Specific Event Type
```bash
python scripts/domain_event_hunter.py hunt PoolSwap 20
```
**What it tells you:**
- Pattern analysis across more blocks
- Consistency of the issue
- Transaction success vs failure patterns

## Reading the Results

### Key Metrics to Look For

**In Summary Report:**
- `transactions_found_in_db` vs `transactions_missing_from_db`
- `blocks_with_discrepancies` 
- `discrepancy_patterns`

**Common Patterns:**
- `transaction_not_found`: Entire transactions missing from database
- `event_count_mismatch`: Some events missing from transactions
- High persistence rate (>90%) = Good
- Low persistence rate (<50%) = Major issue

### Sample Output Interpretation
```json
{
  "summary": {
    "total_gcs_transactions": 25,
    "transactions_found_in_db": 25,
    "transactions_missing_from_db": 0,
    "blocks_with_discrepancies": 8,
    "discrepancy_patterns": {
      "event_count_mismatch": 12
    }
  }
}
```
**Translation:** All transactions are in database, but 12 have wrong event counts (events being generated but not persisted).

## Troubleshooting Tips

### No Events Found
```bash
# Try different event types
python scripts/domain_event_hunter.py multi Trade,PoolSwap,Transfer,Liquidity 5

# Try more blocks
python scripts/domain_event_hunter.py hunt Trade 50
```

### Script Errors
- Ensure you're in the project root directory
- Check that your model database exists and is accessible
- Verify GCS credentials are configured
- Check that the model name is correct

### Large Output Files
- Use smaller sample counts for initial analysis
- Focus on recent blocks (script searches newest first)
- Use summary files for quick overview

## Example Workflow

1. **Quick Health Check**
   ```bash
   python scripts/domain_event_hunter.py multi Trade,PoolSwap,Transfer 5
   ```

2. **Deep Dive on Problem Event**
   ```bash
   python scripts/domain_event_hunter.py hunt Trade 15
   ```

3. **Analyze Results**
   - Open summary JSON for quick overview
   - Open individual block files for transaction details
   - Look for patterns in discrepancies

4. **Follow Up**
   - If events missing: Check transform pipeline
   - If transactions missing: Check database writer
   - If counts wrong: Check domain event writer logic

## Integration with Other Tools

**Use with CSV Exporter:**
```bash
# First find problematic blocks
python scripts/domain_event_hunter.py hunt Trade 10

# Then export database state
python scripts/domain_events_exporter.py blub_test 1000
```

**Use with Block Validator:**
```bash
# Validate overall pipeline health
python scripts/block_processing_validator.py health

# Then hunt for specific issues
python scripts/domain_event_hunter.py hunt Trade 10
```