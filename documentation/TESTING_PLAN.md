# Progressive Testing Plan

This document outlines a step-by-step approach to test the refactored blockchain indexer system.

## Testing Philosophy

Tests are designed to:
1. **Validate architecture** - Ensure dependency injection works correctly
2. **Test incrementally** - Build confidence step by step
3. **Identify issues early** - Fail fast on configuration or setup problems
4. **Provide examples** - Show how to use the refactored system

## Test Phases

### Phase 1: Configuration and Basic Setup

**Goal:** Verify configuration loading and basic service creation.

**Tests:**
1. Configuration file loading
2. Environment variable integration
3. Basic service registration
4. Container functionality

**Files to create:**
- `scripts/test_config.py`
- `scripts/test_container.py`

### Phase 2: Individual Service Testing

**Goal:** Test each service type in isolation.

**Tests:**
1. Contract registry and manager
2. RPC client connectivity
3. Storage handler operations
4. Decoder service creation

**Files to create:**
- `scripts/test_contracts.py`
- `scripts/test_rpc.py`
- `scripts/test_storage.py`

### Phase 3: Integration Testing

**Goal:** Test service interactions and data flow.

**Tests:**
1. Full block decoding pipeline
2. Transformation system
3. End-to-end block processing

**Files to create:**
- `scripts/test_pipeline.py`
- `scripts/test_transformers.py`

### Phase 4: Deployment Testing

**Goal:** Verify deployment configurations work.

**Tests:**
1. Docker container build
2. Environment variable handling
3. Production configuration validation

## Detailed Test Scripts

### Phase 1 Tests

#### Test 1: Configuration Loading

**File:** `scripts/test_config.py`

**Purpose:** Verify configuration system works correctly.

**Test cases:**
- Load config from JSON file
- Environment variable integration
- Configuration validation
- Error handling for missing files/variables

#### Test 2: Container Functionality

**File:** `scripts/test_container.py`

**Purpose:** Verify dependency injection container works.

**Test cases:**
- Service registration
- Dependency resolution
- Singleton behavior
- Factory function execution

### Phase 2 Tests

#### Test 3: Contract Services

**File:** `scripts/test_contracts.py`

**Purpose:** Test contract registry and manager.

**Test cases:**
- Contract loading from configuration
- ABI file reading
- Web3 contract instance creation
- Contract caching behavior

#### Test 4: RPC Connectivity

**File:** `scripts/test_rpc.py`

**Purpose:** Test RPC client functionality.

**Test cases:**
- Connection to RPC endpoint
- Latest block number retrieval
- Block data fetching
- Receipt retrieval

#### Test 5: Storage Operations

**File:** `scripts/test_storage.py`

**Purpose:** Test GCS storage handler.

**Test cases:**
- GCS connection
- Blob existence checking
- Data upload/download
- Path generation

### Phase 3 Tests

#### Test 6: Decoding Pipeline

**File:** `scripts/test_pipeline.py`

**Purpose:** Test complete block processing pipeline.

**Test cases:**
- Raw block to decoded block transformation
- Transaction and log decoding
- Error handling for invalid data

#### Test 7: Transformation System

**File:** `scripts/test_transformers.py`

**Purpose:** Test event transformation system.

**Test cases:**
- Transformer loading from configuration
- Event processing priority handling
- Domain event generation

## Running Tests

### Prerequisites

1. **Environment Setup:**
   ```bash
   cp .env.example .env
   # Edit .env with test configuration
   ```

2. **Test Configuration:**
   ```bash
   cp config/config.json config/test_config.json
   # Edit test_config.json with test data
   ```

### Execution Order

Run tests in sequence to build confidence:

```bash
# Phase 1: Basic setup
python scripts/test_config.py
python scripts/test_container.py

# Phase 2: Individual services  
python scripts/test_contracts.py
python scripts/test_rpc.py
python scripts/test_storage.py

# Phase 3: Integration
python scripts/test_pipeline.py
python scripts/test_transformers.py
```

### Test Output

Each test script should provide:
- ✅ **Success indicators** for passing tests
- ❌ **Clear error messages** for failures
- 📊 **Summary statistics** (e.g., "Loaded 15 contracts successfully")
- 🔍 **Debug information** for troubleshooting

### Expected Results

#### Phase 1 Success Criteria
- Configuration loads without errors
- All required environment variables are present
- Container can create basic services
- Service dependencies resolve correctly

#### Phase 2 Success Criteria
- All configured contracts load successfully
- RPC client connects and fetches data
- Storage client connects to GCS
- Decoders create without dependency errors

#### Phase 3 Success Criteria
- Complete block can be processed end-to-end
- Transformers load and process events
- No memory leaks or resource issues

## Troubleshooting Guide

### Common Issues

#### Configuration Errors
```
❌ FileNotFoundError: config/config.json not found
🔧 Solution: Ensure config file exists and path is correct
```

#### Environment Variable Issues
```
❌ KeyError: 'INDEXER_DB_USER' 
🔧 Solution: Check .env file and environment variable names
```

#### ABI Loading Errors
```
❌ FileNotFoundError: config/abis/tokens/blub.json not found
🔧 Solution: Verify ABI files exist at configured paths
```

#### RPC Connection Issues
```
❌ ConnectionError: Failed to connect to RPC endpoint
🔧 Solution: Check INDEXER_AVAX_RPC URL and network connectivity
```

#### GCS Authentication Issues
```
❌ google.auth.exceptions.DefaultCredentialsError
🔧 Solution: Set GOOGLE_APPLICATION_CREDENTIALS or provide credentials_path
```

### Debug Mode

Enable debug logging for detailed troubleshooting:

```python
# Add to test scripts
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Test Data Requirements

### Minimal Test Configuration

Create `config/test_config.json` with minimal data:

```json
{
    "name": "Test Indexer",
    "version": "v0.1-test",
    "storage": {
        "rpc_prefix": "test-rpc/",
        "decoded_prefix": "test-decoded/",
        "rpc_format": "test-rpc/block_{}.json",
        "decoded_format": "test-decoded/block_{}.json"
    },
    "contracts": {
        "0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd": {
            "name": "Test Token",
            "project": "Test",
            "type": "token",
            "decode": {
                "abi_dir": "tokens",
                "abi": "erc20.json"
            }
        }
    },
    "addresses": {}
}
```

### Test Environment Variables

```bash
# Test database (can be same as dev)
INDEXER_DB_USER=test_user
INDEXER_DB_PASSWORD=test_password
INDEXER_DB_NAME=test_indexer
INDEXER_DB_HOST=localhost

# Test RPC (can use public endpoint)
INDEXER_AVAX_RPC=https://api.avax.network/ext/bc/C/rpc

# Test GCS (use test bucket)
INDEXER_GCS_PROJECT_ID=your-test-project
INDEXER_GCS_BUCKET_NAME=your-test-bucket
INDEXER_GCS_CREDENTIALS_PATH=./test-credentials.json
```

## Continuous Integration

For automated testing:

```yaml
# .github/workflows/test.yml
name: Test Suite
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/test_config.py
      - run: python scripts/test_container.py
      - run: python scripts/test_contracts.py
      # Skip RPC/GCS tests in CI (require external services)
```

## Success Metrics

Each phase should achieve:

### Phase 1: Foundation (Critical)
- [ ] Configuration loads from JSON + environment
- [ ] Container creates and manages services
- [ ] All dependencies resolve without circular imports
- [ ] No global state or singleton issues

### Phase 2: Service Validation (Important)
- [ ] Contract registry loads all configured contracts
- [ ] RPC client successfully connects and fetches data
- [ ] Storage handler connects to GCS (if credentials available)
- [ ] All decoder services instantiate without errors

### Phase 3: Pipeline Integration (Nice to Have)
- [ ] Complete block processes end-to-end
- [ ] Transformers execute without errors
- [ ] Data persists correctly to storage

**Minimum Success Criteria:** Phase 1 and 2 must pass for the refactor to be considered successful.