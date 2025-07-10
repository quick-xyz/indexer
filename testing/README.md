# Testing Module

A focused, development-oriented testing infrastructure for the blockchain indexer.

## Purpose

This testing module provides:
- Quick diagnostics to verify DI container setup
- Database connection verification  
- Pipeline component testing
- System health checks for troubleshooting

This is NOT a comprehensive test suite - it's designed for rapid development iteration and debugging.

## Structure

```
testing/
├── __init__.py              # Testing environment setup with DI container
├── README.md               # This file
├── diagnostics/
│   ├── __init__.py
│   ├── di_diagnostic.py    # DI container and service initialization checks
│   ├── db_diagnostic.py    # Database connection and schema verification
│   ├── pipeline_diagnostic.py  # Pipeline component health checks
│   └── system_diagnostic.py    # Overall system health check
├── pipeline/
│   ├── __init__.py
│   ├── test_block_processing.py  # Test processing a single block
│   └── test_transaction.py       # Test processing a specific transaction
├── tools/
│   ├── __init__.py
│   └── db_inspector.py     # Database inspection tool (moved from root)
└── utils/
    ├── __init__.py
    └── test_helpers.py     # Common test utilities
```

## Usage

### Quick System Check
```bash
# Run all diagnostics
python -m testing.diagnostics.system_diagnostic

# Check specific components
python -m testing.diagnostics.di_diagnostic
python -m testing.diagnostics.db_diagnostic
python -m testing.diagnostics.pipeline_diagnostic
```

### Pipeline Testing
```bash
# Test processing a specific block
python -m testing.pipeline.test_block_processing 12345678

# Test a specific transaction
python -m testing.pipeline.test_transaction 0xabc123... 12345678
```

### Database Inspection
```bash
# Inspect database schema and data
python -m testing.tools.db_inspector
```

## Key Principles

1. **Focused Output**: Clear, concise output that helps identify issues quickly
2. **No External Dependencies**: Uses only the indexer's existing DI container
3. **Development-Oriented**: Designed for debugging, not production testing
4. **Minimal Setup**: Works with existing configuration, no special test databases

## Common Issues and Solutions

### DI Container Issues
- Check environment variables are set correctly
- Verify database migrations have been run
- Ensure configuration has been imported

### Pipeline Issues  
- Verify GCS credentials and bucket access
- Check RPC endpoint connectivity
- Ensure transform registry has transformers loaded

### Database Issues
- Check connection credentials
- Verify both shared and model databases exist
- Ensure schema migrations are up to date