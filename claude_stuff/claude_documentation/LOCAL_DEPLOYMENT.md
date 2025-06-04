# Local Deployment Guide

This guide walks you through setting up the blockchain indexer for local development and testing.

## Prerequisites

### System Requirements
- Python 3.11 or higher
- PostgreSQL 13+ (for database features)
- Git
- 4GB+ RAM
- 10GB+ free disk space

### External Services
- **RPC Endpoint**: QuickNode, Alchemy, or public RPC
- **Google Cloud Storage**: For data persistence (optional for basic testing)
- **Database**: PostgreSQL instance (local or remote)

## Quick Start Setup

### 1. Clone and Setup Project

```bash
# Clone repository
git clone <your-repo-url>
cd blockchain-indexer

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Environment Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit .env file
nano .env
```

**Required Environment Variables:**

```bash
# Database Configuration
INDEXER_DB_USER=your_username
INDEXER_DB_PASSWORD=your_password
INDEXER_DB_NAME=blockchain_indexer
INDEXER_DB_HOST=localhost
INDEXER_DB_PORT=5432

# RPC Configuration
INDEXER_AVAX_RPC=https://your-rpc-endpoint.com

# GCS Configuration (optional for basic testing)
INDEXER_GCS_PROJECT_ID=your-project-id
INDEXER_GCS_BUCKET_NAME=your-bucket-name
INDEXER_GCS_CREDENTIALS_PATH=./credentials/gcs-key.json

# Logging
LOG_LEVEL=INFO
```

### 3. Database Setup

#### Option A: Local PostgreSQL

```bash
# Install PostgreSQL (Ubuntu/Debian)
sudo apt update && sudo apt install postgresql postgresql-contrib

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql
```

```sql
-- In PostgreSQL shell
CREATE USER your_username WITH PASSWORD 'your_password';
CREATE DATABASE blockchain_indexer OWNER your_username;
GRANT ALL PRIVILEGES ON DATABASE blockchain_indexer TO your_username;
\q
```

#### Option B: Docker PostgreSQL

```bash
# Run PostgreSQL in Docker
docker run --name postgres-indexer \
  -e POSTGRES_USER=your_username \
  -e POSTGRES_PASSWORD=your_password \
  -e POSTGRES_DB=blockchain_indexer \
  -p 5432:5432 \
  -d postgres:13

# Verify connection
docker exec -it postgres-indexer psql -U your_username -d blockchain_indexer -c "SELECT version();"
```

### 4. Configuration Files

#### Main Configuration

Create `config/config.json`:

```json
{
    "name": "Local Development Indexer",
    "version": "v0.1-dev",
    "storage": {
        "rpc_prefix": "quicknode/",
        "decoded_prefix": "decoded/",
        "rpc_format": "quicknode/avalanche-mainnet_block_{}.json",
        "decoded_format": "decoded/block_{}.json"
    },
    "contracts": {
        "0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd": {
            "name": "BLUB Token",
            "project": "Blub",
            "type": "token", 
            "decode": {
                "abi_dir": "tokens",
                "abi": "erc20.json"
            },
            "transform": {
                "name": "TokenTransformer",
                "transfers": {
                    "Transfer": 1
                },
                "logs": {},
                "instantiate": {
                    "contract": "0x0f669808d88b2b0b3d23214dcd2a1cc6a8b1b5cd"
                }
            }
        }
    },
    "addresses": {
        "0xe8afa8eb383ced1696c2d8ceb080f47df48f11af": {
            "name": "Blub Treasury",
            "type": "wallet",
            "description": "Main treasury wallet"
        }
    }
}
```

#### ABI Files

Create minimal ABI files for testing:

**`config/abis/tokens/erc20.json`:**
```json
{
    "abi": [
        {
            "type": "event",
            "name": "Transfer",
            "inputs": [
                {"name": "from", "type": "address", "indexed": true},
                {"name": "to", "type": "address", "indexed": true},
                {"name": "value", "type": "uint256", "indexed": false}
            ]
        },
        {
            "type": "function",
            "name": "totalSupply",
            "inputs": [],
            "outputs": [{"name": "", "type": "uint256"}],
            "stateMutability": "view"
        }
    ]
}
```

### 5. GCS Setup (Optional)

#### Option A: Use Service Account Key

```bash
# Create credentials directory
mkdir -p credentials

# Download service account key from Google Cloud Console
# Save as credentials/gcs-key.json

# Set permissions
chmod 600 credentials/gcs-key.json
```

#### Option B: Skip GCS for Initial Testing

Comment out GCS-related tests or use mock storage:

```python
# In test scripts, skip GCS operations
if os.getenv("SKIP_GCS_TESTS"):
    print("Skipping GCS tests")
    return
```

## Verification Steps

### 1. Basic Configuration Test

```bash
python -c "
from indexer import create_indexer
try:
    indexer = create_indexer(config_path='config/config.json')
    print('✅ Configuration loaded successfully')
    print(f'📊 Loaded {len(indexer._config.contracts)} contracts')
except Exception as e:
    print(f'❌ Configuration error: {e}')
"
```

### 2. Database Connection Test

```bash
python -c "
import os
import psycopg2
try:
    conn = psycopg2.connect(
        host=os.getenv('INDEXER_DB_HOST'),
        database=os.getenv('INDEXER_DB_NAME'),
        user=os.getenv('INDEXER_DB_USER'),
        password=os.getenv('INDEXER_DB_PASSWORD')
    )
    print('✅ Database connection successful')
    conn.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"
```

### 3. RPC Connection Test

```bash
python -c "
from indexer import create_indexer
try:
    indexer = create_indexer(config_path='config/config.json')
    rpc = indexer.get('QuickNodeRPCClient')
    latest = rpc.get_latest_block_number()
    print(f'✅ RPC connection successful. Latest block: {latest}')
except Exception as e:
    print(f'❌ RPC connection failed: {e}')
"
```

## Running the Progressive Tests

Once basic setup is complete, run the test suite:

```bash
# Navigate to project directory
cd blockchain-indexer

# Activate virtual environment
source venv/bin/activate

# Run progressive tests
echo "Phase 1: Configuration and Container Tests"
python scripts/test_config.py
python scripts/test_container.py

echo "Phase 2: Service Tests"
python scripts/test_contracts.py
python scripts/test_rpc.py

echo "Phase 3: Integration Tests"
python scripts/test_pipeline.py
```

## Development Workflow

### 1. Daily Development Setup

```bash
# Start development session
cd blockchain-indexer
source venv/bin/activate

# Start database (if using Docker)
docker start postgres-indexer

# Verify setup
python scripts/test_config.py
```

### 2. Making Changes

```bash
# After making code changes, test affected components
python scripts/test_container.py    # If core changes
python scripts/test_contracts.py   # If contract changes
python scripts/test_pipeline.py    # If decoder/transform changes
```

### 3. Adding New Contracts

1. **Add ABI file**: `config/abis/{category}/{contract}.json`
2. **Update config**: Add contract to `config/config.json`
3. **Test loading**: `python scripts/test_contracts.py`

## Troubleshooting

### Common Issues

#### Permission Denied (PostgreSQL)
```bash
# Fix PostgreSQL permissions
sudo -u postgres psql
ALTER USER your_username CREATEDB;
GRANT ALL PRIVILEGES ON DATABASE blockchain_indexer TO your_username;
```

#### Python Module Not Found
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall requirements
pip install -r requirements.txt

# Check Python path
python -c "import sys; print(sys.path)"
```

#### RPC Rate Limiting
```bash
# Use different RPC endpoint or add delays
# Edit .env file with backup RPC URL
INDEXER_AVAX_RPC=https://api.avax.network/ext/bc/C/rpc
```

#### ABI File Errors
```bash
# Validate JSON syntax
python -m json.tool config/abis/tokens/erc20.json

# Check file permissions
ls -la config/abis/tokens/
```

### Performance Optimization

#### For Large Contract Sets
```bash
# Increase Python memory limits
export PYTHONHASHSEED=0
export PYTHONOPTIMIZE=1
```

#### For Development Speed
```bash
# Use minimal configuration for faster startup
cp config/config.json config/minimal_config.json
# Edit minimal_config.json to include only essential contracts
```

## Docker Development (Alternative)

For isolated development environment:

### Dockerfile for Development
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ENV PYTHONPATH=/app

CMD ["python", "scripts/test_config.py"]
```

### Docker Compose for Full Stack
```yaml
# docker-compose.dev.yml
version: '3.8'
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: indexer_user
      POSTGRES_PASSWORD: indexer_pass
      POSTGRES_DB: blockchain_indexer
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  indexer:
    build: .
    depends_on:
      - postgres
    environment:
      INDEXER_DB_HOST: postgres
      INDEXER_DB_USER: indexer_user
      INDEXER_DB_PASSWORD: indexer_pass
      INDEXER_DB_NAME: blockchain_indexer
    volumes:
      - .:/app
      - ./config:/app/config

volumes:
  postgres_data:
```

**Usage:**
```bash
# Start full development stack
docker-compose -f docker-compose.dev.yml up -d

# Run tests in container
docker-compose exec indexer python scripts/test_config.py
```

## Next Steps

After successful local deployment:

1. **Run full test suite** - Ensure all components work
2. **Process test blocks** - Try processing a few recent blocks
3. **Monitor performance** - Check memory usage and processing speed
4. **Add custom transformers** - Implement domain-specific logic
5. **Deploy to staging** - Test in cloud environment

## Getting Help

If you encounter issues:

1. **Check logs**: Look for error messages in terminal output
2. **Verify configuration**: Ensure all environment variables are set
3. **Test incrementally**: Use the progressive test scripts to isolate issues
4. **Check dependencies**: Ensure all external services are accessible
5. **Review documentation**: Refer to API_REFERENCE.md and ARCHITECTURE.md