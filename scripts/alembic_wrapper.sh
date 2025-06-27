#!/bin/bash
# Alembic wrapper that sets PYTHONPATH correctly

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Set PYTHONPATH to project root
export PYTHONPATH="$PROJECT_ROOT"

# Run alembic with all passed arguments
alembic "$@"
