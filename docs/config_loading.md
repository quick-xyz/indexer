# Configuration Loading

The indexer uses a YAML-based configuration system that loads all entity types (addresses, tokens, contracts, models, etc.) with automatic dependency ordering. The universal import command processes everything in one operation with strict validation.

## Basic Usage

```bash
# Import all configuration from YAML file
indexer config universal import config_0.yaml

# Preview what would be imported (recommended first)
indexer config universal import config_0.yaml --dry-run

# Import specific entity types individually
indexer config addresses import config_0.yaml
indexer config tokens import config_0.yaml
indexer config models import config_0.yaml
```

The system validates dependencies automatically (addresses before tokens, models before relations) and uses 3-state validation: new entities are created, exact matches are skipped, conflicts cause errors that require explicit updates.