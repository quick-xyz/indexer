blockchain-indexer/
├── pyproject.toml
├── README.md
├── LICENSE
├── MANIFEST.in
├── setup.py
├── blockchain_indexer/
│   ├── __init__.py
│   ├── __version__.py
│   ├── cli.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── default_config.py
│   │   └── config_manager.py
│   ├── streamer/
│   │   ├── __init__.py
│   │   ├── interfaces.py
│   │   ├── streamer.py
│   │   └── clients/
│   │       ├── __init__.py
│   │       ├── rpc_client.py
│   │       └── websocket_client.py
│   ├── decoder/
│   │   ├── __init__.py
│   │   ├── interfaces.py
│   │   ├── contracts/
│   │   │   ├── __init__.py
│   │   │   ├── manager.py
│   │   │   └── registry.py
│   │   ├── decoders/
│   │   │   ├── __init__.py
│   │   │   ├── block.py
│   │   │   ├── log.py
│   │   │   └── transaction.py
│   │   └── model/
│   │       ├── __init__.py
│   │       ├── block.py
│   │       ├── evm.py
│   │       └── types.py
│   ├── transformer/
│   │   ├── __init__.py
│   │   ├── interfaces.py
│   │   ├── events/
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   └── common_events.py
│   │   ├── framework/
│   │   │   ├── __init__.py
│   │   │   ├── transformer.py
│   │   │   ├── context.py
│   │   │   └── manager.py
│   │   └── listeners/
│   │       ├── __init__.py
│   │       ├── base.py
│   │       ├── database.py
│   │       └── file.py
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── interfaces.py
│   │   ├── base.py
│   │   ├── handler.py
│   │   ├── local.py
│   │   ├── gcs.py
│   │   └── s3.py
│   ├── database/
│   │   ├── __init__.py
│   │   ├── interfaces.py
│   │   ├── db_models/
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   └── status.py
│   │   ├── operations/
│   │   │   ├── __init__.py
│   │   │   ├── manager.py
│   │   │   └── session.py
│   │   └── registry/
│   │       ├── __init__.py
│   │       └── block_registry.py
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── integrated.py
│   │   └── jobs.py
│   └── utils/
│       ├── __init__.py
│       ├── env.py            # Core environment functionality
│       └── logging.py
├── examples/
│   ├── simple_indexer.py
│   └── transformers/
│       ├── __init__.py
│       └── uniswap_v2.py
└── tests/
    ├── __init__.py
    └── test_indexer.py