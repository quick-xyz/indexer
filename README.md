# Blockchain-Indexer Framework

## Overview

The blockchain-indexer is a modular, extensible framework designed to process and index blockchain data. It provides a complete pipeline for streaming, decoding, transforming, and storing blockchain data, with a focus on flexibility and scalability.

## Core Modules

### 1. Configuration

**Purpose**: Provides a centralized configuration system for all components.

**Key Components**:
- `ConfigManager`: Central configuration management
- Configuration dataclasses for different components
- Environment variable integration
- Config file loading (JSON/YAML)

### 2. Streamer

**Purpose**: Retrieves raw blockchain data from various sources.

**Key Components**:
- `BlockStreamer`: Streams blocks from RPC nodes
- `RPCClient`: Communicates with Ethereum RPC endpoints
- Listener pattern for processing new blocks
- Polling and websocket support

### 3. Decoder

**Purpose**: Decodes raw blockchain data into structured formats.

**Key Components**:
- `BlockDecoder`: Decodes blocks into a standardized format
- `TransactionDecoder`: Decodes transactions and receipts
- `LogDecoder`: Decodes event logs
- `ContractRegistry`: Manages contract ABIs
- `ContractManager`: Provides Web3 contract interfaces

### 4. Transformer

**Purpose**: Transforms decoded data into business events.

**Key Components**:
- `TransformationManager`: Manages event transformers
- `BaseEventTransformer`: Base class for custom transformers
- `BusinessEvent`: Base class for business events
- Event listeners for processing business events

### 5. Storage

**Purpose**: Manages persistence of raw and processed data.

**Key Components**:
- `StorageInterface`: Common interface for all storage backends
- `LocalStorage`: Local filesystem storage
- `GCSStorage`: Google Cloud Storage
- `BlockHandler`: Unified interface for block storage

### 6. Database

**Purpose**: Tracks processing status and provides querying capabilities.

**Key Components**:
- `ConnectionManager`: Database connection handling
- `BlockRegistry`: Tracks block processing status
- Database models for blocks and events
- Session management and transaction handling

### 7. Pipeline

**Purpose**: Integrates all components into a unified workflow.

**Key Components**:
- `IntegratedPipeline`: Connects all components
- Block processing and error handling
- Continuous processing mode
- Recovery and reprocessing capabilities

## Usage Patterns

1. **Configuration**: Set up the framework with your specific configuration.
2. **Block Processing**: Process individual blocks or ranges of blocks.
3. **Continuous Processing**: Stream and process new blocks as they are produced.
4. **Custom Transformers**: Add your own transformers to extract specific business events.
5. **Event Listeners**: Register listeners to process business events.

## Extension Points

1. **Custom Transformers**: Create your own transformer classes.
2. **Event Listeners**: Implement your own event listener interfaces.
3. **Storage Backends**: Implement custom storage solutions.
4. **Business Events**: Define your own business event types.