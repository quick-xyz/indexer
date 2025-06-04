# PostgreSQL Multi-Worker Pipeline Development Plan

## Overview

This document outlines the implementation plan for a PostgreSQL-based job queue system with multi-process workers for blockchain block processing. The system provides a simple, reliable foundation that can scale before needing more complex infrastructure like Kafka.

## Architecture Components

### 1. Job Queue (`job_queue.py`)
- **PostgreSQL-based** job storage with ACID guarantees
- **SKIP LOCKED** for concurrent worker access
- **Retry logic** with exponential backoff
- **Priority support** for urgent blocks
- **Comprehensive monitoring** and cleanup

### 2. Worker System (`worker.py`)
- **Multi-process workers** for true parallelism
- **Graceful shutdown** handling
- **Automatic retries** with failure tracking
- **Health monitoring** and auto-restart

### 3. Pipeline Orchestrator (`orchestrator.py`)
- **Job enqueueing** and monitoring
- **Worker management** lifecycle
- **Automatic block discovery** and enqueueing
- **Status reporting** and metrics

### 4. Management Interface (`manager.py`)
- **CLI tool** for operations
- **Multiple processing modes** (continuous, range, single)
- **Status monitoring** and cleanup utilities

## Development Phases

### Phase 1: Core Infrastructure (Week 1)

#### Day 1-2: Database Schema and Job Queue
```sql
-- Create job queue table
CREATE TABLE processing_jobs (
    id SERIAL PRIMARY KEY,
    block_number BIGINT UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    worker_id VARCHAR(50),
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);
```

**Tasks:**
- [ ] Implement `JobQueue` class with PostgreSQL backend
- [ ] Add database schema creation and migration
- [ ] Implement job enqueueing and dequeueing with SKIP LOCKED
- [ ] Add basic retry and cleanup functionality
- [ ] Write unit tests for job queue operations

**Success Criteria:**
- Can enqueue and dequeue jobs without conflicts
- SKIP LOCKED prevents worker collisions
- Failed jobs are properly marked for retry
- Database schema handles concurrent access

#### Day 3-4: Single Worker Implementation
**Tasks:**
- [ ] Implement `BlockProcessor` for end-to-end block processing
- [ ] Create `Worker` class with job polling loop
- [ ] Add graceful shutdown handling (SIGTERM/SIGINT)
- [ ] Integrate with existing decoder and transformer services
- [ ] Add comprehensive error handling and logging

**Success Criteria:**
- Single worker can process blocks end-to-end
- Worker properly updates job status
- Graceful shutdown works correctly
- Error handling prevents worker crashes

#### Day 5: Basic Testing and Integration
**Tasks:**
- [ ] Integration tests with real blockchain data
- [ ] Test worker failure scenarios
- [ ] Verify job queue behavior under load
- [ ] Performance baseline measurements

### Phase 2: Multi-Worker System (Week 2)

#### Day 1-2: Worker Manager
**Tasks:**
- [ ] Implement `WorkerManager` for process lifecycle
- [ ] Add worker health monitoring and restart logic
- [ ] Handle worker process creation and termination
- [ ] Add configuration for worker count

**Success Criteria:**
- Can start/stop multiple worker processes
- Dead workers are automatically restarted
- Clean shutdown of all workers
- No resource leaks or zombie processes

#### Day 3-4: Pipeline Orchestrator
**Tasks:**
- [ ] Implement `PipelineOrchestrator` for high-level management
- [ ] Add automatic block discovery and enqueueing
- [ ] Implement monitoring and status reporting
- [ ] Add continuous processing mode

**Success Criteria:**
- Automatic enqueueing of new blocks
- Real-time status monitoring
- Configurable processing parameters
- Handles RPC failures gracefully

#### Day 5: CLI Management Interface
**Tasks:**
- [ ] Create CLI tool for pipeline management
- [ ] Add commands for different processing modes
- [ ] Implement status dashboard and monitoring
- [ ] Add cleanup and maintenance utilities

### Phase 3: Production Features (Week 3)

#### Day 1-2: Advanced Job Management
**Tasks:**
- [ ] Priority-based job processing
- [ ] Bulk job enqueueing optimizations
- [ ] Advanced retry strategies (exponential backoff)
- [ ] Job metadata and result tracking

#### Day 3-4: Monitoring and Observability
**Tasks:**
- [ ] Detailed metrics collection
- [ ] Performance monitoring dashboard
- [ ] Alert system for failures
- [ ] Log aggregation and analysis

#### Day 5: Performance Optimization
**Tasks:**
- [ ] Database query optimization
- [ ] Connection pooling
- [ ] Batch processing optimizations
- [ ] Memory usage optimization

### Phase 4: Deployment and Operations (Week 4)

#### Day 1-2: Docker and Deployment
**Tasks:**
- [ ] Dockerfile for pipeline workers
- [ ] Docker Compose for local development
- [ ] Kubernetes manifests for cloud deployment
- [ ] Environment-specific configurations

#### Day 3-4: Production Hardening
**Tasks:**
- [ ] Comprehensive error recovery
- [ ] Database backup and recovery procedures
- [ ] Zero-downtime deployment strategies
- [ ] Capacity planning and scaling guidelines

#### Day 5: Documentation and Training
**Tasks:**
- [ ] Operations runbook
- [ ] Troubleshooting guide
- [ ] Performance tuning documentation
- [ ] Team training materials

## Usage Examples

### Continuous Processing
```bash
# Start continuous processing with 5 workers
python -m indexer.pipeline.manager --config config/prod.json start --workers 5
```

### Range Processing
```bash
# Process specific block range
python -m indexer.pipeline.manager range 1000000 1001000 --workers 3
```

### Single Block Processing
```bash
# Process single block for testing
python -m indexer.pipeline.manager single 1000000
```

### Status Monitoring
```bash
# Check pipeline status
python -m indexer.pipeline.manager status

# Cleanup stale jobs
python -m indexer.pipeline.manager cleanup
```

## Configuration Example

```json
{
  "pipeline": {
    "workers": 5,
    "check_interval": 30,
    "max_attempts": 3,
    "timeout_minutes": 30,
    "priority_blocks": 10,
    "auto_enqueue": true
  },
  "database": {
    "pool_size": 20,
    "max_overflow": 30,
    "statement_timeout": 300
  }
}
```

## Performance Expectations

### Target Metrics
- **Throughput**: 1000-5000 blocks/hour (depending on complexity)
- **Latency**: < 30 seconds per block
- **Worker Utilization**: > 80%
- **Error Rate**: < 1%

### Scaling Characteristics
- **5 Workers**: ~2000 blocks/hour
- **10 Workers**: ~4000 blocks/hour  
- **Database**: Can handle 50+ concurrent workers
- **Memory**: ~500MB per worker process

## Migration to Kafka (Future)

When scaling beyond PostgreSQL job queue:

### Phase 1: Hybrid Approach
- Keep PostgreSQL for job status tracking
- Add Kafka for high-throughput block streaming
- Maintain backward compatibility

### Phase 2: Full Kafka Migration
- Move to Kafka Streams for processing
- Use Kafka Connect for data persistence
- Implement exactly-once processing semantics

## Monitoring and Alerting

### Key Metrics
- Queue depth (pending jobs)
- Processing rate (blocks/minute)
- Worker health (active/failed workers)
- Error rates (failed jobs percentage)
- Processing latency (time per block)

### Alert Conditions
- Queue depth > 1000 (processing lag)
- Error rate > 5% (system issues)
- No active workers (system down)
- Processing latency > 60s (performance issues)

## Testing Strategy

### Unit Tests
- Job queue operations
- Worker lifecycle management
- Error handling scenarios
- Database transactions

### Integration Tests
- Multi-worker coordination
- End-to-end block processing
- Failure recovery scenarios
- Performance under load

### Load Tests
- High job enqueueing rates
- Multiple worker scaling
- Database connection limits
- Memory usage patterns

## Risk Mitigation

### Database Issues
- Connection pool exhaustion → Implement proper pooling
- Lock contention → Optimize SKIP LOCKED queries
- Storage growth → Implement job archival

### Worker Issues
- Memory leaks → Regular worker restarts
- Process crashes → Health monitoring and restart
- Resource exhaustion → Resource limits and monitoring

### Performance Issues
- Slow block processing → Optimize transformation logic
- High queue latency → Increase worker count
- Database bottlenecks → Query optimization and indexing

## Success Criteria

### Phase 1 Success
- [ ] Single worker processes blocks reliably
- [ ] Job queue handles concurrent access
- [ ] Basic error handling and retry works
- [ ] Integration with existing services complete

### Phase 2 Success
- [ ] Multiple workers process blocks in parallel
- [ ] No job conflicts or duplicate processing
- [ ] Automatic scaling and health monitoring
- [ ] Real-time status monitoring working

### Phase 3 Success
- [ ] Production-ready error handling
- [ ] Performance meets target metrics
- [ ] Comprehensive monitoring and alerting
- [ ] Operations tooling complete

### Final Success
- [ ] System processes 2000+ blocks/hour reliably
- [ ] < 1% error rate in production
- [ ] Zero-downtime deployments possible
- [ ] Team can operate system independently

This plan provides a solid foundation for building a scalable, reliable blockchain indexing pipeline that can grow with your needs while maintaining simplicity and operational ease.