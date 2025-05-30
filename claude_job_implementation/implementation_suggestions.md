
## Job Queue Schema
sql
CREATE TABLE processing_jobs (
    id SERIAL PRIMARY KEY,
    block_number BIGINT UNIQUE NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    priority INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    worker_id VARCHAR(50),
    attempts INT DEFAULT 0,
    last_error TEXT,
    INDEX (status, priority, created_at)
);

## Worker Pattern with SKIP LOCKED
python
# Each worker process
def get_next_job():
    return db.execute("""
        UPDATE processing_jobs 
        SET status = 'processing', 
            started_at = NOW(), 
            worker_id = %s,
            attempts = attempts + 1
        WHERE id = (
            SELECT id FROM processing_jobs 
            WHERE status = 'pending'
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING block_number
    """, [worker_id])


## Process management
python
# Simple multiprocessing approach
def start_workers(num_workers=5):
    workers = []
    for i in range(num_workers):
        p = Process(target=worker_loop, args=(f"worker-{i}",))
        workers.append(p)
        p.start()
    return workers

def worker_loop(worker_id):
    while True:
        job = get_next_job(worker_id)
        if job:
            process_block(job.block_number)
        else:
            time.sleep(1)  # No work available