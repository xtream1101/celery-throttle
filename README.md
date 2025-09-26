# Advanced Celery Rate Limiting and Queue Management

A robust Redis-based rate-limiting system for processing tasks with strict rate controls using Celery workers and token bucket algorithms.

## Features

- **Dynamic Queue Creation**: Create queues on-the-fly with unique names (`batch_{uuid}`)
- **Smooth Rate Limiting**: Even distribution by default - prevents bursting and ensures consistent request spacing
- **Atomic Operations**: Redis Lua scripts ensure thread-safe rate limiting
- **Real-time Monitoring**: Live monitoring of queue statistics and rate limit status
- **Resilient Design**: Handles Redis failures, worker crashes, and system restarts gracefully
- **Efficient Worker Usage**: Workers only pull tasks they can process immediately
- **Flexible Rate Limits**: Support for very fast to very slow queues (e.g., 1 task per hour)
- **Multiple Time Units**: Support for seconds (s), minutes (m), and hours (h) in rate limit specifications

## Prerequisites

- Python 3.12+
- Redis server
- UV package manager

## Installation

1. Install dependencies:

   ```bash
   uv sync
   ```

2. Start Redis server:

   ```bash
   docker run -p 6379:6379 -d redis
   ```

## Quick Start

### 1. Start the worker(s)

You can run as many of these as you want, but for development start with one, maybe bump the concurrency to 2 or 4.

```bash
uv run celery -A src.tasks worker --loglevel=info --without-mingle --without-gossip --concurrency=1

# or

uv run scripts/run_worker.py
```

### 2. Start the dispatcher

Can run as many as needed, but for development start with one.

```bash
uv run scripts/run_dispatcher.py
```

### 3. Start the Monitor (optional)

```bash
uv run scripts/monitor.py
```

### 4. Run the demo

```bash
uv run examples/demo_simple.py
# or
uv run examples/demo.py
```

**Manual Queue Management:**

```bash
# Create a queue (5 tasks per minute)
uv run scripts/queue_manager_cli.py create "5/1m"

# List queues
uv run scripts/queue_manager_cli.py list

# Submit test tasks
uv run scripts/queue_manager_cli.py test <queue_name> 10

# Show queue details
uv run scripts/queue_manager_cli.py show <queue_name>
```

## System Architecture

### Components

1. **Token Bucket Rate Limiter** (`src/rate_limiter.py`)
   - Implements strict rate limiting using Redis and Lua scripts
   - Atomic token acquisition and status checking
   - Configurable refill rates

2. **Queue Manager** (`src/queue_manager.py`)
   - Dynamic queue creation/removal
   - Rate limit configuration management
   - Statistics tracking

3. **Celery Tasks** (`src/tasks.py`)
   - Rate-limited task processing
   - Task dispatcher for queued items
   - Worker configuration with prefetch=1

4. **Monitoring Tools** (`scripts/monitor.py`)
   - Real-time queue statistics
   - Rate limit status display
   - System overview

5. **Management CLI** (`scripts/queue_manager_cli.py`)
   - Queue CRUD operations
   - Rate limit updates
   - Test task submission

### Rate Limit Format

Rate limits are specified as `"requests/period"` with support for multiple time units and optional burst allowance:

#### Time Units Supported

- **Seconds**: `s` (e.g., `"10/60s"`)
- **Minutes**: `m` (e.g., `"10/5m"`)
- **Hours**: `h` (e.g., `"4000/3h"`)

#### Smooth Rate Limiting (Default)

- `"10/60s"` = 10 requests per 60 seconds, evenly distributed (1 every 6s)
- `"10/5m"` = 10 requests per 5 minutes, evenly distributed (1 every 30s)
- `"4000/3h"` = 4000 requests per 3 hours, evenly distributed (1 every 2.7s)
- `"100/1h"` = 100 requests per hour, evenly distributed (1 every 36s)
- `"1/1h"` = 1 request per hour

#### Burst Rate Limiting (Optional)

- `"10/60s:5"` = 10 requests per 60 seconds with up to 5 token burst allowance
- `"4000/3h:50"` = 4000 requests per 3 hours with up to 50 token burst allowance

**Default behavior**: All rate limits use smooth distribution (burst_allowance=1) to prevent bursting and ensure even request spacing over time.

### Queue Naming

Queues are automatically named as `batch_{uuid4}` where uuid4 is a random UUID.

## Usage Examples

### Creating and Managing Queues

```bash
# Create different types of queues
uv run scripts/queue_manager_cli.py create "10/1m"     # Medium rate (smooth)
uv run scripts/queue_manager_cli.py create "1/5m"      # Very slow (1 per 5 min)
uv run scripts/queue_manager_cli.py create "50/10s"    # Fast rate (smooth)
uv run scripts/queue_manager_cli.py create "4000/3h"   # Large batch over 3 hours
uv run scripts/queue_manager_cli.py create "100/1h"    # Hourly processing
uv run scripts/queue_manager_cli.py create "10/60s:5"  # Medium rate with burst allowance

# List all queues
uv run scripts/queue_manager_cli.py list

# Update rate limit
uv run scripts/queue_manager_cli.py update batch_xxx "20/60s"

# Deactivate queue (stops processing but keeps data)
uv run scripts/queue_manager_cli.py deactivate batch_xxx

# Remove queue completely
uv run scripts/queue_manager_cli.py remove batch_xxx
```

### Monitoring

```bash
# Continuous monitoring (updates every 2 seconds)
uv run scripts/monitor.py

# Single report
uv run scripts/monitor.py --once

# Custom update interval
uv run scripts/monitor.py 5.0  # Update every 5 seconds
```

### Programmatic Usage

```python
import redis
from src.queue_manager import UniversalQueueManager
from src.tasks import RateLimitedTaskSubmitter

# Initialize
redis_client = redis.Redis(host='localhost', port=6379, db=0)
queue_manager = UniversalQueueManager(redis_client)
submitter = RateLimitedTaskSubmitter()

# Create queue
queue_name = queue_manager.create_queue("5/1m")

# Submit tasks
task_data = {"message": "Hello, world!", "priority": 1}
submitted = submitter.submit_task(queue_name, task_data)

# Check status
stats = queue_manager.get_queue_stats(queue_name)
rate_status = queue_manager.get_rate_limit_status(queue_name)
```

## Configuration

### Celery Configuration

The system is configured for strict rate limiting:

- `worker_prefetch_multiplier=1`: Workers take one task at a time
- `task_acks_late=True`: Tasks acknowledged after completion
- `worker_concurrency=1`: Single worker process

### Redis Configuration

Default connection: `localhost:6379/0`

Modify in source files if needed:

```python
redis_client = redis.Redis(host='localhost', port=6379, db=0)
```

## Monitoring Data

The monitoring system displays:

### Queue Statistics

- Queue name and rate limit
- Active/inactive status
- Tasks waiting, processing, completed, failed
- Creation timestamp

### Rate Limit Status

- Available tokens vs capacity
- Token refill rate
- Next token availability time

### System Overview

- Total and active queue counts
- Redis connection status

## Error Handling

The system handles:

- Redis connection failures (fails open)
- Worker crashes (tasks return to queue)
- Invalid rate limit formats
- Queue not found errors
- Task processing failures

## Performance Considerations

- **Token Bucket Precision**: Uses floating-point arithmetic for sub-second precision
- **Redis Memory**: Queues expire after 1 hour of inactivity
- **Dispatcher Frequency**: Default 100ms polling (configurable)
- **Worker Efficiency**: No wasted CPU cycles waiting for rate limits

## Development

### Running Tests

```bash
# Run basic functionality test
uv run examples/demo.py

# Run rate limiter unit tests
uv run pytest test_rate_limiter.py -v
```

### Adding New Features

1. Rate limiting logic: `src/rate_limiter.py`
2. Queue management: `src/queue_manager.py`
3. Task processing: `src/tasks.py`
4. Monitoring: `scripts/monitor.py`

## Troubleshooting

### Worker Not Processing Tasks

1. Check Celery worker logs
2. Verify Redis connection
3. Ensure dispatcher is running
4. Check queue active status

### Rate Limiting Not Working

1. Verify Lua script registration
2. Check Redis for rate limit keys: `redis-cli KEYS "rate_limit:*"`
3. Monitor token bucket status in monitoring output

### High Memory Usage

- Queues auto-expire after 1 hour of inactivity
- Clean up completed/failed task data periodically
- Monitor Redis memory: `redis-cli INFO memory`
