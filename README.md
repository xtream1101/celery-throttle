# Advanced Celery Rate Limiting and Queue Management

A robust Redis-based rate-limiting system for processing tasks with strict rate controls using Celery workers and token bucket algorithms.

## Features

- **Dynamic Queue Creation**: Create queues on-the-fly with unique names (`batch_{uuid}`)
- **Strict Rate Limiting**: No bursting - enforces rate limits precisely using token bucket algorithm
- **Atomic Operations**: Redis Lua scripts ensure thread-safe rate limiting
- **Real-time Monitoring**: Live monitoring of queue statistics and rate limit status
- **Resilient Design**: Handles Redis failures, worker crashes, and system restarts gracefully
- **Efficient Worker Usage**: Workers only pull tasks they can process immediately
- **Flexible Rate Limits**: Support for very fast to very slow queues (e.g., 1 task per hour)

## Prerequisites

- Python 3.12+
- Redis server
- UV package manager

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd rate-limit-tests
```

2. Install dependencies:
```bash
uv sync
```

3. Start Redis server:
```bash
# On macOS with Homebrew
brew services start redis

# On Linux
sudo systemctl start redis

# Or run manually
redis-server
```

## Quick Start

### 1. Run the Demo

First, test the system with the built-in demo:

```bash
python examples/demo.py
```

This will demonstrate:
- Creating queues with different rate limits
- Submitting tasks
- Rate limiting behavior
- Queue management operations

### 2. Start the System Components

Open 4 terminal windows:

**Terminal 1 - Start Celery Worker:**
```bash
python scripts/run_worker.py
```

**Terminal 2 - Start Task Dispatcher:**
```bash
python scripts/run_dispatcher.py
```

**Terminal 3 - Start Monitoring:**
```bash
python scripts/monitor.py
```

**Terminal 4 - Queue Management:**
```bash
# Create a queue (5 tasks per 60 seconds)
python scripts/queue_manager_cli.py create "5/60s"

# List queues
python scripts/queue_manager_cli.py list

# Submit test tasks
python scripts/queue_manager_cli.py test <queue_name> 10

# Show queue details
python scripts/queue_manager_cli.py show <queue_name>
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

Rate limits are specified as `"requests/period_seconds"`:
- `"10/60s"` = 10 requests per 60 seconds
- `"1/3600s"` = 1 request per hour
- `"100/10s"` = 100 requests per 10 seconds

### Queue Naming

Queues are automatically named as `batch_{uuid4}` where uuid4 is a random UUID.

## Usage Examples

### Creating and Managing Queues

```bash
# Create different types of queues
python scripts/queue_manager_cli.py create "10/60s"    # Medium rate
python scripts/queue_manager_cli.py create "1/300s"    # Very slow (1 per 5 min)
python scripts/queue_manager_cli.py create "50/10s"    # Fast rate

# List all queues
python scripts/queue_manager_cli.py list

# Update rate limit
python scripts/queue_manager_cli.py update batch_xxx "20/60s"

# Deactivate queue (stops processing but keeps data)
python scripts/queue_manager_cli.py deactivate batch_xxx

# Remove queue completely
python scripts/queue_manager_cli.py remove batch_xxx
```

### Monitoring

```bash
# Continuous monitoring (updates every 2 seconds)
python scripts/monitor.py

# Single report
python scripts/monitor.py --once

# Custom update interval
python scripts/monitor.py 5.0  # Update every 5 seconds
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
queue_name = queue_manager.create_queue("5/60s")

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
python examples/demo.py
```

### Adding New Features

1. Rate limiting logic: `src/rate_limiter.py`
2. Queue management: `src/queue_manager.py`
3. Task processing: `src/tasks.py`
4. Monitoring: `scripts/monitor.py`

## Troubleshooting

### Redis Connection Issues
```bash
# Check if Redis is running
redis-cli ping

# Should return "PONG"
```

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

## License

[Add your license here]