# Quick Reference

Fast reference guide for common Celery Throttle operations.

## Installation

```bash
pip install celery-throttle
```

## Basic Setup

```python
from celery import Celery
from celery_throttle import CeleryThrottle

app = Celery('myapp')

@app.task
def my_task(data):
    return {"processed": True}

throttle = CeleryThrottle(celery_app=app)
```

## Rate Limit Formats

| Format | Description | Example |
|--------|-------------|---------|
| `N/Xs` | N requests per X seconds | `10/30s` = 10 per 30 seconds |
| `N/Xm` | N requests per X minutes | `100/5m` = 100 per 5 minutes |
| `N/Xh` | N requests per X hours | `1000/2h` = 1000 per 2 hours |
| `N/Xs:B` | With burst allowance | `10/60s:5` = 10/min, 5 burst |

## Queue Operations

```python
# Create queue
throttle.create_queue("10/1m", "my_queue")

# Submit task
throttle.submit_task("my_queue", "myapp.my_task", {"data": "value"})

# Submit multiple tasks
tasks = [("myapp.my_task", (arg1,), {"kwarg": "value"}) for i in range(100)]
throttle.submit_multiple_tasks("my_queue", tasks)

# Get statistics
stats = throttle.get_queue_stats("my_queue")
print(f"Waiting: {stats.tasks_waiting}, Completed: {stats.tasks_completed}")

# Get rate limit status
status = throttle.get_rate_limit_status("my_queue")
print(f"Available: {status['available_tokens']}, Next in: {status['next_token_in']}s")

# Update rate limit
throttle.update_rate_limit("my_queue", "20/1m")

# Activate/deactivate
throttle.activate_queue("my_queue")
throttle.deactivate_queue("my_queue")

# Remove queue
throttle.remove_queue("my_queue")

# List all queues
for queue in throttle.list_queues():
    print(f"{queue.name}: {queue.rate_limit}")
```

## CLI Commands

### Start Dispatcher

```bash
celery-throttle dispatcher --celery-app=myapp:app
```

### Queue Management

```bash
# Create
celery-throttle queue create "10/1m"

# List
celery-throttle queue list

# Show details
celery-throttle queue show <queue-name>

# Update
celery-throttle queue update <queue-name> "20/1m"

# Remove
celery-throttle queue remove <queue-name>

# Cleanup
celery-throttle queue cleanup-empty
celery-throttle queue cleanup-all
```

## Worker Commands

```bash
# Basic worker (required settings)
celery -A myapp worker --prefetch-multiplier=1

# Recommended production settings
celery -A myapp worker \
  --prefetch-multiplier=1 \
  --without-mingle \
  --without-gossip \
  --loglevel=info

# Queue-specific worker
celery -A myapp worker \
  --queues=rate-limited-queue \
  --prefetch-multiplier=1
```

## Configuration

### Environment Variables

```bash
export CELERY_THROTTLE_REDIS_HOST=localhost
export CELERY_THROTTLE_REDIS_PORT=6379
export CELERY_THROTTLE_REDIS_DB=0
export CELERY_THROTTLE_TARGET_QUEUE=rate-limited-queue
export CELERY_THROTTLE_QUEUE_PREFIX=myapp
```

### Python Config

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig

# Simple
throttle = CeleryThrottle(celery_app=app, queue_prefix="myapp")

# With config object
config = CeleryThrottleConfig(
    target_queue="my-queue",
    queue_prefix="myapp",
    redis=RedisConfig(host="localhost", port=6379, db=1)
)
throttle = CeleryThrottle(celery_app=app, config=config)

# From dict
config_dict = {
    "target_queue": "my-queue",
    "queue_prefix": "myapp",
    "redis": {"host": "localhost", "port": 6379}
}
throttle = CeleryThrottle.from_config_dict(celery_app=app, config_dict=config_dict)
```

## Common Patterns

### API Rate Limiting

```python
throttle.create_queue("100/1m", "api_calls")
throttle.submit_task("api_calls", "myapp.call_api", "/endpoint")
```

### Email Batching

```python
throttle.create_queue("50/1m", "emails")
for user in users:
    throttle.submit_task("emails", "myapp.send_email", user.email, "Subject", "Body")
```

### Batch Processing

```python
throttle.create_queue("1000/5m", "batch")
tasks = [("myapp.process", (i,), {}) for i in range(10000)]
results = throttle.submit_multiple_tasks("batch", tasks)
print(f"Submitted: {results['submitted']}, Queued: {results['queued']}")
```

### Multi-Service Isolation

```python
# Service A
service_a = CeleryThrottle(celery_app=app, queue_prefix="service_a")
service_a.create_queue("100/1m", "tasks")

# Service B
service_b = CeleryThrottle(celery_app=app, queue_prefix="service_b")
service_b.create_queue("200/1m", "tasks")

# Completely isolated in Redis
```

## Troubleshooting

### Tasks Not Processing

1. Check dispatcher is running: `celery-throttle dispatcher --celery-app=myapp:app`
2. Check worker is running with correct settings
3. Verify queue exists: `celery-throttle queue list`
4. Check queue is active: `celery-throttle queue show <queue-name>`

### Rate Limit Too Slow

```python
# Check current status
status = throttle.get_rate_limit_status("my_queue")
print(f"Available tokens: {status['available_tokens']}")

# Update rate limit
throttle.update_rate_limit("my_queue", "200/1m")  # Increase rate
```

### Tasks Backing Up

```python
# Check statistics
stats = throttle.get_queue_stats("my_queue")
print(f"Waiting: {stats.tasks_waiting}")

# Options:
# 1. Increase rate limit
throttle.update_rate_limit("my_queue", "100/1m")

# 2. Add burst allowance
throttle.update_rate_limit("my_queue", "50/1m:20")

# 3. Add more workers
# celery -A myapp worker --concurrency=2
```

### Connection Issues

```python
# Test Redis connection
from celery_throttle.config import RedisConfig
config = RedisConfig(host="localhost", port=6379)
client = config.create_client()
client.ping()  # Should return True
```

## Performance Tips

1. **Use `--prefetch-multiplier=1`** (required for accurate rate limiting)
2. Use queue prefixes to isolate services
3. Set appropriate burst allowances for traffic spikes
4. Monitor queue statistics regularly
5. Use dedicated workers for rate-limited vs regular tasks
6. Consider different Redis DBs for different environments

## See Also

- [Full Documentation](README.md)
- [Configuration Guide](CONFIGURATION.md)
- [Detailed Examples](EXAMPLES.md)
- [Changelog](CHANGELOG.md)