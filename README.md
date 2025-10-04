# Celery Throttle

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://img.shields.io/pypi/v/celery-throttle.svg)](https://pypi.org/project/celery-throttle/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Advanced rate limiting and queue management for Celery workers using Redis-based token bucket algorithms.**

Celery Throttle provides a robust solution for processing tasks with strict rate controls, ensuring efficient resource usage and compliance with API rate limits or processing constraints.

## Why Celery Throttle?

- **🎯 Works with Your Existing Tasks** - Apply rate limiting to any `@app.task` function without modification
- **📝 Named Queues** - Use meaningful names like `"email_notifications"` instead of auto-generated UUIDs
- **⚡ Precise Rate Limiting** - Redis Lua scripts ensure atomic, thread-safe rate control
- **🔄 Dynamic Queue Management** - Create, modify, and remove queues on-the-fly
- **💥 Burst Control** - Optional burst allowance for handling traffic spikes
- **🏷️ Multi-Service Support** - Redis key prefixing enables multiple isolated services
- **⚙️ Modern Configuration** - Streamlined setup with pydantic-settings

## 📦 Installation

```bash
pip install celery-throttle
```

**Requirements:**

- Python 3.12+
- Redis server
- Celery 5.5+

## 🚀 Quick Start

### 1. Use Your Existing Celery Tasks

```python
from celery import Celery
from celery_throttle import CeleryThrottle

# Your existing Celery app and tasks
app = Celery('myapp')

@app.task
def send_email(to_email, subject, body):
    # Your email sending logic
    return {"status": "sent", "to": to_email}

@app.task
def call_external_api(endpoint, data):
    # Your API calling logic
    return {"status": "success", "endpoint": endpoint}

# Add rate limiting
throttle = CeleryThrottle(celery_app=app)

# Create named queues with different rate limits
throttle.create_queue("10/1m", "email_queue")      # 10 emails per minute
throttle.create_queue("100/1h", "api_queue")       # 100 API calls per hour

# Submit tasks with rate limiting
throttle.submit_task("email_queue", "myapp.send_email", "user@example.com", "Hello!", "Welcome!")
throttle.submit_task("api_queue", "myapp.call_external_api", "/users/123", {"action": "update"})
```

### 2. Run the Workers

You need two components:

```bash
# Terminal 1: Celery worker (processes the actual tasks)
celery -A myapp worker --loglevel=info --prefetch-multiplier=1

# Terminal 2: Rate limiter dispatcher (manages the queues)
celery-throttle dispatcher --celery-app=myapp:app
```

**Important Worker Settings:**

- `--prefetch-multiplier=1` - **Required** to prevent task prefetching which defeats rate limiting

That's it! Your tasks are now rate-limited.

## 📋 Rate Limit Formats

Flexible rate limit syntax with various time units and optional burst allowances.

### Time Units

- **Seconds**: `"10/60s"` (10 requests per 60 seconds)
- **Minutes**: `"10/5m"` (10 requests per 5 minutes)
- **Hours**: `"4000/3h"` (4000 requests per 3 hours)

### Format Pattern

```plaintext
requests/period[time_unit][:burst_allowance]
```

- `requests`: Number of requests allowed
- `period`: Time period value
- `time_unit`: `s` (seconds), `m` (minutes), or `h` (hours)
- `burst_allowance`: Optional burst token capacity (defaults to 1)

### Examples

```python
# Basic rate limits
throttle.create_queue("10/60s", "queue1")     # 10 requests per 60 seconds
throttle.create_queue("5/2m", "queue2")       # 5 requests per 2 minutes
throttle.create_queue("1000/1h", "queue3")    # 1000 requests per 1 hour

# Rate limits with burst allowance
throttle.create_queue("10/60s:5", "queue4")   # 10/min with 5 burst tokens
throttle.create_queue("100/1h:20", "queue5")  # 100/hour with 20 burst tokens
```

### Distribution Modes

#### **Smooth Distribution (Default)**

Tasks are distributed evenly over time to prevent resource bursting:

```python
throttle.create_queue("10/60s")   # 1 task every 6 seconds
throttle.create_queue("100/1h")   # 1 task every 36 seconds
```

#### **Burst Allowance (Optional)**

Allow traffic spikes while maintaining overall rate limits:

```python
throttle.create_queue("10/60s:5")   # Allow up to 5 immediate tasks, then smooth distribution
```

## 🔧 Configuration

### Simple Configuration

```python
from celery_throttle import CeleryThrottle

# Load from environment variables (CELERY_THROTTLE_* prefix)
throttle = CeleryThrottle(celery_app=app)

# Or use kwargs
throttle = CeleryThrottle(
    celery_app=app,
    target_queue="my-rate-limited-queue",
    queue_prefix="myapp"
)
```

### Advanced Configuration

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig

config = CeleryThrottleConfig(
    app_name="my-rate-limited-app",
    target_queue="rate-limited-queue",
    queue_prefix="my_app",
    redis=RedisConfig(
        host="localhost",
        port=6379,
        db=1
    )
)

throttle = CeleryThrottle(celery_app=app, config=config)
```

### Environment Variables

```bash
export CELERY_THROTTLE_REDIS_HOST=localhost
export CELERY_THROTTLE_REDIS_PORT=6379
export CELERY_THROTTLE_REDIS_DB=1
export CELERY_THROTTLE_APP_NAME=my-app
export CELERY_THROTTLE_TARGET_QUEUE=rate-limited-tasks
export CELERY_THROTTLE_QUEUE_PREFIX=my_app
```

**Configuration Precedence:**

1. Kwargs (highest priority)
2. Config object
3. Environment variables
4. Default values

See [CONFIGURATION.md](CONFIGURATION.md) for detailed configuration options.

## 🎮 Command Line Interface

### Dispatcher

```bash
# Start the task dispatcher
celery-throttle dispatcher --celery-app=myapp:app

# With custom Redis settings
celery-throttle --redis-host=localhost --redis-port=6379 dispatcher --celery-app=myapp:app

# With custom interval (default: 0.1s)
celery-throttle dispatcher --celery-app=myapp:app --interval=0.5
```

### Queue Management

```bash
# Create queues
celery-throttle queue create "10/1m"                  # Auto-generated name
celery-throttle queue create "5/30s:3"                # With burst allowance

# List all queues
celery-throttle queue list

# Show queue details
celery-throttle queue show <queue-name>

# Update rate limit
celery-throttle queue update <queue-name> "20/1m"

# Activate/deactivate queues
celery-throttle queue activate <queue-name>
celery-throttle queue deactivate <queue-name>

# Remove queues
celery-throttle queue remove <queue-name>
celery-throttle queue cleanup-empty                   # Remove empty queues
celery-throttle queue cleanup-all                     # Remove all queues
```

## 📊 Monitoring & Statistics

```python
# Get queue statistics
stats = throttle.get_queue_stats("email_queue")
print(f"Waiting: {stats.tasks_waiting}")
print(f"Processing: {stats.tasks_processing}")
print(f"Completed: {stats.tasks_completed}")
print(f"Failed: {stats.tasks_failed}")

# Get rate limit status
status = throttle.get_rate_limit_status("email_queue")
print(f"Available tokens: {status['available_tokens']}")
print(f"Next token in: {status['next_token_in']} seconds")

# List all queues
for queue in throttle.list_queues():
    status = "active" if queue.active else "inactive"
    print(f"{queue.name}: {queue.rate_limit} ({status})")
```

## 🏗️ Architecture

### How It Works

1. **Queue Creation** - Queues are created with rate limits and stored in Redis
2. **Task Submission** - Tasks are processed immediately or queued based on token availability
3. **Token Management** - Redis Lua scripts atomically manage token buckets for precise rate limiting
4. **Task Dispatch** - Background dispatcher efficiently schedules queued tasks when tokens become available
5. **Worker Processing** - Celery workers process tasks with strict rate limit compliance

### Components

- **TokenBucketRateLimiter** - Atomic Redis-based rate limiting with Lua scripts
- **UniversalQueueManager** - Dynamic queue creation and lifecycle management
- **RateLimitedTaskProcessor** - Celery integration with injectable app support
- **RateLimitedTaskSubmitter** - Task submission with rate limit checking
- **RateLimitedTaskDispatcher** - Efficient scheduling of queued tasks
- **CLI** - Complete command-line management interface

## 📚 Common Use Cases

### API Rate Limiting

```python
# Different API endpoints with different limits
throttle.create_queue("300/15m", "twitter_api")       # Twitter limit
throttle.create_queue("5000/1h", "github_api")        # GitHub limit
throttle.create_queue("1/1s", "slack_api")            # Slack limit

# Submit tasks
throttle.submit_task("twitter_api", "myapp.post_tweet", "Hello world")
throttle.submit_task("github_api", "myapp.create_issue", "bug", "Fix this")
throttle.submit_task("slack_api", "myapp.send_message", "general", "Hi!")
```

### Batch Processing

```python
# Process large dataset over time
throttle.create_queue("100/5m", "batch_processing")

# Submit batch of tasks
tasks = [
    ("myapp.process_item", (i, f"item_{i}"), {})
    for i in range(5000)
]
results = throttle.submit_multiple_tasks("batch_processing", tasks)

print(f"Immediately processed: {results['submitted']}")
print(f"Queued for later: {results['queued']}")
```

### Multiple Services

```python
# Service A - Email notifications
email_config = CeleryThrottleConfig(
    app_name="email_service",
    target_queue="email_rate_limited",
    queue_prefix="email"
)
email_throttle = CeleryThrottle(celery_app=app, config=email_config)
email_throttle.create_queue("100/1m", "notifications")

# Service B - API calls
api_config = CeleryThrottleConfig(
    app_name="api_service",
    target_queue="api_rate_limited",
    queue_prefix="api"
)
api_throttle = CeleryThrottle(celery_app=app, config=api_config)
api_throttle.create_queue("1000/1h", "external_calls")

# Each service has isolated Redis keys and Celery queues
```

See [EXAMPLES.md](EXAMPLES.md) for more detailed examples.

## 🔌 Advanced: Custom Task Processing

You can extend `RateLimitedTaskProcessor` to add instrumentation, metrics, or custom logging:

```python
from celery_throttle import CeleryThrottle, RateLimitedTaskProcessor
import time

class MetricsTaskProcessor(RateLimitedTaskProcessor):
    def execute_task(self, queue_name: str, task_name: str, args: tuple, kwargs: dict):
        # Add metrics, timing, custom logging, etc.
        start_time = time.time()
        logger.info(f"⏱️  Starting {task_name} from {queue_name}")

        try:
            result = super().execute_task(queue_name, task_name, args, kwargs)
            duration = time.time() - start_time
            logger.info(f"✅ Completed {task_name} in {duration:.2f}s")
            # Send metrics to your monitoring system
            return result
        except Exception as e:
            logger.error(f"❌ Failed {task_name}: {e}")
            # Send error metrics
            raise

# Use custom processor
throttle = CeleryThrottle(celery_app=app, task_processor_cls=MetricsTaskProcessor)
```

**Note:** This is optional and only needed if you want to instrument task execution. For most use cases, the default processor is sufficient.

## 🛡️ Best Practices

### Worker Configuration

For optimal rate limiting, use these Celery worker settings:

```bash
celery -A myapp worker \
  --prefetch-multiplier=1 \
  --without-mingle \
  --without-gossip \
  --loglevel=info
```

**Why these settings?**

- `--prefetch-multiplier=1` - **Required** - Prevents workers from prefetching multiple tasks
- `--without-mingle` - Improves startup time
- `--without-gossip` - Reduces network chatter

### Queue-Specific Workers

For better isolation, run dedicated workers:

```bash
# Worker for rate-limited tasks only
celery -A myapp worker --queues=rate-limited-queue --prefetch-multiplier=1

# Worker for regular tasks
celery -A myapp worker --queues=celery
```

### Multiple Services on Same Redis

Use different queue prefixes to isolate services:

```python
# Service A
throttle_a = CeleryThrottle(celery_app=app, queue_prefix="service_a")

# Service B
throttle_b = CeleryThrottle(celery_app=app, queue_prefix="service_b")

# Redis keys: service_a:* and service_b:* are completely isolated
```

## 🧪 Testing

```bash
# Install development dependencies
pip install celery-throttle[dev]

# Run tests
pytest

# Run with coverage
pytest --cov=celery_throttle

# Run specific test file
pytest tests/test_rate_limiter.py -v
```

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📖 Documentation

- [Quick Reference](QUICKREF.md) - Fast lookup for common operations
- [Configuration Guide](CONFIGURATION.md) - Detailed configuration options
- [Examples](EXAMPLES.md) - Comprehensive usage examples
- [Changelog](CHANGELOG.md) - Version history and changes
- [API Reference](docs/API.md) - Complete API documentation (coming soon)

## 🔗 Links

- [GitHub Repository](https://github.com/xtream1101/celery-throttle)
- [Issue Tracker](https://github.com/xtream1101/celery-throttle/issues)
- [PyPI Package](https://pypi.org/project/celery-throttle/)
