# Celery Throttle

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Advanced rate limiting and queue management for Celery workers using Redis-based token bucket algorithms.**

Celery Throttle provides a robust solution for processing tasks with strict rate controls, ensuring efficient resource usage and compliance with API rate limits or processing constraints.

## 🚀 Features

- **🔄 Dynamic Queue Creation**: Create and manage queues on-the-fly with configurable rate limits
- **⚡ Atomic Rate Limiting**: Redis Lua scripts ensure thread-safe, precise rate limiting
- **👥 Dedicated Worker Pools**: Isolate rate-limited tasks with dedicated Celery workers
- **🏷️ Queue Isolation**: Redis key prefixing enables multiple isolated setups per instance
- **📊 Real-time Monitoring**: Live monitoring of queue statistics, rate limit status, and worker queue sizes
- **🛡️ Resilient Design**: Handles Redis failures, worker crashes, and system restarts gracefully
- **🎯 Efficient Workers**: Workers only pull tasks they can process immediately - no resource waste
- **🕐 Flexible Time Units**: Support for seconds (s), minutes (m), and hours (h)
- **💥 Burst Control**: Optional burst allowance for handling traffic spikes
- **📈 Smooth Distribution**: Even task distribution prevents resource bursting by default

## 📦 Installation

```bash
pip install celery-throttle
```

**Requirements:**

- Python 3.12+
- Redis server
- Celery 5.5+

## 🏃 Quick Start

### Basic Usage

```python
from celery_throttle import CeleryThrottle

# Initialize with default configuration
throttle = CeleryThrottle()

# Create a rate-limited queue (5 tasks per minute)
queue_name = throttle.create_queue("5/1m")

# Submit tasks
task_data = {"message": "Hello, world!", "user_id": 123}
submitted = throttle.submit_task(queue_name, task_data)

if submitted:
    print("Task submitted for immediate processing")
else:
    print("Task queued due to rate limiting")
```

### With Existing Celery App

```python
from celery import Celery
from celery_throttle import CeleryThrottle

# Your existing Celery app
app = Celery('my-app')
app.conf.update(broker_url='redis://localhost:6379/0')

# Add rate limiting to your existing app
throttle = CeleryThrottle(celery_app=app)

# Your existing tasks continue to work normally
@app.task
def my_existing_task(data):
    return process_data(data)

# Now you can also create rate-limited queues
api_queue = throttle.create_queue("100/1h")  # 100 API calls per hour
```

### Command Line Interface

```bash
# Start the system components
celery-throttle worker                                    # Start Celery worker (all queues)
celery-throttle worker --queues=my_queue                  # Start worker for specific queues
celery-throttle dedicated-worker                          # Start dedicated worker (rate-limited only)
celery-throttle dispatcher                                # Start task dispatcher

# Monitoring and status
celery-throttle monitor                                   # Real-time monitoring (queues + workers)
celery-throttle worker-status                             # Show worker information
celery-throttle worker-status --detailed                  # Detailed worker queue information

# Configuration options
celery-throttle --target-queue=my_queue --queue-prefix=my_app dedicated-worker
celery-throttle --redis-host=localhost --redis-port=6379 dispatcher

# Manage queues
celery-throttle queue create "10/1m"                      # Create queue
celery-throttle queue list                                # List all queues
celery-throttle queue show <queue-name>                   # Show queue details
celery-throttle queue test <queue-name> 5                 # Submit 5 test tasks
```

## 📋 Rate Limit Formats

### Time Units

- **Seconds**: `"10/60s"` (10 requests per 60 seconds)
- **Minutes**: `"10/5m"` (10 requests per 5 minutes)
- **Hours**: `"4000/3h"` (4000 requests per 3 hours)

### Distribution Modes

#### Smooth Distribution (Default)

```python
throttle.create_queue("10/60s")   # 1 task every 6 seconds
throttle.create_queue("100/1h")   # 1 task every 36 seconds
throttle.create_queue("1/5m")     # 1 task every 5 minutes
```

#### Burst Allowance (Optional)

```python
throttle.create_queue("10/60s:5")   # 10/minute with up to 5 burst tokens
throttle.create_queue("100/1h:20")  # 100/hour with up to 20 burst tokens
```

## 🔧 Configuration

### Basic Configuration

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig

config = CeleryThrottleConfig(
    app_name="my-rate-limited-app",
    target_queue="my_rate_limited_tasks",  # Celery queue for rate-limited tasks
    queue_prefix="my_app",                 # Redis key prefix for isolation
    redis=RedisConfig(host="localhost", port=6379, db=1),
    celery={
        "broker_url": "redis://localhost:6379/1",
        "worker_concurrency": 2
    }
)

throttle = CeleryThrottle(config=config)
```

### Dedicated Worker Pools

Set up dedicated workers that only process rate-limited tasks:

```python
# Configure with a dedicated queue
config = CeleryThrottleConfig(
    target_queue="rate_limited_queue",
    queue_prefix="throttle"
)

throttle = CeleryThrottle(config=config)

# Start a worker that only processes rate-limited tasks
throttle.run_dedicated_worker()

# Or specify queues manually
throttle.run_worker(queues=["rate_limited_queue", "other_queue"])
```

### Using Environment Variables

```bash
export CELERY_THROTTLE_REDIS_HOST=localhost
export CELERY_THROTTLE_REDIS_PORT=6379
export CELERY_THROTTLE_REDIS_DB=1
export CELERY_THROTTLE_BROKER_URL=redis://localhost:6379/1
export CELERY_THROTTLE_APP_NAME=my-app
export CELERY_THROTTLE_TARGET_QUEUE=my_rate_limited_queue
export CELERY_THROTTLE_QUEUE_PREFIX=my_app
```

```python
throttle = CeleryThrottle.from_env()
```

### Using Dictionary Configuration

```python
config_dict = {
    "app_name": "my-app",
    "target_queue": "my_rate_limited_queue",
    "queue_prefix": "my_app",
    "redis": {"host": "localhost", "port": 6379, "db": 1},
    "celery": {"worker_concurrency": 4}
}

throttle = CeleryThrottle.from_config_dict(config_dict)
```

## 🔍 Monitoring

### Real-time CLI Monitor

```bash
# Monitor queues and workers in real-time
celery-throttle monitor

# Show current worker status
celery-throttle worker-status

# Show detailed worker queue information
celery-throttle worker-status --detailed
```

### Programmatic Monitoring

#### Queue Statistics

```python
# Get queue statistics
stats = throttle.get_queue_stats(queue_name)
print(f"Waiting: {stats.tasks_waiting}")
print(f"Processing: {stats.tasks_processing}")
print(f"Completed: {stats.tasks_completed}")

# Get rate limit status
status = throttle.get_rate_limit_status(queue_name)
print(f"Available tokens: {status['available_tokens']}")
print(f"Next token in: {status['next_token_in']} seconds")

# List all queues
for queue in throttle.list_queues():
    print(f"{queue.name}: {queue.rate_limit} ({'active' if queue.active else 'inactive'})")
```

#### Worker Monitoring

```python
# Get worker information and queue sizes
worker_info = throttle.get_worker_info()
for worker_name, info in worker_info.items():
    print(f"Worker: {worker_name}")
    print(f"  Status: {info.status}")
    print(f"  Active tasks: {info.active_tasks}")
    print(f"  Reserved tasks: {info.reserved_tasks}")
    print(f"  Queue sizes: {info.queue_sizes}")

# Get worker queue summary
queue_summary = throttle.get_worker_queue_summary()
for queue_name, info in queue_summary.items():
    print(f"Queue {queue_name}: {info.total_size} tasks across {len(info.workers_listening)} workers")

# Check worker infrastructure health
is_healthy = throttle.is_worker_infrastructure_healthy()
worker_count = throttle.get_worker_count()
print(f"Workers: {worker_count} active, Status: {'HEALTHY' if is_healthy else 'UNHEALTHY'}")
```

## 🏗️ Architecture

### Components

1. **Token Bucket Rate Limiter**: Atomic Redis-based rate limiting with Lua scripts
2. **Queue Manager**: Dynamic queue creation, configuration, and lifecycle management
3. **Task Processor**: Celery integration with injectable app support
4. **Task Dispatcher**: Efficient scheduling of queued tasks when tokens are available
5. **CLI Interface**: Complete command-line management and monitoring

### How It Works

1. **Queue Creation**: Queues are created with specific rate limits and stored in Redis
2. **Task Submission**: Tasks are either processed immediately or queued based on token availability
3. **Token Management**: Redis Lua scripts atomically manage token buckets for precise rate limiting
4. **Task Dispatch**: Background dispatcher efficiently schedules queued tasks when tokens become available
5. **Worker Processing**: Celery workers process tasks with strict rate limit compliance

## 📚 Examples

### Worker Pool Isolation

Isolate rate-limited tasks from your regular Celery workers:

```python
from celery import Celery
from celery_throttle import CeleryThrottle, CeleryThrottleConfig

# Your main application with regular tasks
main_app = Celery('main_app')

@main_app.task
def regular_task(data):
    return process_data(data)

# Rate-limited service with dedicated workers
config = CeleryThrottleConfig(
    target_queue="api_rate_limited",
    queue_prefix="api_service"
)

throttle = CeleryThrottle(config=config)
api_queue = throttle.create_queue("100/1h")

# Submit rate-limited tasks
throttle.submit_task(api_queue, {"api_call": "fetch_data"})

```

#### Start workers in separate terminals

#### Terminal 1: Regular tasks only (or use existing celery command)

```bash
celery-throttle worker --queues=celery
```

#### Terminal 2: Rate-limited tasks only

```bash
celery-throttle --target-queue=api_rate_limited --queue-prefix=api_service dedicated-worker
```

#### Terminal 3: Task dispatcher

```bash
celery-throttle --target-queue=api_rate_limited --queue-prefix=api_service dispatcher
```

### Multiple Isolated Services

Run multiple rate-limiting setups in the same Redis instance:

```python
# Service A integration
service_a_config = CeleryThrottleConfig(
    app_name="service_a_service",
    target_queue="service_a_rate_limited",
    queue_prefix="service_a"
)
service_a_throttle = CeleryThrottle(config=service_a_config)
service_a_queue = service_a_throttle.create_queue("300/15m")

# Service B integration
service_b_config = CeleryThrottleConfig(
    app_name="service_b_service",
    target_queue="service_b_rate_limited",
    queue_prefix="service_b"
)
service_b_throttle = CeleryThrottle(config=service_b_config)
service_b_queue = service_b_throttle.create_queue("5000/1h")

# Each service has isolated:
# - Redis keys (service_a:* vs service_b:*)
# - Celery queues (service_a_rate_limited vs service_b_rate_limited)
# - Worker pools (can scale independently)
```

### API Rate Limiting

```python
from celery_throttle import CeleryThrottle

throttle = CeleryThrottle()

# Different API endpoints with different limits
service_a_queue = throttle.create_queue("300/15m")  # Service A limit
service_b_queue = throttle.create_queue("5000/1h")   # Service B limit
service_c_queue = throttle.create_queue("1/1s")       # Service C limit

# Submit API calls
throttle.submit_task(service_a_queue, {"action": "post", "text": "Hello world"})
throttle.submit_task(service_b_queue, {"action": "create_issue", "repo": "my-repo"})
throttle.submit_task(service_c_queue, {"action": "send_message", "channel": "general"})
```

### Batch Processing

```python
# Process large dataset over time
batch_queue = throttle.create_queue("1000/6h")  # 1000 items over 6 hours

# Submit batch of tasks
tasks = [{"item_id": i, "data": f"item_{i}"} for i in range(5000)]
results = throttle.submit_multiple_tasks(batch_queue, tasks)

print(f"Immediately processed: {results['submitted']}")
print(f"Queued for later: {results['queued']}")
```

### Custom Task Processing

```python
import time
from celery_throttle.tasks.processor import RateLimitedTaskProcessor

class CustomTaskProcessor(RateLimitedTaskProcessor):
    def _register_task(self):
        @self.app.task(bind=True)
        def process_rate_limited_task(task_self, queue_name, task_data):
            # Custom processing logic
            if task_data.get("type") == "email":
                return self.send_email(task_data)
            elif task_data.get("type") == "webhook":
                return self.call_webhook(task_data)
            # ... custom logic

        self.process_rate_limited_task = process_rate_limited_task

# Use custom processor
throttle = CeleryThrottle()
custom_processor = CustomTaskProcessor(throttle.app, throttle.redis, throttle.queue_manager)
throttle.task_processor = custom_processor
```

## 🧪 Testing

```bash
# Install development dependencies
pip install celery-throttle[dev]

# Run tests
pytest

# Run with coverage
pytest --cov=celery_throttle

# Run examples
python -m examples.demo
python -m examples.library_usage.basic_usage
```

## 🔧 Development

### Local Development Setup

```bash
# Clone repository
git clone https://github.com/user/celery-throttle.git
cd celery-throttle

# Install in development mode
pip install -e .[dev]

# Start Redis (using Docker)
docker run -p 6379:6379 -d redis

# Run tests
pytest
```

## 🐛 Troubleshooting

### Common Issues

#### Workers not processing tasks

- Check Redis connection: `redis-cli ping`
- Verify Celery worker is running: `celery-throttle worker` or `celery-throttle dedicated-worker`
- Ensure dispatcher is running: `celery-throttle dispatcher`
- Check queue status: `celery-throttle queue list`
- Verify target queue configuration matches worker queues

#### Rate limiting not working

- Verify Redis Lua script support
- Check token bucket status: `celery-throttle queue show <queue-name>`
- Monitor Redis keys: `redis-cli KEYS "rate_limit:*"`

#### High memory usage

- Queues auto-expire after 1 hour of inactivity
- Clean up old queues: `celery-throttle queue cleanup-empty`
- Monitor Redis memory: `redis-cli INFO memory`

### Performance Tuning

- **Worker Concurrency**: Increase for non-rate-limited tasks
- **Dispatcher Interval**: Tune based on your precision needs
- **Redis Configuration**: Optimize for your memory and persistence requirements
- **Token Bucket Precision**: Sub-second precision available for high-frequency limits

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🤝 Acknowledgments

- Built on [Celery](https://github.com/celery/celery) for distributed task processing
- Uses [Redis](https://redis.io/) for atomic operations and persistence
- Implements token bucket algorithm for precise rate limiting
- Inspired by real-world API rate limiting challenges
