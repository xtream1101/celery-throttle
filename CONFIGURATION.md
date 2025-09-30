# Configuration Guide

Complete guide to configuring Celery Throttle for your application.

## Table of Contents

- [Overview](#overview)
- [Configuration Methods](#configuration-methods)
- [Configuration Options](#configuration-options)
- [Redis Configuration](#redis-configuration)
- [Celery Configuration](#celery-configuration)
- [Environment Variables](#environment-variables)
- [Configuration Examples](#configuration-examples)

## Overview

Celery Throttle uses [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) for configuration management, providing:

- ✅ Type validation
- ✅ Environment variable support with prefixes
- ✅ Nested configuration objects
- ✅ Multiple configuration methods
- ✅ Clear configuration precedence

### Configuration Precedence

Configuration is resolved in the following order (highest to lowest priority):

1. **Kwargs** - Arguments passed directly to `CeleryThrottle()`
2. **Config Object** - Explicit `CeleryThrottleConfig` instance
3. **Environment Variables** - Variables with `CELERY_THROTTLE_` prefix
4. **Default Values** - Built-in defaults

## Configuration Methods

### Method 1: Simple Initialization (Environment Variables)

The simplest approach - just set environment variables and initialize:

```python
from celery_throttle import CeleryThrottle

# Loads configuration from environment variables
throttle = CeleryThrottle(celery_app=app)
```

```bash
export CELERY_THROTTLE_REDIS_HOST=localhost
export CELERY_THROTTLE_REDIS_PORT=6379
export CELERY_THROTTLE_TARGET_QUEUE=my-queue
```

### Method 2: Kwargs Override

Quick and convenient for small overrides:

```python
from celery_throttle import CeleryThrottle

throttle = CeleryThrottle(
    celery_app=app,
    target_queue="custom-queue",
    queue_prefix="myapp",
    redis={"host": "redis.example.com", "port": 6379}
)
```

### Method 3: Config Object

Explicit and type-safe configuration:

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig

config = CeleryThrottleConfig(
    app_name="my-application",
    target_queue="rate-limited-tasks",
    queue_prefix="myapp",
    redis=RedisConfig(
        host="redis.example.com",
        port=6379,
        db=1,
        password="secret"
    )
)

throttle = CeleryThrottle(celery_app=app, config=config)
```

### Method 4: Config Dictionary

Flexible configuration from dicts (useful for loading from files):

```python
from celery_throttle import CeleryThrottle

config_dict = {
    "app_name": "my-app",
    "target_queue": "rate-limited-queue",
    "queue_prefix": "myapp",
    "redis": {
        "host": "redis.example.com",
        "port": 6379,
        "db": 1
    }
}

throttle = CeleryThrottle.from_config_dict(celery_app=app, config_dict=config_dict)
```

### Method 5: Mixed Approach

Combine config object with kwargs overrides:

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig

# Base configuration
base_config = CeleryThrottleConfig(
    app_name="my-app",
    queue_prefix="production"
)

# Override specific settings
throttle = CeleryThrottle(
    celery_app=app,
    config=base_config,
    target_queue="high-priority-queue"  # Override
)
```

## Configuration Options

### Main Configuration (`CeleryThrottleConfig`)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `app_name` | `str` | `"celery-throttle"` | Application name for logging and identification |
| `target_queue` | `str` | `"rate-limited-queue"` | Celery queue where rate-limited tasks are sent |
| `queue_prefix` | `str` | `"throttle"` | Redis key prefix for isolation (enables multi-service) |
| `redis` | `RedisConfig` | See below | Redis connection configuration |
| `celery` | `CeleryConfig` | See below | Celery application configuration |

## Redis Configuration

### Options (`RedisConfig`)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | `str` | `"localhost"` | Redis server hostname |
| `port` | `int` | `6379` | Redis server port |
| `db` | `int` | `0` | Redis database number (0-15) |
| `password` | `str` | `None` | Redis password (optional) |
| `decode_responses` | `bool` | `False` | Whether to decode responses to strings |

### Examples

**Basic Redis:**
```python
from celery_throttle.config import RedisConfig

redis = RedisConfig(
    host="localhost",
    port=6379,
    db=0
)
```

**Redis with Authentication:**
```python
redis = RedisConfig(
    host="redis.example.com",
    port=6380,
    db=1,
    password="my-secret-password"
)
```

**Redis Cluster/Sentinel:**
```python
# For advanced Redis setups, pass a custom client
import redis

redis_client = redis.RedisCluster(
    host="cluster.example.com",
    port=6379
)

throttle = CeleryThrottle(celery_app=app, redis_client=redis_client)
```

## Celery Configuration

### Options (`CeleryConfig`)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `broker_url` | `str` | `"redis://localhost:6379/0"` | Celery broker URL |
| `result_backend` | `str` | `"redis://localhost:6379/0"` | Celery result backend URL |
| `task_serializer` | `str` | `"json"` | Task serialization format |
| `accept_content` | `list` | `["json"]` | Accepted content types |
| `result_serializer` | `str` | `"json"` | Result serialization format |
| `timezone` | `str` | `"UTC"` | Timezone for scheduled tasks |
| `enable_utc` | `bool` | `True` | Enable UTC timezone |
| `worker_prefetch_multiplier` | `int` | `1` | Worker prefetch multiplier |
| `task_acks_late` | `bool` | `True` | Acknowledge tasks after execution |
| `worker_concurrency` | `int` | `1` | Number of concurrent worker processes |

### Examples

**Custom Celery Settings:**
```python
from celery_throttle.config import CeleryConfig

celery_config = CeleryConfig(
    broker_url="redis://redis.example.com:6379/0",
    result_backend="redis://redis.example.com:6379/1",
    worker_concurrency=2,
    timezone="America/New_York"
)
```

**Apply to Existing Celery App:**
```python
celery_config = CeleryConfig(
    broker_url="redis://localhost:6379/0",
    worker_concurrency=4
)

celery_config.apply_to_app(app)
```

## Environment Variables

All configuration options can be set via environment variables with the `CELERY_THROTTLE_` prefix.

### Main Settings

```bash
export CELERY_THROTTLE_APP_NAME=my-app
export CELERY_THROTTLE_TARGET_QUEUE=rate-limited-tasks
export CELERY_THROTTLE_QUEUE_PREFIX=myapp
```

### Redis Settings

Use the `CELERY_THROTTLE_REDIS_` prefix:

```bash
export CELERY_THROTTLE_REDIS_HOST=redis.example.com
export CELERY_THROTTLE_REDIS_PORT=6379
export CELERY_THROTTLE_REDIS_DB=1
export CELERY_THROTTLE_REDIS_PASSWORD=secret
export CELERY_THROTTLE_REDIS_DECODE_RESPONSES=false
```

### Celery Settings

Use the `CELERY_THROTTLE_` prefix:

```bash
export CELERY_THROTTLE_BROKER_URL=redis://localhost:6379/0
export CELERY_THROTTLE_RESULT_BACKEND=redis://localhost:6379/1
export CELERY_THROTTLE_WORKER_CONCURRENCY=2
export CELERY_THROTTLE_WORKER_PREFETCH_MULTIPLIER=1
export CELERY_THROTTLE_TASK_ACKS_LATE=true
```

### .env File Support

Use python-dotenv to load from `.env` files:

```bash
# .env file
CELERY_THROTTLE_REDIS_HOST=localhost
CELERY_THROTTLE_REDIS_PORT=6379
CELERY_THROTTLE_TARGET_QUEUE=my-queue
CELERY_THROTTLE_QUEUE_PREFIX=myapp
```

```python
from dotenv import load_dotenv
from celery_throttle import CeleryThrottle

load_dotenv()
throttle = CeleryThrottle(celery_app=app)
```

## Configuration Examples

### Development Configuration

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig

dev_config = CeleryThrottleConfig(
    app_name="myapp-dev",
    target_queue="dev-rate-limited",
    queue_prefix="dev",
    redis=RedisConfig(
        host="localhost",
        port=6379,
        db=0
    )
)

throttle = CeleryThrottle(celery_app=app, config=dev_config)
```

### Production Configuration

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig

prod_config = CeleryThrottleConfig(
    app_name="myapp-prod",
    target_queue="rate-limited-tasks",
    queue_prefix="prod",
    redis=RedisConfig(
        host="redis-master.example.com",
        port=6379,
        db=1,
        password=os.getenv("REDIS_PASSWORD")
    )
)

throttle = CeleryThrottle(celery_app=app, config=prod_config)
```

### Multi-Tenant Configuration

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig

def create_throttle_for_tenant(tenant_id: str):
    config = CeleryThrottleConfig(
        app_name=f"myapp-{tenant_id}",
        target_queue=f"tenant-{tenant_id}-rate-limited",
        queue_prefix=f"tenant_{tenant_id}"
    )
    return CeleryThrottle(celery_app=app, config=config)

# Each tenant gets isolated queues and Redis keys
tenant_a_throttle = create_throttle_for_tenant("tenant_a")
tenant_b_throttle = create_throttle_for_tenant("tenant_b")
```

### Testing Configuration

```python
import fakeredis
from celery import Celery
from celery_throttle import CeleryThrottle

def create_test_throttle():
    # Use fake Redis for testing
    fake_redis = fakeredis.FakeRedis(decode_responses=False)
    test_app = Celery('test-app')

    return CeleryThrottle(
        celery_app=test_app,
        redis_client=fake_redis,
        queue_prefix="test"
    )

throttle = create_test_throttle()
```

### Multiple Services on Same Redis

```python
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig

# Service A - Email notifications
email_config = CeleryThrottleConfig(
    app_name="email-service",
    target_queue="email-rate-limited",
    queue_prefix="email"  # Redis keys: email:*
)
email_throttle = CeleryThrottle(celery_app=app, config=email_config)

# Service B - API calls
api_config = CeleryThrottleConfig(
    app_name="api-service",
    target_queue="api-rate-limited",
    queue_prefix="api"  # Redis keys: api:*
)
api_throttle = CeleryThrottle(celery_app=app, config=api_config)

# Services are completely isolated
```

## Best Practices

### 1. Use Queue Prefixes for Isolation

Always use unique `queue_prefix` values when running multiple services on the same Redis:

```python
# ✅ Good - isolated
service_a = CeleryThrottle(celery_app=app, queue_prefix="service_a")
service_b = CeleryThrottle(celery_app=app, queue_prefix="service_b")

# ❌ Bad - will conflict
service_a = CeleryThrottle(celery_app=app, queue_prefix="throttle")
service_b = CeleryThrottle(celery_app=app, queue_prefix="throttle")
```

### 2. Use Different Redis DBs for Environments

Separate development, staging, and production:

```python
# Development
dev_config = CeleryThrottleConfig(redis=RedisConfig(db=0))

# Staging
staging_config = CeleryThrottleConfig(redis=RedisConfig(db=1))

# Production
prod_config = CeleryThrottleConfig(redis=RedisConfig(db=2))
```

### 3. Environment-Specific Configuration

Use environment variables for environment-specific settings:

```python
import os
from celery_throttle import CeleryThrottle

# Load from environment (dev/staging/prod)
throttle = CeleryThrottle(
    celery_app=app,
    queue_prefix=os.getenv("ENVIRONMENT", "dev")
)
```

### 4. Secure Credentials

Never hardcode passwords - use environment variables or secret managers:

```python
# ✅ Good
config = CeleryThrottleConfig(
    redis=RedisConfig(
        password=os.getenv("REDIS_PASSWORD")
    )
)

# ❌ Bad
config = CeleryThrottleConfig(
    redis=RedisConfig(
        password="hardcoded-secret"  # Don't do this!
    )
)
```

### 5. Validate Configuration

Check your configuration after initialization:

```python
throttle = CeleryThrottle(celery_app=app)

# Verify configuration
assert throttle.config.queue_prefix == "expected_prefix"
assert throttle.redis.ping()  # Test Redis connection
```

## Troubleshooting

### Connection Issues

**Problem:** Cannot connect to Redis

```python
# Solution: Check Redis host and port
from celery_throttle.config import RedisConfig

redis_config = RedisConfig(host="localhost", port=6379)
redis_client = redis_config.create_client()

# Test connection
redis_client.ping()  # Should return True
```

### Queue Name Conflicts

**Problem:** Different services using same queue names

```python
# Solution: Use unique queue prefixes
service_a = CeleryThrottle(celery_app=app, queue_prefix="service_a")
service_b = CeleryThrottle(celery_app=app, queue_prefix="service_b")
```

### Configuration Not Loading

**Problem:** Environment variables not being read

```python
# Solution: Check prefix and load order
import os

# Verify environment variable exists
print(os.getenv("CELERY_THROTTLE_REDIS_HOST"))

# Load before initializing
throttle = CeleryThrottle(celery_app=app)
print(throttle.config.redis.host)  # Should match env var
```

## See Also

- [Main README](README.md) - Getting started guide
- [Examples](EXAMPLES.md) - Comprehensive usage examples
- [Pydantic Settings Documentation](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)