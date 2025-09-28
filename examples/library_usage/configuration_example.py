#!/usr/bin/env python3
"""
Example showing different ways to configure celery-throttle.

This example demonstrates configuration via:
1. Configuration objects
2. Dictionary configuration
3. Environment variables
"""

import os
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig, CeleryConfig


def example_with_config_objects():
    """Configure using Pydantic configuration objects."""
    print("Example 1: Configuration with objects")
    print("-" * 40)

    # Create custom Redis configuration
    redis_config = RedisConfig(
        host="localhost",
        port=6379,
        db=1,  # Use different database
        decode_responses=False
    )

    # Create custom Celery configuration
    celery_config = CeleryConfig(
        broker_url="redis://localhost:6379/1",
        result_backend="redis://localhost:6379/1",
        worker_concurrency=2,  # Allow 2 concurrent workers
        app_name="my-custom-throttle-app"
    )

    # Create main configuration
    config = CeleryThrottleConfig(
        redis=redis_config,
        celery=celery_config,
        app_name="my-custom-app"
    )

    # Initialize with configuration
    throttle = CeleryThrottle(config=config)

    queue_name = throttle.create_queue("3/30s")
    print(f"Created queue: {queue_name}")

    # Show configuration
    print(f"Redis host: {throttle.config.redis.host}")
    print(f"Redis DB: {throttle.config.redis.db}")
    print(f"App name: {throttle.config.app_name}")

    throttle.remove_queue(queue_name)
    print()


def example_with_dict_config():
    """Configure using dictionary."""
    print("Example 2: Configuration with dictionary")
    print("-" * 40)

    config_dict = {
        "app_name": "dict-configured-app",
        "redis": {
            "host": "localhost",
            "port": 6379,
            "db": 2
        },
        "celery": {
            "broker_url": "redis://localhost:6379/2",
            "result_backend": "redis://localhost:6379/2",
            "worker_concurrency": 3
        }
    }

    throttle = CeleryThrottle.from_config_dict(config_dict)

    queue_name = throttle.create_queue("1/5s")
    print(f"Created queue: {queue_name}")
    print(f"App name: {throttle.config.app_name}")
    print(f"Worker concurrency: {throttle.config.celery.worker_concurrency}")

    throttle.remove_queue(queue_name)
    print()


def example_with_env_vars():
    """Configure using environment variables."""
    print("Example 3: Configuration with environment variables")
    print("-" * 40)

    # Set environment variables
    os.environ['CELERY_THROTTLE_REDIS_HOST'] = 'localhost'
    os.environ['CELERY_THROTTLE_REDIS_PORT'] = '6379'
    os.environ['CELERY_THROTTLE_REDIS_DB'] = '3'
    os.environ['CELERY_THROTTLE_BROKER_URL'] = 'redis://localhost:6379/3'
    os.environ['CELERY_THROTTLE_RESULT_BACKEND'] = 'redis://localhost:6379/3'
    os.environ['CELERY_THROTTLE_APP_NAME'] = 'env-configured-app'
    os.environ['CELERY_THROTTLE_WORKER_CONCURRENCY'] = '4'

    # Create from environment
    throttle = CeleryThrottle.from_env()

    queue_name = throttle.create_queue("2/20s")
    print(f"Created queue: {queue_name}")
    print(f"App name: {throttle.config.app_name}")
    print(f"Redis DB: {throttle.config.redis.db}")

    throttle.remove_queue(queue_name)

    # Clean up environment variables
    for key in list(os.environ.keys()):
        if key.startswith('CELERY_THROTTLE_'):
            del os.environ[key]
    print()


def example_with_mixed_config():
    """Configure with base config and override specific values."""
    print("Example 4: Mixed configuration (override specific values)")
    print("-" * 40)

    # Start with environment configuration
    os.environ['CELERY_THROTTLE_APP_NAME'] = 'base-app'
    os.environ['CELERY_THROTTLE_REDIS_DB'] = '4'

    # Create base configuration from environment
    base_config = CeleryThrottleConfig.from_env()

    # Override specific values
    throttle = CeleryThrottle(
        config=base_config,
        app_name="overridden-app",  # This will override the env var
    )

    queue_name = throttle.create_queue("5/45s")
    print(f"Created queue: {queue_name}")
    print(f"App name (overridden): {throttle.config.app_name}")
    print(f"Redis DB (from env): {throttle.config.redis.db}")

    throttle.remove_queue(queue_name)

    # Clean up
    for key in list(os.environ.keys()):
        if key.startswith('CELERY_THROTTLE_'):
            del os.environ[key]
    print()


def main():
    print("CeleryThrottle Configuration Examples")
    print("=" * 50)
    print()

    example_with_config_objects()
    example_with_dict_config()
    example_with_env_vars()
    example_with_mixed_config()

    print("All configuration examples completed!")


if __name__ == "__main__":
    main()