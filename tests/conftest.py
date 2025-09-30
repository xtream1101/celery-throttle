import pytest
import fakeredis
from celery import Celery
from unittest.mock import Mock

from celery_throttle.core.rate_limiter import RateLimit, TokenBucketRateLimiter
from celery_throttle.queue.manager import UniversalQueueManager
from celery_throttle.tasks.processor import RateLimitedTaskProcessor
from celery_throttle.config import CeleryThrottleConfig


@pytest.fixture
def redis_client():
    """Provide a fake Redis client for testing without requiring a real Redis server."""
    return fakeredis.FakeRedis(decode_responses=False)


@pytest.fixture
def celery_app():
    """Provide a Celery app instance for testing."""
    app = Celery("test-app")

    # Register a mock task for testing
    @app.task(name="test_task")
    def test_task(*args, **kwargs):
        return {"args": args, "kwargs": kwargs}

    return app


@pytest.fixture
def rate_limit():
    """Provide a standard rate limit for testing."""
    return RateLimit.from_string("10/60s")


@pytest.fixture
def rate_limiter(redis_client):
    """Provide a TokenBucketRateLimiter instance."""
    return TokenBucketRateLimiter(redis_client)


@pytest.fixture
def queue_manager(redis_client):
    """Provide a UniversalQueueManager instance."""
    return UniversalQueueManager(redis_client, queue_prefix="test_throttle")


@pytest.fixture
def task_processor(celery_app, redis_client, queue_manager):
    """Provide a RateLimitedTaskProcessor instance."""
    return RateLimitedTaskProcessor(
        celery_app, redis_client, queue_manager, target_queue="test-queue"
    )


@pytest.fixture
def throttle_config():
    """Provide a CeleryThrottleConfig instance for testing."""
    return CeleryThrottleConfig(
        redis={"host": "localhost", "port": 6379, "db": 0},
        app_name="test-app",
        target_queue="test-queue",
        queue_prefix="test_throttle",
    )


@pytest.fixture
def mock_celery_task():
    """Provide a mock Celery task."""
    task = Mock()
    task.name = "mock_task"
    task.apply_async = Mock(return_value=Mock(id="test-task-id"))
    return task
