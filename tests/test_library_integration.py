import pytest
import redis
from celery import Celery
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig, CeleryConfig


class TestLibraryIntegration:
    """Test the main library interface and integration."""

    def test_default_initialization(self):
        """Test CeleryThrottle initializes with defaults."""
        throttle = CeleryThrottle()
        assert throttle.redis is not None
        assert throttle.app is not None
        assert throttle.queue_manager is not None
        assert throttle.task_processor is not None
        assert throttle.task_submitter is not None
        assert throttle.task_dispatcher is not None

    def test_with_existing_celery_app(self):
        """Test CeleryThrottle works with existing Celery app."""
        app = Celery('test-app')
        throttle = CeleryThrottle(celery_app=app)
        assert throttle.app is app

    def test_with_existing_redis_client(self):
        """Test CeleryThrottle works with existing Redis client."""
        redis_client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=False)
        throttle = CeleryThrottle(redis_client=redis_client)
        assert throttle.redis is redis_client

    def test_with_config_object(self):
        """Test CeleryThrottle works with configuration object."""
        config = CeleryThrottleConfig(
            app_name="test-config-app",
            redis=RedisConfig(db=2),
            celery=CeleryConfig(worker_concurrency=2)
        )
        throttle = CeleryThrottle(config=config)
        assert throttle.config.app_name == "test-config-app"
        assert throttle.config.redis.db == 2
        assert throttle.config.celery.worker_concurrency == 2

    def test_from_config_dict(self):
        """Test creating CeleryThrottle from config dictionary."""
        config_dict = {
            "app_name": "dict-config-app",
            "redis": {"db": 3},
            "celery": {"worker_concurrency": 3}
        }
        throttle = CeleryThrottle.from_config_dict(config_dict)
        assert throttle.config.app_name == "dict-config-app"
        assert throttle.config.redis.db == 3
        assert throttle.config.celery.worker_concurrency == 3

    def test_queue_operations(self):
        """Test basic queue operations."""
        throttle = CeleryThrottle()

        # Create queue
        queue_name = throttle.create_queue("5/60s")
        assert queue_name.startswith("batch_")

        # List queues
        queues = throttle.list_queues()
        assert any(q.name == queue_name for q in queues)

        # Get queue stats
        stats = throttle.get_queue_stats(queue_name)
        assert stats is not None
        assert stats.name == queue_name

        # Get rate limit status
        rate_status = throttle.get_rate_limit_status(queue_name)
        assert rate_status is not None
        assert "available_tokens" in rate_status

        # Remove queue
        assert throttle.remove_queue(queue_name) is True

        # Verify removal
        assert throttle.remove_queue(queue_name) is False

    def test_task_submission(self):
        """Test task submission functionality."""
        throttle = CeleryThrottle()

        # Create queue
        queue_name = throttle.create_queue("2/10s")

        # Submit single task
        task_data = {"message": "test task"}
        result = throttle.submit_task(queue_name, task_data)
        assert isinstance(result, bool)

        # Submit multiple tasks
        tasks_data = [{"id": i} for i in range(3)]
        results = throttle.submit_multiple_tasks(queue_name, tasks_data)
        assert "submitted" in results
        assert "queued" in results
        assert results["submitted"] + results["queued"] == 3

        # Clean up
        throttle.remove_queue(queue_name)

    def test_queue_management_operations(self):
        """Test queue management operations."""
        throttle = CeleryThrottle()

        # Create queue
        queue_name = throttle.create_queue("3/30s")

        # Update rate limit
        assert throttle.update_rate_limit(queue_name, "5/60s") is True
        config = throttle.queue_manager.get_queue_config(queue_name)
        assert str(config.rate_limit) == "5/1m"

        # Deactivate queue
        assert throttle.deactivate_queue(queue_name) is True
        config = throttle.queue_manager.get_queue_config(queue_name)
        assert config.active is False

        # Activate queue
        assert throttle.activate_queue(queue_name) is True
        config = throttle.queue_manager.get_queue_config(queue_name)
        assert config.active is True

        # Clean up
        throttle.remove_queue(queue_name)


class TestConfigurationSystem:
    """Test the configuration system."""

    def test_redis_config_create_client(self):
        """Test Redis configuration creates client correctly."""
        config = RedisConfig(host="localhost", port=6379, db=5)
        client = config.create_client()
        assert isinstance(client, redis.Redis)

    def test_celery_config_apply_to_app(self):
        """Test Celery configuration applies to app correctly."""
        app = Celery('test-app')
        config = CeleryConfig(
            broker_url="redis://localhost:6379/5",
            worker_concurrency=4,
            task_acks_late=False
        )
        config.apply_to_app(app)

        assert app.conf.broker_url == "redis://localhost:6379/5"
        assert app.conf.worker_concurrency == 4
        assert app.conf.task_acks_late is False

    def test_config_from_dict(self):
        """Test creating configuration from dictionary."""
        config_dict = {
            "app_name": "test-app",
            "redis": {
                "host": "localhost",
                "port": 6379,
                "db": 4
            },
            "celery": {
                "worker_concurrency": 2,
                "task_acks_late": False
            }
        }

        config = CeleryThrottleConfig.from_dict(config_dict)
        assert config.app_name == "test-app"
        assert config.redis.host == "localhost"
        assert config.redis.db == 4
        assert config.celery.worker_concurrency == 2
        assert config.celery.task_acks_late is False

    def test_config_override_with_kwargs(self):
        """Test configuration override with kwargs."""
        base_config = CeleryThrottleConfig(app_name="base-app")
        throttle = CeleryThrottle(config=base_config, app_name="overridden-app")

        assert throttle.config.app_name == "overridden-app"


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_nonexistent_queue_operations(self):
        """Test operations on non-existent queues."""
        throttle = CeleryThrottle()
        fake_queue = "nonexistent_queue"

        # Should return False/None for non-existent queues
        assert throttle.remove_queue(fake_queue) is False
        assert throttle.update_rate_limit(fake_queue, "5/60s") is False
        assert throttle.activate_queue(fake_queue) is False
        assert throttle.deactivate_queue(fake_queue) is False
        assert throttle.get_queue_stats(fake_queue) is None
        assert throttle.get_rate_limit_status(fake_queue) is None

    def test_invalid_rate_limit_format(self):
        """Test creation with invalid rate limit format."""
        throttle = CeleryThrottle()

        with pytest.raises(ValueError):
            throttle.create_queue("invalid_format")

    def test_submit_task_to_nonexistent_queue(self):
        """Test submitting task to non-existent queue."""
        throttle = CeleryThrottle()

        result = throttle.submit_task("nonexistent_queue", {"test": "data"})
        assert result is False  # Should fail gracefully