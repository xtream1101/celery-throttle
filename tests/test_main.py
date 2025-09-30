from unittest.mock import Mock, patch

from celery_throttle.main import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig
from celery_throttle.tasks.processor import RateLimitedTaskProcessor


class TestCeleryThrottle:
    """Test cases for CeleryThrottle main class."""

    def test_initialization_with_defaults(self, celery_app):
        """Test basic initialization with default config."""
        with patch("celery_throttle.main.redis.Redis") as mock_redis_cls:
            mock_redis = Mock()
            mock_redis_cls.return_value = mock_redis

            throttle = CeleryThrottle(celery_app=celery_app)

            assert throttle.app == celery_app
            assert throttle.redis == mock_redis
            assert throttle.queue_manager is not None
            assert throttle.task_processor is not None
            assert throttle.task_submitter is not None
            assert throttle.task_dispatcher is not None

    def test_initialization_with_explicit_config(
        self, celery_app, throttle_config, redis_client
    ):
        """Test initialization with explicit config."""
        throttle = CeleryThrottle(
            celery_app=celery_app, redis_client=redis_client, config=throttle_config
        )

        assert throttle.app == celery_app
        assert throttle.redis == redis_client
        assert throttle.config == throttle_config

    def test_initialization_with_kwargs_override(self, celery_app, redis_client):
        """Test initialization with kwargs overriding config."""
        config = CeleryThrottleConfig(
            app_name="original_app", target_queue="original_queue"
        )

        throttle = CeleryThrottle(
            celery_app=celery_app,
            redis_client=redis_client,
            config=config,
            target_queue="overridden_queue",
        )

        assert throttle.config.target_queue == "overridden_queue"
        assert throttle.config.app_name == "original_app"

    def test_initialization_with_custom_processor(self, celery_app, redis_client):
        """Test initialization with custom processor instance."""
        custom_processor = Mock(spec=RateLimitedTaskProcessor)

        throttle = CeleryThrottle(
            celery_app=celery_app,
            redis_client=redis_client,
            task_processor=custom_processor,
        )

        assert throttle.task_processor == custom_processor

    def test_initialization_with_custom_processor_class(
        self, celery_app, redis_client, queue_manager
    ):
        """Test initialization with custom processor class."""

        class CustomProcessor(RateLimitedTaskProcessor):
            custom_attribute = "custom_value"

        throttle = CeleryThrottle(
            celery_app=celery_app,
            redis_client=redis_client,
            task_processor_cls=CustomProcessor,
        )

        assert isinstance(throttle.task_processor, CustomProcessor)
        assert throttle.task_processor.custom_attribute == "custom_value"

    def test_from_config_dict(self, celery_app, redis_client):
        """Test creating instance from config dictionary."""
        config_dict = {
            "app_name": "my_app",
            "target_queue": "my_queue",
            "queue_prefix": "myprefix",
        }

        throttle = CeleryThrottle.from_config_dict(
            celery_app=celery_app, config_dict=config_dict, redis_client=redis_client
        )

        assert throttle.config.app_name == "my_app"
        assert throttle.config.target_queue == "my_queue"
        assert throttle.config.queue_prefix == "myprefix"

    def test_create_queue(self, celery_app, redis_client):
        """Test creating a queue."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        queue_name = throttle.create_queue("10/60s", queue_name="test_queue")

        assert queue_name == "test_queue"
        assert throttle.queue_manager.queue_exists("test_queue")

    def test_create_queue_with_auto_name(self, celery_app, redis_client):
        """Test creating a queue with auto-generated name."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        queue_name = throttle.create_queue("10/60s")

        assert queue_name.startswith("batch_")
        assert throttle.queue_manager.queue_exists(queue_name)

    def test_remove_queue(self, celery_app, redis_client):
        """Test removing a queue."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        throttle.create_queue("10/60s", queue_name="test_queue")
        result = throttle.remove_queue("test_queue")

        assert result is True
        assert not throttle.queue_manager.queue_exists("test_queue")

    def test_submit_task(self, celery_app, redis_client):
        """Test submitting a task."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)
        throttle.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            result = throttle.submit_task("test_queue", "test_task", 1, 2, key="value")

        assert result is True

    def test_submit_multiple_tasks(self, celery_app, redis_client):
        """Test submitting multiple tasks."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)
        throttle.create_queue("10/60s", queue_name="test_queue")

        tasks = [
            ("test_task", (1,), {}),
            ("test_task", (2,), {}),
        ]

        with patch("time.time", return_value=1000.0):
            stats = throttle.submit_multiple_tasks("test_queue", tasks)

        assert stats["submitted"] >= 0
        assert stats["queued"] >= 0

    def test_get_queue_stats(self, celery_app, redis_client):
        """Test getting queue statistics."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)
        throttle.create_queue("10/60s", queue_name="test_queue")

        stats = throttle.get_queue_stats("test_queue")

        assert stats is not None
        assert stats.name == "test_queue"
        assert hasattr(stats, "tasks_waiting")
        assert hasattr(stats, "tasks_processing")

    def test_list_queues(self, celery_app, redis_client):
        """Test listing queues."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        throttle.create_queue("10/60s", queue_name="queue1")
        throttle.create_queue("20/120s", queue_name="queue2")

        queues = throttle.list_queues()

        assert len(queues) == 2
        queue_names = [q.name for q in queues]
        assert "queue1" in queue_names
        assert "queue2" in queue_names

    def test_get_rate_limit_status(self, celery_app, redis_client):
        """Test getting rate limit status."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)
        throttle.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            status = throttle.get_rate_limit_status("test_queue")

        assert status is not None
        assert "available_tokens" in status
        assert "capacity" in status

    def test_update_rate_limit(self, celery_app, redis_client):
        """Test updating queue rate limit."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)
        throttle.create_queue("10/60s", queue_name="test_queue")

        result = throttle.update_rate_limit("test_queue", "20/120s")

        assert result is True

        config = throttle.queue_manager.get_queue_config("test_queue")
        assert config.rate_limit.requests == 20
        assert config.rate_limit.period_seconds == 120

    def test_activate_queue(self, celery_app, redis_client):
        """Test activating a queue."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)
        throttle.create_queue("10/60s", queue_name="test_queue")
        throttle.deactivate_queue("test_queue")

        result = throttle.activate_queue("test_queue")

        assert result is True

        config = throttle.queue_manager.get_queue_config("test_queue")
        assert config.active is True

    def test_deactivate_queue(self, celery_app, redis_client):
        """Test deactivating a queue."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)
        throttle.create_queue("10/60s", queue_name="test_queue")

        result = throttle.deactivate_queue("test_queue")

        assert result is True

        config = throttle.queue_manager.get_queue_config("test_queue")
        assert config.active is False

    def test_set_task_processor_with_instance(self, celery_app, redis_client):
        """Test setting a custom task processor instance."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        custom_processor = Mock(spec=RateLimitedTaskProcessor)
        throttle.set_task_processor(custom_processor)

        assert throttle.task_processor == custom_processor

    def test_set_task_processor_with_class(self, celery_app, redis_client):
        """Test setting a custom task processor class."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        class CustomProcessor(RateLimitedTaskProcessor):
            custom_method_called = False

            def execute_task(self, *args, **kwargs):
                self.custom_method_called = True
                return super().execute_task(*args, **kwargs)

        throttle.set_task_processor(CustomProcessor)

        assert isinstance(throttle.task_processor, CustomProcessor)

    def test_run_dispatcher(self, celery_app, redis_client):
        """Test running the dispatcher."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        # Mock the dispatcher to stop immediately
        with patch.object(throttle.task_dispatcher, "run_dispatcher") as mock_run:
            throttle.run_dispatcher(interval=0.5)

            mock_run.assert_called_once_with(0.5)

    def test_queue_prefix_isolation(self, celery_app, redis_client):
        """Test that different instances with different prefixes are isolated."""
        throttle1 = CeleryThrottle(
            celery_app=celery_app, redis_client=redis_client, queue_prefix="app1"
        )

        throttle2 = CeleryThrottle(
            celery_app=celery_app, redis_client=redis_client, queue_prefix="app2"
        )

        throttle1.create_queue("10/60s", queue_name="shared_name")
        throttle2.create_queue("20/120s", queue_name="shared_name")

        # Each should see their own queue with different configs
        config1 = throttle1.get_queue_stats("shared_name")
        config2 = throttle2.get_queue_stats("shared_name")

        assert config1.rate_limit.requests == 10
        assert config2.rate_limit.requests == 20

    def test_submitter_references_processor(self, celery_app, redis_client):
        """Test that submitter properly references the processor."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        # Verify that submitter uses the same processor
        assert throttle.task_submitter.task_processor == throttle.task_processor

    def test_dispatcher_references_processor(self, celery_app, redis_client):
        """Test that dispatcher properly references the processor."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        # Verify that dispatcher uses the same processor
        assert throttle.task_dispatcher.task_processor == throttle.task_processor

    def test_set_task_processor_updates_submitter(self, celery_app, redis_client):
        """Test that setting a new processor updates the submitter."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        new_processor = Mock(spec=RateLimitedTaskProcessor)
        throttle.set_task_processor(new_processor)

        assert throttle.task_submitter.task_processor == new_processor

    def test_set_task_processor_updates_dispatcher(self, celery_app, redis_client):
        """Test that setting a new processor updates the dispatcher."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        new_processor = Mock(spec=RateLimitedTaskProcessor)
        throttle.set_task_processor(new_processor)

        assert throttle.task_dispatcher.task_processor == new_processor

    def test_initialization_creates_redis_client_from_config(self, celery_app):
        """Test that Redis client is created from config if not provided."""
        config = CeleryThrottleConfig(
            redis={"host": "localhost", "port": 6379, "db": 0}
        )

        with patch("celery_throttle.config.redis.Redis") as mock_redis_cls:
            mock_redis = Mock()
            mock_redis_cls.return_value = mock_redis

            throttle = CeleryThrottle(celery_app=celery_app, config=config)

            # Verify Redis client was created
            mock_redis_cls.assert_called_once()
            assert throttle.redis == mock_redis

    def test_end_to_end_workflow(self, celery_app, redis_client):
        """Test complete workflow: create queue, submit tasks, check stats."""
        throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

        # Create queue
        queue_name = throttle.create_queue("10/60s", queue_name="workflow_queue")

        # Submit tasks
        with patch("time.time", return_value=1000.0):
            throttle.submit_task(queue_name, "test_task", 1, 2)

        # Check stats
        stats = throttle.get_queue_stats(queue_name)
        assert stats is not None

        # Update rate limit
        throttle.update_rate_limit(queue_name, "20/120s")

        # Deactivate queue
        throttle.deactivate_queue(queue_name)

        # Remove queue
        result = throttle.remove_queue(queue_name)
        assert result is True
