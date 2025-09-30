import pytest
from datetime import datetime
from unittest.mock import patch

from celery_throttle.queue.manager import UniversalQueueManager


class TestUniversalQueueManager:
    """Test cases for UniversalQueueManager."""

    def test_initialization(self, redis_client):
        """Test queue manager initialization."""
        manager = UniversalQueueManager(redis_client, queue_prefix="test")
        assert manager.redis == redis_client
        assert manager.queue_prefix == "test"
        assert manager._queues_key == "test:queues:config"
        assert manager._stats_key_prefix == "test:queues:stats"

    def test_create_queue_with_auto_name(self, queue_manager):
        """Test creating a queue with auto-generated name."""
        queue_name = queue_manager.create_queue("10/60s")

        assert queue_name.startswith("batch_")
        assert queue_manager.queue_exists(queue_name)

    def test_create_queue_with_custom_name(self, queue_manager):
        """Test creating a queue with custom name."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="my_queue")

        assert queue_name == "my_queue"
        assert queue_manager.queue_exists(queue_name)

    def test_create_queue_stores_config(self, queue_manager):
        """Test that queue creation stores proper configuration."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")
        config = queue_manager.get_queue_config(queue_name)

        assert config is not None
        assert config.name == "test_queue"
        assert config.rate_limit.requests == 10
        assert config.rate_limit.period_seconds == 60
        assert config.active is True

    def test_create_queue_initializes_stats(self, queue_manager):
        """Test that queue creation initializes statistics."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")
        stats = queue_manager.get_queue_stats(queue_name)

        assert stats is not None
        assert stats.tasks_waiting == 0
        assert stats.tasks_processing == 0
        assert stats.tasks_completed == 0
        assert stats.tasks_failed == 0

    def test_queue_exists(self, queue_manager):
        """Test checking queue existence."""
        assert queue_manager.queue_exists("nonexistent") is False

        queue_manager.create_queue("10/60s", queue_name="test_queue")
        assert queue_manager.queue_exists("test_queue") is True

    def test_get_queue_config_nonexistent(self, queue_manager):
        """Test getting config for non-existent queue."""
        config = queue_manager.get_queue_config("nonexistent")
        assert config is None

    def test_list_queues_empty(self, queue_manager):
        """Test listing queues when none exist."""
        queues = queue_manager.list_queues()
        assert queues == []

    def test_list_queues_multiple(self, queue_manager):
        """Test listing multiple queues."""
        queue_manager.create_queue("10/60s", queue_name="queue1")
        queue_manager.create_queue("20/120s", queue_name="queue2")
        queue_manager.create_queue("30/180s", queue_name="queue3")

        queues = queue_manager.list_queues()
        assert len(queues) == 3

        queue_names = [q.name for q in queues]
        assert "queue1" in queue_names
        assert "queue2" in queue_names
        assert "queue3" in queue_names

    def test_list_queues_sorted_by_created_at(self, queue_manager):
        """Test that list_queues returns queues sorted by creation time."""
        with patch("celery_throttle.queue.manager.datetime") as mock_datetime:
            # Create queues at different times
            mock_datetime.now.return_value = datetime(2024, 1, 1, 10, 0, 0)
            queue_manager.create_queue("10/60s", queue_name="queue1")

            mock_datetime.now.return_value = datetime(2024, 1, 1, 11, 0, 0)
            queue_manager.create_queue("10/60s", queue_name="queue2")

            mock_datetime.now.return_value = datetime(2024, 1, 1, 9, 0, 0)
            queue_manager.create_queue("10/60s", queue_name="queue3")

        queues = queue_manager.list_queues()
        # Should be sorted by created_at
        assert queues[0].name == "queue3"
        assert queues[1].name == "queue1"
        assert queues[2].name == "queue2"

    def test_get_active_queues(self, queue_manager):
        """Test getting only active queues."""
        queue_manager.create_queue("10/60s", queue_name="active1")
        queue_manager.create_queue("10/60s", queue_name="active2")
        queue_manager.create_queue("10/60s", queue_name="inactive1")

        queue_manager.deactivate_queue("inactive1")

        active_queues = queue_manager.get_active_queues()
        active_names = [q.name for q in active_queues]

        assert len(active_queues) == 2
        assert "active1" in active_names
        assert "active2" in active_names
        assert "inactive1" not in active_names

    def test_activate_queue(self, queue_manager):
        """Test activating a queue."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")
        queue_manager.deactivate_queue(queue_name)

        result = queue_manager.activate_queue(queue_name)
        assert result is True

        config = queue_manager.get_queue_config(queue_name)
        assert config.active is True

    def test_deactivate_queue(self, queue_manager):
        """Test deactivating a queue."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        result = queue_manager.deactivate_queue(queue_name)
        assert result is True

        config = queue_manager.get_queue_config(queue_name)
        assert config.active is False

    def test_activate_nonexistent_queue(self, queue_manager):
        """Test activating a non-existent queue."""
        result = queue_manager.activate_queue("nonexistent")
        assert result is False

    def test_deactivate_nonexistent_queue(self, queue_manager):
        """Test deactivating a non-existent queue."""
        result = queue_manager.deactivate_queue("nonexistent")
        assert result is False

    def test_update_rate_limit(self, queue_manager):
        """Test updating rate limit for a queue."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        result = queue_manager.update_rate_limit(queue_name, "20/120s")
        assert result is True

        config = queue_manager.get_queue_config(queue_name)
        assert config.rate_limit.requests == 20
        assert config.rate_limit.period_seconds == 120

    def test_update_rate_limit_nonexistent(self, queue_manager):
        """Test updating rate limit for non-existent queue."""
        result = queue_manager.update_rate_limit("nonexistent", "20/120s")
        assert result is False

    def test_remove_queue(self, queue_manager):
        """Test removing a queue."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")
        assert queue_manager.queue_exists(queue_name)

        result = queue_manager.remove_queue(queue_name)
        assert result is True
        assert queue_manager.queue_exists(queue_name) is False

    def test_remove_queue_cleans_all_data(self, queue_manager, redis_client):
        """Test that removing a queue cleans up all associated data."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add some data to the queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:{queue_name}"
        processing_key = f"{queue_manager.queue_prefix}:processing:{queue_name}"
        bucket_key = f"{queue_manager.queue_prefix}:rate_limit:{queue_name}"

        redis_client.lpush(task_queue_key, "task1")
        redis_client.sadd(processing_key, "task2")
        redis_client.set(bucket_key, "data")

        # Remove the queue
        queue_manager.remove_queue(queue_name)

        # Verify all data is cleaned up
        assert redis_client.exists(task_queue_key) == 0
        assert redis_client.exists(processing_key) == 0
        assert redis_client.exists(bucket_key) == 0

    def test_remove_nonexistent_queue(self, queue_manager):
        """Test removing a non-existent queue."""
        result = queue_manager.remove_queue("nonexistent")
        assert result is False

    def test_increment_stat(self, queue_manager):
        """Test incrementing queue statistics."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        queue_manager.increment_stat(queue_name, "tasks_completed", 5)
        queue_manager.increment_stat(queue_name, "tasks_failed", 2)

        stats = queue_manager.get_queue_stats(queue_name)
        assert stats.tasks_completed == 5
        assert stats.tasks_failed == 2

    def test_get_queue_stats_with_waiting_tasks(self, queue_manager, redis_client):
        """Test queue stats reflect waiting tasks in Redis."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add tasks to the pending queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:{queue_name}"
        redis_client.lpush(task_queue_key, "task1", "task2", "task3")

        stats = queue_manager.get_queue_stats(queue_name)
        assert stats.tasks_waiting == 3

    def test_get_queue_stats_with_processing_tasks(self, queue_manager, redis_client):
        """Test queue stats reflect processing tasks in Redis."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add tasks to the processing set
        processing_key = f"{queue_manager.queue_prefix}:processing:{queue_name}"
        redis_client.sadd(processing_key, "task1", "task2")

        stats = queue_manager.get_queue_stats(queue_name)
        assert stats.tasks_processing == 2

    def test_get_queue_stats_nonexistent(self, queue_manager):
        """Test getting stats for non-existent queue."""
        stats = queue_manager.get_queue_stats("nonexistent")
        assert stats is None

    def test_can_process_task_active_queue(self, queue_manager):
        """Test checking if task can be processed on active queue."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            can_process, wait_time = queue_manager.can_process_task(queue_name)

        # First request should succeed
        assert can_process is True
        assert wait_time == 0.0

    def test_can_process_task_inactive_queue(self, queue_manager):
        """Test that inactive queues cannot process tasks."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")
        queue_manager.deactivate_queue(queue_name)

        can_process, wait_time = queue_manager.can_process_task(queue_name)

        assert can_process is False
        assert wait_time == 0.0

    def test_can_process_task_nonexistent_queue(self, queue_manager):
        """Test checking if task can be processed on non-existent queue."""
        can_process, wait_time = queue_manager.can_process_task("nonexistent")

        assert can_process is False
        assert wait_time == 0.0

    def test_get_rate_limit_status(self, queue_manager):
        """Test getting rate limit status for a queue."""
        queue_name = queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            status = queue_manager.get_rate_limit_status(queue_name)

        assert status is not None
        assert "available_tokens" in status
        assert "capacity" in status
        assert "refill_rate" in status

    def test_get_rate_limit_status_nonexistent(self, queue_manager):
        """Test getting rate limit status for non-existent queue."""
        status = queue_manager.get_rate_limit_status("nonexistent")
        assert status is None

    def test_queue_prefix_isolation(self, redis_client):
        """Test that different queue prefixes are isolated."""
        manager1 = UniversalQueueManager(redis_client, queue_prefix="app1")
        manager2 = UniversalQueueManager(redis_client, queue_prefix="app2")

        manager1.create_queue("10/60s", queue_name="test_queue")
        manager2.create_queue("20/120s", queue_name="test_queue")

        # Both managers should see their own queue
        config1 = manager1.get_queue_config("test_queue")
        config2 = manager2.get_queue_config("test_queue")

        assert config1.rate_limit.requests == 10
        assert config2.rate_limit.requests == 20

    def test_multiple_queues_independent_rate_limits(self, queue_manager):
        """Test that multiple queues have independent rate limits."""
        queue_manager.create_queue("1/10s", queue_name="slow_queue")
        queue_manager.create_queue("100/10s", queue_name="fast_queue")

        with patch("time.time", return_value=1000.0):
            # Slow queue should have token
            can_process_slow, _ = queue_manager.can_process_task("slow_queue")
            assert can_process_slow is True

            # Fast queue should also have token (independent)
            can_process_fast, _ = queue_manager.can_process_task("fast_queue")
            assert can_process_fast is True

    def test_rate_limit_with_burst_allowance(self, queue_manager):
        """Test queue with burst allowance."""
        queue_name = queue_manager.create_queue("10/60s:5", queue_name="burst_queue")

        config = queue_manager.get_queue_config(queue_name)
        assert config.rate_limit.burst_allowance == 5

    def test_parse_invalid_rate_limit_string(self, queue_manager):
        """Test that invalid rate limit string raises error."""
        with pytest.raises(ValueError):
            queue_manager.create_queue("invalid")

    def test_queue_config_model_serialization(self, queue_manager):
        """Test that queue config can be serialized and deserialized."""
        queue_name = queue_manager.create_queue("10/60s:3", queue_name="test_queue")

        # Get config and verify it's a proper Pydantic model
        config = queue_manager.get_queue_config(queue_name)
        config_dict = config.model_dump()

        assert "name" in config_dict
        assert "rate_limit" in config_dict
        assert "active" in config_dict
        assert "created_at" in config_dict
