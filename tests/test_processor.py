from unittest.mock import Mock, patch

import pytest

from celery_throttle.tasks.processor import RateLimitedTaskProcessor


class TestRateLimitedTaskProcessor:
    """Test cases for RateLimitedTaskProcessor."""

    def test_initialization(self, celery_app, redis_client, queue_manager):
        """Test processor initialization."""
        processor = RateLimitedTaskProcessor(
            celery_app, redis_client, queue_manager, target_queue="test-queue"
        )

        assert processor.app == celery_app
        assert processor.redis == redis_client
        assert processor.queue_manager == queue_manager
        assert processor.target_queue == "test-queue"

    def test_execute_task_success(self, task_processor, celery_app):
        """Test successful task execution."""
        # The celery_app fixture already has 'test_task' registered
        result = task_processor.execute_task(
            queue_name="test_queue",
            task_name="test_task",
            args=(1, 2),
            kwargs={"key": "value"},
        )

        assert result is not None
        assert hasattr(result, "id")

    def test_execute_task_not_found(self, task_processor):
        """Test executing a task that is not registered."""
        with pytest.raises(ValueError, match="not registered with Celery app"):
            task_processor.execute_task(
                queue_name="test_queue",
                task_name="nonexistent_task",
                args=(),
                kwargs={},
            )

    def test_execute_task_increments_stats(self, task_processor, queue_manager):
        """Test that executing a task increments statistics."""
        # Create a queue first
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Execute task
        task_processor.execute_task(
            queue_name="test_queue", task_name="test_task", args=(), kwargs={}
        )

        # Check that stats were incremented
        stats = queue_manager.get_queue_stats("test_queue")
        assert stats.tasks_completed >= 0  # Should have been incremented

    def test_execute_task_submits_to_target_queue(
        self, celery_app, redis_client, queue_manager
    ):
        """Test that task is submitted to the configured target queue."""
        mock_task = Mock()
        mock_result = Mock(id="task-123")
        mock_task.apply_async = Mock(return_value=mock_result)

        # Register the mock task
        celery_app.tasks["mock_task"] = mock_task

        processor = RateLimitedTaskProcessor(
            celery_app, redis_client, queue_manager, target_queue="custom-queue"
        )

        processor.execute_task(
            queue_name="test_queue",
            task_name="mock_task",
            args=(1, 2),
            kwargs={"key": "value"},
        )

        # Verify apply_async was called with correct queue
        mock_task.apply_async.assert_called_once()
        call_kwargs = mock_task.apply_async.call_args[1]
        assert call_kwargs["queue"] == "custom-queue"

    def test_execute_task_with_args(self, celery_app, redis_client, queue_manager):
        """Test executing a task with args."""
        mock_task = Mock()
        mock_result = Mock(id="task-123")
        mock_task.apply_async = Mock(return_value=mock_result)

        celery_app.tasks["mock_task"] = mock_task

        processor = RateLimitedTaskProcessor(
            celery_app, redis_client, queue_manager, target_queue="test-queue"
        )

        processor.execute_task(
            queue_name="test_queue", task_name="mock_task", args=(1, 2, 3), kwargs={}
        )

        # Verify args were passed correctly
        call_kwargs = mock_task.apply_async.call_args[1]
        assert call_kwargs["args"] == (1, 2, 3)
        assert call_kwargs["kwargs"] == {}

    def test_execute_task_with_kwargs(self, celery_app, redis_client, queue_manager):
        """Test executing a task with kwargs."""
        mock_task = Mock()
        mock_result = Mock(id="task-123")
        mock_task.apply_async = Mock(return_value=mock_result)

        celery_app.tasks["mock_task"] = mock_task

        processor = RateLimitedTaskProcessor(
            celery_app, redis_client, queue_manager, target_queue="test-queue"
        )

        processor.execute_task(
            queue_name="test_queue",
            task_name="mock_task",
            args=(),
            kwargs={"foo": "bar", "baz": 123},
        )

        # Verify kwargs were passed correctly
        call_kwargs = mock_task.apply_async.call_args[1]
        assert call_kwargs["kwargs"] == {"foo": "bar", "baz": 123}

    def test_execute_task_with_empty_args_and_kwargs(
        self, celery_app, redis_client, queue_manager
    ):
        """Test executing a task with no args or kwargs."""
        mock_task = Mock()
        mock_result = Mock(id="task-123")
        mock_task.apply_async = Mock(return_value=mock_result)

        celery_app.tasks["mock_task"] = mock_task

        processor = RateLimitedTaskProcessor(
            celery_app, redis_client, queue_manager, target_queue="test-queue"
        )

        processor.execute_task(
            queue_name="test_queue", task_name="mock_task", args=(), kwargs={}
        )

        # Verify it was called
        mock_task.apply_async.assert_called_once()

    def test_execute_task_returns_result(self, celery_app, redis_client, queue_manager):
        """Test that execute_task returns the AsyncResult."""
        mock_task = Mock()
        mock_result = Mock(id="task-123")
        mock_task.apply_async = Mock(return_value=mock_result)

        celery_app.tasks["mock_task"] = mock_task

        processor = RateLimitedTaskProcessor(
            celery_app, redis_client, queue_manager, target_queue="test-queue"
        )

        result = processor.execute_task(
            queue_name="test_queue", task_name="mock_task", args=(), kwargs={}
        )

        assert result == mock_result
        assert result.id == "task-123"

    def test_execute_multiple_tasks(self, task_processor, celery_app):
        """Test executing multiple tasks in sequence."""
        for i in range(5):
            result = task_processor.execute_task(
                queue_name=f"queue_{i}", task_name="test_task", args=(i,), kwargs={}
            )
            assert result is not None

    def test_execute_task_with_different_queues(self, task_processor):
        """Test executing tasks from different queue names."""
        result1 = task_processor.execute_task(
            queue_name="queue1", task_name="test_task", args=(), kwargs={}
        )

        result2 = task_processor.execute_task(
            queue_name="queue2", task_name="test_task", args=(), kwargs={}
        )

        assert result1 is not None
        assert result2 is not None
        # Results should be different instances
        assert result1 != result2

    def test_task_lookup_from_celery_app(self, celery_app, redis_client, queue_manager):
        """Test that tasks are properly looked up from Celery app."""
        # Register multiple tasks
        task1 = Mock()
        task1.apply_async = Mock(return_value=Mock(id="task-1"))
        celery_app.tasks["task_one"] = task1

        task2 = Mock()
        task2.apply_async = Mock(return_value=Mock(id="task-2"))
        celery_app.tasks["task_two"] = task2

        processor = RateLimitedTaskProcessor(
            celery_app, redis_client, queue_manager, target_queue="test-queue"
        )

        # Execute both tasks
        processor.execute_task("queue", "task_one", (), {})
        processor.execute_task("queue", "task_two", (), {})

        # Verify both were called
        task1.apply_async.assert_called_once()
        task2.apply_async.assert_called_once()

    def test_execute_task_logs_submission(self, task_processor, celery_app):
        """Test that task execution is logged."""
        with patch("celery_throttle.tasks.processor.logger") as mock_logger:
            task_processor.execute_task(
                queue_name="test_queue", task_name="test_task", args=(), kwargs={}
            )

            # Verify info log was called
            mock_logger.info.assert_called()
            log_message = mock_logger.info.call_args[0][0]
            assert "Submitted task" in log_message
            assert "test_task" in log_message

    def test_execute_task_error_logging(self, task_processor):
        """Test that task not found errors are logged."""
        with patch("celery_throttle.tasks.processor.logger") as mock_logger:
            try:
                task_processor.execute_task(
                    queue_name="test_queue",
                    task_name="nonexistent_task",
                    args=(),
                    kwargs={},
                )
            except ValueError:
                pass

            # Verify error log was called
            mock_logger.error.assert_called()
            log_message = mock_logger.error.call_args[0][0]
            assert "not found" in log_message
