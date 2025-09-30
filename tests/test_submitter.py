import pytest
import json
from unittest.mock import patch

from celery_throttle.tasks.processor import RateLimitedTaskSubmitter


class TestRateLimitedTaskSubmitter:
    """Test cases for RateLimitedTaskSubmitter."""

    @pytest.fixture
    def submitter(self, redis_client, queue_manager, task_processor):
        """Provide a task submitter instance."""
        return RateLimitedTaskSubmitter(redis_client, queue_manager, task_processor)

    def test_initialization(self, redis_client, queue_manager, task_processor):
        """Test submitter initialization."""
        submitter = RateLimitedTaskSubmitter(
            redis_client, queue_manager, task_processor
        )

        assert submitter.redis == redis_client
        assert submitter.queue_manager == queue_manager
        assert submitter.task_processor == task_processor

    def test_submit_task_immediate_processing(self, submitter, queue_manager):
        """Test task submission when token is available (immediate processing)."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            result = submitter.submit_task(
                "test_queue", "test_task", "arg1", key="value"
            )

        assert result is True

    def test_submit_task_queued(self, submitter, queue_manager, redis_client):
        """Test task submission when no token is available (queued)."""
        queue_manager.create_queue("1/10s", queue_name="slow_queue")

        with patch("time.time", return_value=1000.0):
            # First task should be processed immediately
            result1 = submitter.submit_task("slow_queue", "test_task")
            assert result1 is True

            # Second task should be queued
            result2 = submitter.submit_task("slow_queue", "test_task")
            assert result2 is False

            # Verify task was added to queue
            task_queue_key = f"{queue_manager.queue_prefix}:queue:slow_queue"
            queue_length = redis_client.llen(task_queue_key)
            assert queue_length == 1

    def test_submit_task_nonexistent_queue(self, submitter):
        """Test submitting task to non-existent queue."""
        result = submitter.submit_task("nonexistent_queue", "test_task")
        assert result is False

    def test_submit_task_with_args(self, submitter, queue_manager):
        """Test task submission with positional arguments."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            result = submitter.submit_task("test_queue", "test_task", 1, 2, 3)

        assert result is True

    def test_submit_task_with_kwargs(self, submitter, queue_manager):
        """Test task submission with keyword arguments."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            result = submitter.submit_task(
                "test_queue", "test_task", foo="bar", baz=123
            )

        assert result is True

    def test_submit_task_with_args_and_kwargs(self, submitter, queue_manager):
        """Test task submission with both args and kwargs."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            result = submitter.submit_task("test_queue", "test_task", 1, 2, foo="bar")

        assert result is True

    def test_submit_task_queued_payload_structure(
        self, submitter, queue_manager, redis_client
    ):
        """Test that queued task has correct payload structure."""
        queue_manager.create_queue("1/10s", queue_name="slow_queue")

        with patch("time.time", return_value=1000.0):
            # Consume the token
            submitter.submit_task("slow_queue", "test_task")

            # This should be queued
            submitter.submit_task("slow_queue", "my_task", "arg1", "arg2", key1="val1")

            # Get the queued task
            task_queue_key = f"{queue_manager.queue_prefix}:queue:slow_queue"
            task_payload_json = redis_client.lindex(task_queue_key, 0)
            task_payload = json.loads(task_payload_json.decode())

            assert task_payload["task_name"] == "my_task"
            assert task_payload["args"] == ["arg1", "arg2"]
            assert task_payload["kwargs"] == {"key1": "val1"}
            assert "submitted_at" in task_payload
            assert "wait_time" in task_payload

    def test_submit_multiple_tasks_empty_list(self, submitter, queue_manager):
        """Test submitting empty task list."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        stats = submitter.submit_multiple_tasks("test_queue", [])

        assert stats["submitted"] == 0
        assert stats["queued"] == 0

    def test_submit_multiple_tasks_all_submitted(self, submitter, queue_manager):
        """Test submitting multiple tasks when all can be processed immediately."""
        queue_manager.create_queue("100/60s:3", queue_name="fast_queue")  # Burst of 3

        tasks = [
            ("test_task", (1,), {}),
            ("test_task", (2,), {}),
            ("test_task", (3,), {}),
        ]

        with patch("time.time", return_value=1000.0):
            stats = submitter.submit_multiple_tasks("fast_queue", tasks)

        assert stats["submitted"] == 3
        assert stats["queued"] == 0

    def test_submit_multiple_tasks_mixed_results(self, submitter, queue_manager):
        """Test submitting multiple tasks with some queued."""
        queue_manager.create_queue("2/10s:2", queue_name="limited_queue")  # Burst of 2

        tasks = [
            ("test_task", (1,), {}),
            ("test_task", (2,), {}),
            ("test_task", (3,), {}),
            ("test_task", (4,), {}),
        ]

        with patch("time.time", return_value=1000.0):
            stats = submitter.submit_multiple_tasks("limited_queue", tasks)

        # First 2 should be submitted, rest queued
        assert stats["submitted"] == 2
        assert stats["queued"] == 2

    def test_submit_multiple_tasks_with_two_element_tuples(
        self, submitter, queue_manager
    ):
        """Test submitting tasks as (task_name, args) tuples."""
        queue_manager.create_queue("10/60s:2", queue_name="test_queue")  # Burst of 2

        tasks = [
            ("test_task", (1, 2)),
            ("test_task", (3, 4)),
        ]

        with patch("time.time", return_value=1000.0):
            stats = submitter.submit_multiple_tasks("test_queue", tasks)

        assert stats["submitted"] == 2
        assert stats["queued"] == 0

    def test_submit_multiple_tasks_with_one_element_tuples(
        self, submitter, queue_manager
    ):
        """Test submitting tasks as (task_name,) tuples."""
        queue_manager.create_queue("10/60s:2", queue_name="test_queue")  # Burst of 2

        tasks = [
            ("test_task",),
            ("test_task",),
        ]

        with patch("time.time", return_value=1000.0):
            stats = submitter.submit_multiple_tasks("test_queue", tasks)

        assert stats["submitted"] == 2
        assert stats["queued"] == 0

    def test_submit_task_logs_immediate_submission(self, submitter, queue_manager):
        """Test that immediate submission is logged."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            with patch("celery_throttle.tasks.processor.logger") as mock_logger:
                submitter.submit_task("test_queue", "test_task")

                # Check that info log was called
                mock_logger.info.assert_called()
                log_messages = [call[0][0] for call in mock_logger.info.call_args_list]
                assert any(
                    "submitted" in msg.lower() and "immediate" in msg.lower()
                    for msg in log_messages
                )

    def test_submit_task_logs_queued_submission(self, submitter, queue_manager):
        """Test that queued submission is logged."""
        queue_manager.create_queue("1/10s", queue_name="slow_queue")

        with patch("time.time", return_value=1000.0):
            with patch("celery_throttle.tasks.processor.logger") as mock_logger:
                # First task consumes token
                submitter.submit_task("slow_queue", "test_task")

                # Second task should be queued
                submitter.submit_task("slow_queue", "test_task")

                # Check that queued log was called
                log_messages = [call[0][0] for call in mock_logger.info.call_args_list]
                assert any("queued" in msg.lower() for msg in log_messages)

    def test_submit_task_logs_error_for_nonexistent_queue(self, submitter):
        """Test that error is logged for non-existent queue."""
        with patch("celery_throttle.tasks.processor.logger") as mock_logger:
            submitter.submit_task("nonexistent_queue", "test_task")

            # Verify error log was called
            mock_logger.error.assert_called()
            log_message = mock_logger.error.call_args[0][0]
            assert "does not exist" in log_message

    def test_submit_task_inactive_queue(self, submitter, queue_manager):
        """Test that inactive queues don't process tasks."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")
        queue_manager.deactivate_queue("test_queue")

        result = submitter.submit_task("test_queue", "test_task")

        # Should not process (inactive queue acts like no tokens available)
        assert result is False

    def test_multiple_queues_independent(self, submitter, queue_manager):
        """Test that submitting to different queues is independent."""
        queue_manager.create_queue("1/10s", queue_name="queue1")
        queue_manager.create_queue("1/10s", queue_name="queue2")

        with patch("time.time", return_value=1000.0):
            # Consume token on queue1
            result1 = submitter.submit_task("queue1", "test_task")
            assert result1 is True

            # queue2 should still have token
            result2 = submitter.submit_task("queue2", "test_task")
            assert result2 is True

    def test_task_processor_called_on_immediate_submission(
        self, submitter, queue_manager, task_processor
    ):
        """Test that task processor is called when task is submitted immediately."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            with patch.object(task_processor, "execute_task") as mock_execute:
                submitter.submit_task("test_queue", "my_task", 1, 2, key="value")

                mock_execute.assert_called_once_with(
                    "test_queue", "my_task", (1, 2), {"key": "value"}
                )

    def test_task_processor_not_called_when_queued(
        self, submitter, queue_manager, task_processor
    ):
        """Test that task processor is not called when task is queued."""
        queue_manager.create_queue("1/10s", queue_name="slow_queue")

        with patch("time.time", return_value=1000.0):
            with patch.object(task_processor, "execute_task") as mock_execute:
                # First task consumes token
                submitter.submit_task("slow_queue", "test_task")
                call_count_after_first = mock_execute.call_count

                # Second task should be queued, not executed
                submitter.submit_task("slow_queue", "test_task")
                call_count_after_second = mock_execute.call_count

                # Should only have been called once (for first task)
                assert call_count_after_second == call_count_after_first

    def test_submit_task_with_burst_allowance(self, submitter, queue_manager):
        """Test submitting tasks with burst allowance."""
        queue_manager.create_queue("10/60s:5", queue_name="burst_queue")

        with patch("time.time", return_value=1000.0):
            # Should be able to submit 5 tasks immediately
            for i in range(5):
                result = submitter.submit_task("burst_queue", "test_task")
                assert result is True, f"Failed on task {i + 1}"

            # 6th should be queued
            result = submitter.submit_task("burst_queue", "test_task")
            assert result is False
