import pytest
import json
from unittest.mock import patch
from datetime import datetime

from celery_throttle.tasks.processor import RateLimitedTaskDispatcher


class TestRateLimitedTaskDispatcher:
    """Test cases for RateLimitedTaskDispatcher."""

    @pytest.fixture
    def dispatcher(self, redis_client, queue_manager, task_processor):
        """Provide a task dispatcher instance."""
        return RateLimitedTaskDispatcher(redis_client, queue_manager, task_processor)

    def test_initialization(self, redis_client, queue_manager, task_processor):
        """Test dispatcher initialization."""
        dispatcher = RateLimitedTaskDispatcher(
            redis_client, queue_manager, task_processor
        )

        assert dispatcher.redis == redis_client
        assert dispatcher.queue_manager == queue_manager
        assert dispatcher.task_processor == task_processor

    def test_dispatch_pending_tasks_no_queues(self, dispatcher):
        """Test dispatching when no queues exist."""
        dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        assert dispatched == 0
        assert next_check_times == {}

    def test_dispatch_pending_tasks_empty_queue(self, dispatcher, queue_manager):
        """Test dispatching when queue exists but has no pending tasks."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        with patch("time.time", return_value=1000.0):
            dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        assert dispatched == 0

    def test_dispatch_pending_tasks_success(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test successfully dispatching a pending task."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add a task to the pending queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [1, 2],
            "kwargs": {"key": "value"},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        assert dispatched == 1

    def test_dispatch_pending_tasks_no_token_available(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test dispatching when no token is available."""
        queue_manager.create_queue("1/10s", queue_name="slow_queue")

        # Consume the token
        with patch("time.time", return_value=1000.0):
            queue_manager.can_process_task("slow_queue")

        # Add a task to the pending queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:slow_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [],
            "kwargs": {},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        # Should not dispatch
        assert dispatched == 0
        # Should have a next check time
        assert "slow_queue" in next_check_times

    def test_dispatch_pending_tasks_multiple_queues(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test dispatching from multiple queues."""
        queue_manager.create_queue("10/60s", queue_name="queue1")
        queue_manager.create_queue("10/60s", queue_name="queue2")

        # Add tasks to both queues
        for queue_name in ["queue1", "queue2"]:
            task_queue_key = f"{queue_manager.queue_prefix}:queue:{queue_name}"
            task_payload = {
                "task_name": "test_task",
                "args": [],
                "kwargs": {},
                "submitted_at": datetime.now().isoformat(),
                "wait_time": 0,
            }
            redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        assert dispatched == 2

    def test_dispatch_pending_tasks_inactive_queue_ignored(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test that inactive queues are ignored."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")
        queue_manager.deactivate_queue("test_queue")

        # Add a task to the pending queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [],
            "kwargs": {},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        # Should not dispatch from inactive queue
        assert dispatched == 0

    def test_dispatch_pending_tasks_calls_processor(
        self, dispatcher, queue_manager, redis_client, task_processor
    ):
        """Test that dispatcher calls task processor correctly."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add a task to the pending queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        task_payload = {
            "task_name": "my_task",
            "args": [1, 2, 3],
            "kwargs": {"foo": "bar"},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            with patch.object(task_processor, "execute_task") as mock_execute:
                dispatcher.dispatch_pending_tasks()

                mock_execute.assert_called_once_with(
                    "test_queue", "my_task", (1, 2, 3), {"foo": "bar"}
                )

    def test_dispatch_pending_tasks_json_decode_error(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test handling of JSON decode errors."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add invalid JSON to the queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        redis_client.lpush(task_queue_key, b"invalid json")

        with patch("time.time", return_value=1000.0):
            dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        # Should handle error gracefully
        assert dispatched == 0

        # Task should be removed from queue (consumed during rpop)
        queue_length = redis_client.llen(task_queue_key)
        assert queue_length == 0

    def test_dispatch_pending_tasks_execution_error(
        self, dispatcher, queue_manager, redis_client, task_processor
    ):
        """Test handling of task execution errors."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add a task to the pending queue
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [],
            "kwargs": {},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            with patch.object(
                task_processor, "execute_task", side_effect=Exception("Execution error")
            ):
                dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        # Should handle error and not crash
        assert dispatched == 0

        # Stats should reflect failure
        stats = queue_manager.get_queue_stats("test_queue")
        assert stats.tasks_failed > 0

    def test_dispatch_pending_tasks_fifo_order(
        self, dispatcher, queue_manager, redis_client, task_processor
    ):
        """Test that tasks are dispatched in FIFO order."""
        queue_manager.create_queue("10/60s:3", queue_name="test_queue")

        # Add multiple tasks
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        for i in range(3):
            task_payload = {
                "task_name": f"task_{i}",
                "args": [i],
                "kwargs": {},
                "submitted_at": datetime.now().isoformat(),
                "wait_time": 0,
            }
            redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            with patch.object(task_processor, "execute_task") as mock_execute:
                # Dispatch tasks multiple times since dispatcher only does one per cycle
                for _ in range(3):
                    dispatcher.dispatch_pending_tasks()

                # Tasks should be dispatched in reverse order (FIFO)
                calls = mock_execute.call_args_list
                assert len(calls) == 3
                assert calls[0][0][1] == "task_0"
                assert calls[1][0][1] == "task_1"
                assert calls[2][0][1] == "task_2"

    def test_run_dispatcher_keyboard_interrupt(self, dispatcher):
        """Test that dispatcher stops gracefully on KeyboardInterrupt."""
        with patch.object(
            dispatcher, "dispatch_pending_tasks", side_effect=KeyboardInterrupt
        ):
            # Should not raise exception
            dispatcher.run_dispatcher(interval=0.1)

    def test_run_dispatcher_handles_errors(self, dispatcher):
        """Test that dispatcher continues after errors."""
        call_count = [0]

        def mock_dispatch():
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Test error")
            elif call_count[0] == 2:
                raise KeyboardInterrupt()  # Stop after second call
            return 0, {}

        with patch.object(
            dispatcher, "dispatch_pending_tasks", side_effect=mock_dispatch
        ):
            dispatcher.run_dispatcher(interval=0.1)

        # Should have been called twice (once with error, once to stop)
        assert call_count[0] == 2

    def test_run_dispatcher_adaptive_timing_on_success(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test that dispatcher uses shorter interval after successful dispatch."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        call_count = [0]
        sleep_times = []

        def mock_sleep(duration):
            sleep_times.append(duration)
            call_count[0] += 1
            if call_count[0] >= 2:
                raise KeyboardInterrupt()

        # Add a task
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [],
            "kwargs": {},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            with patch("time.sleep", side_effect=mock_sleep):
                dispatcher.run_dispatcher(interval=1.0)

        # After dispatching a task, should use shorter sleep (0.1s)
        assert sleep_times[0] <= 0.1

    def test_dispatch_pending_tasks_removes_from_queue(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test that dispatched tasks are removed from queue."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add a task
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [],
            "kwargs": {},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        initial_length = redis_client.llen(task_queue_key)
        assert initial_length == 1

        with patch("time.time", return_value=1000.0):
            dispatcher.dispatch_pending_tasks()

        final_length = redis_client.llen(task_queue_key)
        assert final_length == 0

    def test_dispatch_multiple_tasks_from_same_queue(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test dispatching multiple tasks from same queue in one cycle."""
        queue_manager.create_queue("10/60s:5", queue_name="test_queue")

        # Add multiple tasks
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        for i in range(3):
            task_payload = {
                "task_name": "test_task",
                "args": [i],
                "kwargs": {},
                "submitted_at": datetime.now().isoformat(),
                "wait_time": 0,
            }
            redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            # First dispatch should only dispatch one task per queue per cycle
            dispatched, _ = dispatcher.dispatch_pending_tasks()

        # Only one task should be dispatched per cycle
        assert dispatched == 1

        remaining = redis_client.llen(task_queue_key)
        assert remaining == 2

    def test_dispatch_with_wait_time_calculation(
        self, dispatcher, queue_manager, redis_client
    ):
        """Test that next check times are calculated correctly."""
        queue_manager.create_queue("1/10s", queue_name="slow_queue")

        # Consume the token
        with patch("time.time", return_value=1000.0):
            queue_manager.can_process_task("slow_queue")

        # Add a task
        task_queue_key = f"{queue_manager.queue_prefix}:queue:slow_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [],
            "kwargs": {},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            dispatched, next_check_times = dispatcher.dispatch_pending_tasks()

        assert dispatched == 0
        assert "slow_queue" in next_check_times
        assert next_check_times["slow_queue"] > 1000.0

    def test_dispatch_logs_success(self, dispatcher, queue_manager, redis_client):
        """Test that successful dispatch is logged."""
        queue_manager.create_queue("10/60s", queue_name="test_queue")

        # Add a task
        task_queue_key = f"{queue_manager.queue_prefix}:queue:test_queue"
        task_payload = {
            "task_name": "test_task",
            "args": [],
            "kwargs": {},
            "submitted_at": datetime.now().isoformat(),
            "wait_time": 0,
        }
        redis_client.lpush(task_queue_key, json.dumps(task_payload))

        with patch("time.time", return_value=1000.0):
            with patch("celery_throttle.tasks.processor.logger") as mock_logger:
                dispatcher.dispatch_pending_tasks()

                # Verify info log was called for dispatch
                mock_logger.info.assert_called()
                log_messages = [call[0][0] for call in mock_logger.info.call_args_list]
                assert any("Dispatched" in msg for msg in log_messages)
