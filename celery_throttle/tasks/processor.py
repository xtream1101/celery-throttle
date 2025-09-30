import json
import logging
import time
from datetime import datetime
from typing import Dict, List

import redis
from celery import Celery

from ..config import CeleryThrottleConfig
from ..queue.manager import UniversalQueueManager

logger = logging.getLogger(__name__)

# Get default from config
_default_target_queue = CeleryThrottleConfig().target_queue


class RateLimitedTaskProcessor:
    """Handles task processing with rate limiting integration."""

    def __init__(
        self,
        celery_app: Celery,
        redis_client: redis.Redis,
        queue_manager: UniversalQueueManager,
        target_queue: str = _default_target_queue,
    ):
        self.app = celery_app
        self.redis = redis_client
        self.queue_manager = queue_manager
        self.target_queue = target_queue

    def execute_task(self, queue_name: str, task_name: str, args: tuple, kwargs: dict):
        """Execute a Celery task by name with rate limiting tracking."""
        # Get the task from the Celery app
        if task_name not in self.app.tasks:
            logger.error(f"Task {task_name} not found in Celery app")
            raise ValueError(f"Task {task_name} not registered with Celery app")

        task = self.app.tasks[task_name]

        # Submit the task to be processed
        result = task.apply_async(args=args, kwargs=kwargs, queue=self.target_queue)
        logger.info(
            f"Submitted task {task_name} with ID {result.id} from queue {queue_name}"
        )

        # Update stats
        self.queue_manager.increment_stat(queue_name, "tasks_submitted", 1)

        return result


class RateLimitedTaskSubmitter:
    """Submits tasks to queues, respecting rate limits."""

    def __init__(
        self,
        redis_client: redis.Redis,
        queue_manager: UniversalQueueManager,
        task_processor: RateLimitedTaskProcessor,
    ):
        self.redis = redis_client
        self.queue_manager = queue_manager
        self.task_processor = task_processor

    def submit_task(self, queue_name: str, task_name: str, *args, **kwargs) -> bool:
        """
        Submit a task to a queue. Returns True if submitted, False if rate limited.
        """
        if not self.queue_manager.queue_exists(queue_name):
            logger.error(f"Queue {queue_name} does not exist")
            return False

        # Check if we can process immediately
        can_process, wait_time = self.queue_manager.can_process_task(queue_name)

        if can_process:
            # Submit task immediately
            self.task_processor.execute_task(queue_name, task_name, args, kwargs)
            logger.info(
                f"Task {task_name} submitted to {queue_name} for immediate processing"
            )
            return True
        else:
            # Add to pending queue
            task_queue_key = f"{self.queue_manager.queue_prefix}:queue:{queue_name}"
            task_payload = {
                "task_name": task_name,
                "args": args,
                "kwargs": kwargs,
                "submitted_at": datetime.now().isoformat(),
                "wait_time": wait_time,
            }
            self.redis.lpush(task_queue_key, json.dumps(task_payload))
            logger.info(
                f"Task {task_name} queued for {queue_name}, next available in {wait_time:.2f}s"
            )
            return False

    def submit_multiple_tasks(
        self, queue_name: str, tasks_list: List[tuple]
    ) -> Dict[str, int]:
        """Submit multiple tasks to a queue. Each task should be (task_name, args, kwargs)."""
        stats = {"submitted": 0, "queued": 0}

        for task_info in tasks_list:
            if len(task_info) == 3:
                task_name, args, kwargs = task_info
            elif len(task_info) == 2:
                task_name, args = task_info
                kwargs = {}
            else:
                task_name = task_info[0]
                args = ()
                kwargs = {}

            if self.submit_task(queue_name, task_name, *args, **kwargs):
                stats["submitted"] += 1
            else:
                stats["queued"] += 1

        return stats


class RateLimitedTaskDispatcher:
    """Dispatches queued tasks when tokens become available."""

    def __init__(
        self,
        redis_client: redis.Redis,
        queue_manager: UniversalQueueManager,
        task_processor: RateLimitedTaskProcessor,
    ):
        self.redis = redis_client
        self.queue_manager = queue_manager
        self.task_processor = task_processor

    def dispatch_pending_tasks(self):
        """Check all active queues and dispatch pending tasks when tokens are available."""
        active_queues = self.queue_manager.get_active_queues()
        dispatched_count = 0
        next_check_times = {}

        for queue_config in active_queues:
            queue_name = queue_config.name
            task_queue_key = f"{self.queue_manager.queue_prefix}:queue:{queue_name}"

            # Check if there are pending tasks
            if self.redis.llen(task_queue_key) == 0:
                continue

            # Try to acquire token
            can_process, wait_time = self.queue_manager.can_process_task(queue_name)

            if can_process:
                # Get next task from queue
                task_payload_json = self.redis.rpop(task_queue_key)
                if task_payload_json:
                    try:
                        task_payload = json.loads(task_payload_json.decode())
                        task_name = task_payload["task_name"]
                        args = tuple(task_payload.get("args", []))
                        kwargs = task_payload.get("kwargs", {})

                        # Execute the task
                        self.task_processor.execute_task(
                            queue_name, task_name, args, kwargs
                        )

                        dispatched_count += 1
                        logger.info(
                            f"Dispatched pending task {task_name} from {queue_name}"
                        )

                    except json.JSONDecodeError as e:
                        logger.error(
                            f"Failed to decode task payload from {queue_name}: {e}"
                        )
                        self.queue_manager.increment_stat(queue_name, "tasks_failed", 1)
                    except Exception as e:
                        logger.error(f"Failed to execute task from {queue_name}: {e}")
                        self.queue_manager.increment_stat(queue_name, "tasks_failed", 1)
            else:
                # Store when this queue should be checked again
                if wait_time > 0:
                    next_check_times[queue_name] = time.time() + wait_time

        return dispatched_count, next_check_times

    def run_dispatcher(self, interval: float = 0.5):
        """Run the dispatcher in a loop with adaptive timing."""
        logger.info(f"Starting task dispatcher with base {interval}s interval")

        while True:
            try:
                dispatched, next_check_times = self.dispatch_pending_tasks()
                if dispatched > 0:
                    logger.info(f"Dispatched {dispatched} tasks")

                # Calculate sleep time - use shorter interval if tasks were dispatched
                # or if any queue will have tokens available soon
                sleep_time = interval
                if next_check_times:
                    current_time = time.time()
                    min_wait_time = min(
                        check_time - current_time
                        for check_time in next_check_times.values()
                    )
                    if min_wait_time > 0:
                        # Sleep until the soonest token becomes available, but cap at interval
                        sleep_time = min(interval, min_wait_time)

                if dispatched > 0:
                    sleep_time = min(sleep_time, 0.1)  # Check quickly after success

                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("Dispatcher stopped by user")
                break
            except Exception as e:
                logger.error(f"Dispatcher error: {e}")
                time.sleep(1)  # Wait longer on error
