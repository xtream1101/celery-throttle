import time
import random
import json
from typing import Any, Dict, List, Optional
from datetime import datetime
import redis
from celery import Celery
from loguru import logger

from ..queue.manager import UniversalQueueManager


class RateLimitedTaskProcessor:
    """Handles task processing with rate limiting integration."""

    def __init__(self, celery_app: Celery, redis_client: redis.Redis, queue_manager: UniversalQueueManager, target_queue: str = "rate_limited_tasks"):
        self.app = celery_app
        self.redis = redis_client
        self.queue_manager = queue_manager
        self.target_queue = target_queue
        self._register_task()

    def _register_task(self):
        """Register the rate-limited task with the Celery app."""
        @self.app.task(bind=True, queue=self.target_queue)
        def process_rate_limited_task(task_self, queue_name: str, task_data: Dict[str, Any]):
            """
            Process a task with rate limiting.
            This task should only be called when we know a token is available.
            """
            task_id = task_self.request.id
            start_time = time.time()

            logger.info(f"Starting task {task_id} for queue {queue_name}")

            # Mark task as processing
            processing_key = f"{self.queue_manager.queue_prefix}:processing:{queue_name}"
            self.redis.sadd(processing_key, task_id)

            # Update stats
            self.queue_manager.increment_stat(queue_name, "tasks_processing", 1)

            try:
                # Simulate work with random sleep (customize this in your implementation)
                work_duration = random.uniform(0.1, 2.0)  # 0.1 to 2 seconds
                logger.info(f"Task {task_id} processing for {work_duration:.2f}s...")
                time.sleep(work_duration)

                # Task completed successfully
                end_time = time.time()
                logger.info(f"Task {task_id} completed in {end_time - start_time:.2f}s")

                # Update stats
                self.queue_manager.increment_stat(queue_name, "tasks_completed", 1)
                self.queue_manager.increment_stat(queue_name, "tasks_processing", -1)

                return {
                    "status": "success",
                    "queue_name": queue_name,
                    "task_id": task_id,
                    "duration": end_time - start_time,
                    "work_duration": work_duration,
                    "completed_at": datetime.now().isoformat()
                }

            except Exception as e:
                logger.error(f"Task {task_id} failed: {e}")

                # Update stats
                self.queue_manager.increment_stat(queue_name, "tasks_failed", 1)
                self.queue_manager.increment_stat(queue_name, "tasks_processing", -1)

                raise
            finally:
                # Remove from processing set
                self.redis.srem(processing_key, task_id)

        # Store reference to the task for external access
        self.process_rate_limited_task = process_rate_limited_task


class RateLimitedTaskSubmitter:
    """Submits tasks to queues, respecting rate limits."""

    def __init__(self, redis_client: redis.Redis, queue_manager: UniversalQueueManager,
                 task_processor: RateLimitedTaskProcessor):
        self.redis = redis_client
        self.queue_manager = queue_manager
        self.task_processor = task_processor

    def submit_task(self, queue_name: str, task_data: Dict[str, Any]) -> bool:
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
            self.task_processor.process_rate_limited_task.apply_async(
                args=[queue_name, task_data],
                queue=self.task_processor.target_queue
            )
            logger.info(f"Task submitted to {queue_name} for immediate processing")
            return True
        else:
            # Add to pending queue
            task_queue_key = f"{self.queue_manager.queue_prefix}:queue:{queue_name}"
            task_payload = {
                "data": task_data,
                "submitted_at": datetime.now().isoformat(),
                "wait_time": wait_time
            }
            self.redis.lpush(task_queue_key, json.dumps(task_payload))
            logger.info(f"Task queued for {queue_name}, next available in {wait_time:.2f}s")
            return False

    def submit_multiple_tasks(self, queue_name: str, tasks_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Submit multiple tasks to a queue."""
        stats = {"submitted": 0, "queued": 0}

        for task_data in tasks_data:
            if self.submit_task(queue_name, task_data):
                stats["submitted"] += 1
            else:
                stats["queued"] += 1

        return stats


class RateLimitedTaskDispatcher:
    """Dispatches queued tasks when tokens become available."""

    def __init__(self, redis_client: redis.Redis, queue_manager: UniversalQueueManager,
                 task_processor: RateLimitedTaskProcessor):
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
                        task_data = task_payload["data"]

                        # Submit task for processing
                        self.task_processor.process_rate_limited_task.apply_async(
                            args=[queue_name, task_data],
                            queue=self.task_processor.target_queue
                        )

                        dispatched_count += 1
                        logger.info(f"Dispatched pending task from {queue_name}")

                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode task payload from {queue_name}: {e}")
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
                    min_wait_time = min(check_time - current_time for check_time in next_check_times.values())
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