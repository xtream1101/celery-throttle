import time
import random
import json
from typing import Any, Dict, List
from datetime import datetime
import redis
from celery import Celery
from loguru import logger

from .queue_manager import UniversalQueueManager


# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
queue_manager = UniversalQueueManager(redis_client)

# Celery app configuration
app = Celery('rate_limited_worker')
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    # Critical: Set prefetch to 1 to prevent workers from hoarding tasks
    worker_prefetch_multiplier=1,
    # Prevent task prefetching
    task_acks_late=True,
    # Configure worker to only take one task at a time
    worker_concurrency=1,
)


@app.task(bind=True)
def process_rate_limited_task(self, queue_name: str, task_data: Dict[str, Any]):
    """
    Process a task with rate limiting.
    This task should only be called when we know a token is available.
    """
    task_id = self.request.id
    start_time = time.time()

    logger.info(f"Starting task {task_id} for queue {queue_name}")

    # Mark task as processing
    processing_key = f"processing:{queue_name}"
    redis_client.sadd(processing_key, task_id)

    # Update stats
    queue_manager.increment_stat(queue_name, "tasks_processing", 1)

    try:
        # Simulate work with random sleep
        work_duration = random.uniform(0.1, 2.0)  # 0.1 to 2 seconds
        logger.info(f"Task {task_id} processing for {work_duration:.2f}s...")
        time.sleep(work_duration)

        # Task completed successfully
        end_time = time.time()
        logger.info(f"Task {task_id} completed in {end_time - start_time:.2f}s")

        # Update stats
        queue_manager.increment_stat(queue_name, "tasks_completed", 1)
        queue_manager.increment_stat(queue_name, "tasks_processing", -1)

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
        queue_manager.increment_stat(queue_name, "tasks_failed", 1)
        queue_manager.increment_stat(queue_name, "tasks_processing", -1)

        raise
    finally:
        # Remove from processing set
        redis_client.srem(processing_key, task_id)


class RateLimitedTaskSubmitter:
    """Submits tasks to queues, respecting rate limits."""

    def __init__(self):
        self.redis = redis_client
        self.queue_manager = queue_manager

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
            process_rate_limited_task.apply_async(
                args=[queue_name, task_data],
                queue=queue_name
            )
            logger.info(f"Task submitted to {queue_name} for immediate processing")
            return True
        else:
            # Add to pending queue
            task_queue_key = f"queue:{queue_name}"
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

    def __init__(self):
        self.redis = redis_client
        self.queue_manager = queue_manager

    def dispatch_pending_tasks(self):
        """Check all active queues and dispatch pending tasks when tokens are available."""
        active_queues = self.queue_manager.get_active_queues()
        dispatched_count = 0

        for queue_config in active_queues:
            queue_name = queue_config.name
            task_queue_key = f"queue:{queue_name}"

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
                        process_rate_limited_task.apply_async(
                            args=[queue_name, task_data],
                            queue=queue_name
                        )

                        dispatched_count += 1
                        logger.info(f"Dispatched pending task from {queue_name}")

                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode task payload from {queue_name}: {e}")
                        queue_manager.increment_stat(queue_name, "tasks_failed", 1)

        return dispatched_count

    def run_dispatcher(self, interval: float = 0.1):
        """Run the dispatcher in a loop."""
        logger.info(f"Starting task dispatcher with {interval}s interval")

        while True:
            try:
                dispatched = self.dispatch_pending_tasks()
                if dispatched > 0:
                    logger.debug(f"Dispatched {dispatched} tasks")
                time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Dispatcher stopped by user")
                break
            except Exception as e:
                logger.error(f"Dispatcher error: {e}")
                time.sleep(1)  # Wait longer on error