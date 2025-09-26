import os
import time
import random
import redis
from celery import Celery, Task
from kombu import Queue
from typing import Dict, Any, Optional
from rate_limiter import RedisTokenBucketRateLimiter, TokenBucketConfig
from queue_metrics import QueueMetricsCollector

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
rate_limiter = RedisTokenBucketRateLimiter(redis_client)
metrics_collector = QueueMetricsCollector(redis_client)

# Celery app configuration
app = Celery('rate_limited_queues')

app.conf.update(
    # Redis as broker and result backend
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',

    # Worker settings for strict rate limiting
    worker_prefetch_multiplier=1,  # Only fetch one task at a time
    task_acks_late=True,           # Ack after task completion
    task_reject_on_worker_lost=True,  # Re-queue tasks if worker dies

    # Task routing - all tasks go through our custom router
    task_routes={
        'celery_app.process_task': {'queue': 'default'},
    },

    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',

    # Task time limits
    task_time_limit=300,  # 5 minutes hard limit
    task_soft_time_limit=240,  # 4 minutes soft limit

    # Result expiration
    result_expires=3600,  # 1 hour
)

class RateLimitedTask(Task):
    """Custom task class that enforces rate limiting before execution."""

    def retry_with_rate_limit(self, queue_name: str, rate_limit: float, *args, **kwargs):
        """Retry task with rate limiting applied."""
        config = TokenBucketConfig(requests_per_minute=rate_limit, queue_name=queue_name)

        # Try to acquire token
        success, wait_time = rate_limiter.acquire_token(config)

        if not success:
            # Need to wait - retry after the calculated wait time
            raise self.retry(countdown=wait_time, max_retries=None)

        # Token acquired, proceed with task
        return True

@app.task(bind=True, base=RateLimitedTask)
def process_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main task processor that handles rate limiting and task execution.

    task_data should contain:
    - queue_name: str
    - rate_limit: float (requests per minute)
    - payload: Any (the actual task data)
    - task_id: str (unique identifier)
    """
    queue_name = task_data['queue_name']
    rate_limit = task_data['rate_limit']
    payload = task_data['payload']
    task_id = task_data.get('task_id', self.request.id)

    start_time = time.time()

    try:
        # Record task start
        metrics_collector.record_task_start(queue_name, task_id, start_time)

        # Apply rate limiting
        self.retry_with_rate_limit(queue_name, rate_limit)

        # Simulate API request work (replace with actual work)
        work_duration = random.uniform(0.5, 2.0)  # 0.5 to 2 seconds
        time.sleep(work_duration)

        # Record successful completion
        end_time = time.time()
        processing_time = end_time - start_time

        metrics_collector.record_task_completion(
            queue_name, task_id, start_time, end_time, 'success'
        )

        return {
            'status': 'success',
            'task_id': task_id,
            'queue_name': queue_name,
            'processing_time': processing_time,
            'payload_size': len(str(payload)),
            'completed_at': end_time
        }

    except Exception as e:
        # Record failure
        end_time = time.time()
        processing_time = end_time - start_time

        metrics_collector.record_task_completion(
            queue_name, task_id, start_time, end_time, 'failed', str(e)
        )

        # Re-raise to let Celery handle retries if configured
        raise

def get_active_queues() -> list[str]:
    """Get list of all active queues from Redis."""
    # Look for queue-specific keys to discover active queues
    queue_keys = redis_client.keys("queue:*:config")
    queues = []

    for key in queue_keys:
        if isinstance(key, bytes):
            key = key.decode()
        queue_name = key.replace("queue:", "").replace(":config", "")
        queues.append(queue_name)

    return queues

def update_worker_queues():
    """Update worker to listen to all active queues."""
    active_queues = get_active_queues()

    # Always include default queue
    if 'default' not in active_queues:
        active_queues.append('default')

    # Create Queue objects for Celery
    queue_objects = []
    for queue_name in active_queues:
        queue_objects.append(Queue(queue_name, routing_key=queue_name))

    # Update app configuration
    app.conf.task_queues = tuple(queue_objects)

    return active_queues

# Initialize with default queue
app.conf.task_queues = (Queue('default'),)

if __name__ == '__main__':
    # Run worker
    app.start()