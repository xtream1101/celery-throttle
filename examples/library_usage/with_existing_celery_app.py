#!/usr/bin/env python3
"""
Example showing how to use celery-throttle with an existing Celery app.

This example demonstrates how to integrate rate limiting into an existing
Celery application without changing your existing setup.
"""

from celery import Celery
from celery_throttle import CeleryThrottle
import redis
from datetime import datetime


# Your existing Celery app
app = Celery('my_existing_app')
app.conf.update(
    broker_url='redis://localhost:6379/0',
    result_backend='redis://localhost:6379/0',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Your existing Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)


# Your existing Celery tasks
@app.task
def my_existing_task(data):
    """An existing task that you want to keep."""
    print(f"Processing existing task: {data}")
    return {"status": "completed", "data": data}


def main():
    print("Using CeleryThrottle with Existing Celery App")
    print("=" * 50)

    # Initialize CeleryThrottle with your existing app and Redis client
    throttle = CeleryThrottle(
        celery_app=app,
        redis_client=redis_client
    )

    # Create rate-limited queues
    fast_queue = throttle.create_queue("10/60s")  # 10 requests per minute
    slow_queue = throttle.create_queue("1/10s")   # 1 request per 10 seconds

    print(f"Created fast queue: {fast_queue}")
    print(f"Created slow queue: {slow_queue}")

    # Submit tasks to rate-limited queues
    print("\nSubmitting tasks to fast queue...")
    for i in range(5):
        task_data = {"id": i, "type": "fast_task", "timestamp": datetime.now().isoformat()}
        submitted = throttle.submit_task(fast_queue, task_data)
        status = "immediate" if submitted else "queued"
        print(f"  Task {i}: {status}")

    print("\nSubmitting tasks to slow queue...")
    for i in range(3):
        task_data = {"id": i, "type": "slow_task", "timestamp": datetime.now().isoformat()}
        submitted = throttle.submit_task(slow_queue, task_data)
        status = "immediate" if submitted else "queued"
        print(f"  Task {i}: {status}")

    # Your existing task still works normally
    print("\nSubmitting to existing task...")
    result = my_existing_task.delay({"message": "This is my existing task"})
    print(f"Existing task ID: {result.id}")

    # Show all queues
    print("\nAll queues:")
    for queue in throttle.list_queues():
        print(f"  {queue.name}: {queue.rate_limit} ({'active' if queue.active else 'inactive'})")

    print("\nTo start processing:")
    print("  # Start your worker (handles both rate-limited and regular tasks)")
    print("  celery -A my_existing_app worker --loglevel=info")
    print("  ")
    print("  # Start the rate-limited task dispatcher")
    print("  celery-throttle dispatcher")

    # Clean up
    print(f"\nCleaning up...")
    throttle.remove_queue(fast_queue)
    throttle.remove_queue(slow_queue)
    print("Example completed!")


if __name__ == "__main__":
    main()