#!/usr/bin/env python3
"""
Basic example of dedicated worker pools.

This shows the simplest way to set up a dedicated worker pool
for rate-limited tasks separate from your regular Celery workers.
"""

from celery import Celery
from celery_throttle import CeleryThrottle, CeleryThrottleConfig
from datetime import datetime


def main():
    print("Basic Dedicated Worker Example")
    print("=" * 40)

    # Configure rate limiting with a dedicated queue
    config = CeleryThrottleConfig(
        app_name="my_rate_limited_app",
        target_queue="my_rate_limited_queue",  # Tasks will be sent to this queue
        queue_prefix="my_app"  # Redis keys will be prefixed with this
    )

    # Initialize throttle system
    throttle = CeleryThrottle(config=config)

    # Create a rate-limited queue
    queue_name = throttle.create_queue("10/60s")  # 10 requests per minute
    print(f"Created queue: {queue_name}")

    # Submit some tasks
    print("\nSubmitting tasks...")
    for i in range(5):
        task_data = {"task_id": i, "timestamp": datetime.now().isoformat()}
        submitted = throttle.submit_task(queue_name, task_data)
        status = "immediate" if submitted else "queued"
        print(f"  Task {i}: {status}")

    print("\nTo process these tasks:")
    print("\n1. Start dedicated worker (only processes rate-limited tasks):")
    print("   python -c \"")
    print("   from examples.library_usage.basic_dedicated_worker import start_dedicated_worker")
    print("   start_dedicated_worker()\"")

    print("\n2. Start dispatcher (moves queued tasks to worker when rate limit allows):")
    print("   python -c \"")
    print("   from examples.library_usage.basic_dedicated_worker import start_dispatcher")
    print("   start_dispatcher()\"")

    print(f"\nQueue stats:")
    stats = throttle.get_queue_stats(queue_name)
    if stats:
        print(f"  Waiting: {stats.tasks_waiting}")
        print(f"  Processing: {stats.tasks_processing}")
        print(f"  Completed: {stats.tasks_completed}")

    # Clean up
    print(f"\nCleaning up...")
    throttle.remove_queue(queue_name)
    print("Example completed!")


def start_dedicated_worker():
    """Start a worker that only processes rate-limited tasks."""
    config = CeleryThrottleConfig(
        app_name="my_rate_limited_app",
        target_queue="my_rate_limited_queue",
        queue_prefix="my_app"
    )

    throttle = CeleryThrottle(config=config)
    print(f"Starting dedicated worker for queue: {config.target_queue}")
    throttle.run_dedicated_worker()


def start_dispatcher():
    """Start the dispatcher that moves queued tasks to workers."""
    config = CeleryThrottleConfig(
        app_name="my_rate_limited_app",
        target_queue="my_rate_limited_queue",
        queue_prefix="my_app"
    )

    throttle = CeleryThrottle(config=config)
    print("Starting dispatcher...")
    throttle.run_dispatcher()


if __name__ == "__main__":
    main()