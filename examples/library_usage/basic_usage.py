#!/usr/bin/env python3
"""
Basic usage example of the celery-throttle library.

This example shows how to use the library programmatically with default configuration.
"""

from celery_throttle import CeleryThrottle
from datetime import datetime
import time


def main():
    # Initialize CeleryThrottle with default configuration
    throttle = CeleryThrottle()

    print("CeleryThrottle Basic Usage Example")
    print("=" * 40)

    # Create a queue with a rate limit of 5 requests per 10 seconds
    queue_name = throttle.create_queue("5/10s")
    print(f"Created queue: {queue_name}")

    # Submit some test tasks
    print("\nSubmitting 8 test tasks...")
    for i in range(8):
        task_data = {
            "task_id": i + 1,
            "message": f"Test task {i + 1}",
            "timestamp": datetime.now().isoformat()
        }

        submitted = throttle.submit_task(queue_name, task_data)
        if submitted:
            print(f"  ✓ Task {i + 1} submitted immediately")
        else:
            print(f"  ⏳ Task {i + 1} queued (rate limited)")

    # Show queue stats
    print("\nQueue Statistics:")
    stats = throttle.get_queue_stats(queue_name)
    if stats:
        print(f"  Waiting: {stats.tasks_waiting}")
        print(f"  Processing: {stats.tasks_processing}")
        print(f"  Completed: {stats.tasks_completed}")
        print(f"  Failed: {stats.tasks_failed}")

    # Show rate limit status
    print("\nRate Limit Status:")
    rate_status = throttle.get_rate_limit_status(queue_name)
    if rate_status:
        print(f"  Available tokens: {rate_status['available_tokens']:.2f}/{rate_status['capacity']:.0f}")
        print(f"  Refill rate: {rate_status['refill_rate']:.4f} tokens/sec")
        if rate_status['next_token_in'] > 0:
            print(f"  Next token in: {rate_status['next_token_in']:.2f} seconds")

    print(f"\nTo start processing tasks, run:")
    print(f"  celery-throttle worker")
    print(f"  celery-throttle dispatcher")

    print(f"\nTo monitor the queue:")
    print(f"  celery-throttle queue show {queue_name}")

    # Clean up
    print(f"\nCleaning up queue: {queue_name}")
    throttle.remove_queue(queue_name)
    print("Example completed!")


if __name__ == "__main__":
    main()