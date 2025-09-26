"""
Comprehensive demo of the rate limiting system.
This script demonstrates creating queues, submitting tasks, and monitoring.
"""

import sys
import os
import time
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.queue_manager import UniversalQueueManager
from src.tasks import RateLimitedTaskSubmitter
from src.rate_limiter import RateLimit
import redis


def demo_basic_functionality():
    """Demonstrate basic queue operations."""
    print("=" * 60)
    print("DEMO: Basic Queue Operations")
    print("=" * 60)

    # Initialize components
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    queue_manager = UniversalQueueManager(redis_client)
    task_submitter = RateLimitedTaskSubmitter()

    try:
        redis_client.ping()
        print("✓ Connected to Redis")
    except:
        print("✗ Redis connection failed - make sure Redis is running!")
        return

    # Create test queues with different rate limits
    print("\n1. Creating test queues...")

    fast_queue = queue_manager.create_queue("10/10s")  # 10 tasks per 10 seconds
    print(f"   Fast queue: {fast_queue}")

    slow_queue = queue_manager.create_queue("2/60s")   # 2 tasks per 60 seconds
    print(f"   Slow queue: {slow_queue}")

    very_slow_queue = queue_manager.create_queue("1/300s")  # 1 task per 5 minutes
    print(f"   Very slow queue: {very_slow_queue}")

    # List all queues
    print("\n2. Current queues:")
    queues = queue_manager.list_queues()
    for queue in queues:
        status = "ACTIVE" if queue.active else "INACTIVE"
        print(f"   {queue.name}: {queue.rate_limit} ({status})")

    # Show queue details
    print(f"\n3. Details for fast queue ({fast_queue}):")
    config = queue_manager.get_queue_config(fast_queue)
    if config:
        print(f"   Rate Limit: {config.rate_limit}")
        print(f"   Created: {config.created_at}")
        print(f"   Active: {config.active}")

    # Check rate limit status
    print(f"\n4. Rate limit status for fast queue:")
    status = queue_manager.get_rate_limit_status(fast_queue)
    if status:
        print(f"   Available tokens: {status['available_tokens']:.2f}/{status['capacity']:.0f}")
        print(f"   Refill rate: {status['refill_rate']:.4f} tokens/sec")

    # Submit some test tasks
    print(f"\n5. Submitting 5 test tasks to fast queue...")
    for i in range(5):
        task_data = {
            "task_number": i + 1,
            "message": f"Test task {i + 1}",
            "submitted_at": datetime.now().isoformat()
        }

        submitted = task_submitter.submit_task(fast_queue, task_data)
        if submitted:
            print(f"   Task {i + 1}: Submitted for immediate processing")
        else:
            print(f"   Task {i + 1}: Queued (rate limited)")

    # Show updated stats
    print(f"\n6. Updated queue statistics:")
    stats = queue_manager.get_queue_stats(fast_queue)
    if stats:
        print(f"   Waiting: {stats.tasks_waiting}")
        print(f"   Processing: {stats.tasks_processing}")
        print(f"   Completed: {stats.tasks_completed}")
        print(f"   Failed: {stats.tasks_failed}")

    print(f"\n7. Testing rate limit updates...")
    print(f"   Current rate limit: {config.rate_limit}")
    queue_manager.update_rate_limit(fast_queue, "5/30s")
    updated_config = queue_manager.get_queue_config(fast_queue)
    print(f"   Updated rate limit: {updated_config.rate_limit}")

    print(f"\n8. Testing queue deactivation...")
    queue_manager.deactivate_queue(slow_queue)
    print(f"   Deactivated {slow_queue}")

    # Show final queue list
    print(f"\n9. Final queue status:")
    queues = queue_manager.list_queues()
    for queue in queues:
        status = "ACTIVE" if queue.active else "INACTIVE"
        print(f"   {queue.name}: {queue.rate_limit} ({status})")

    print(f"\n✓ Demo completed successfully!")
    print(f"Note: To see tasks actually processing, start the Celery worker and dispatcher:")
    print(f"  Terminal 1: python scripts/run_worker.py")
    print(f"  Terminal 2: python scripts/run_dispatcher.py")
    print(f"  Terminal 3: python scripts/monitor.py")

    return [fast_queue, slow_queue, very_slow_queue]


def demo_rate_limiting_behavior():
    """Demonstrate strict rate limiting behavior."""
    print("\n" + "=" * 60)
    print("DEMO: Rate Limiting Behavior")
    print("=" * 60)

    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    queue_manager = UniversalQueueManager(redis_client)

    # Create a queue with very strict rate limit
    test_queue = queue_manager.create_queue("3/10s")  # 3 tasks per 10 seconds
    print(f"Created test queue: {test_queue} (3 tasks per 10 seconds)")

    print(f"\n1. Testing token bucket behavior...")
    for i in range(8):
        can_process, wait_time = queue_manager.can_process_task(test_queue)
        status = queue_manager.get_rate_limit_status(test_queue)

        print(f"   Attempt {i+1}: Can process={can_process}, "
              f"Wait time={wait_time:.2f}s, "
              f"Available tokens={status['available_tokens']:.2f}")

        if i == 2:  # After exhausting initial tokens
            print(f"   Waiting 3 seconds to allow token refill...")
            time.sleep(3)

    print(f"\n✓ Rate limiting demo completed!")
    return test_queue


def cleanup_demo_queues(queue_names):
    """Clean up demo queues."""
    print(f"\nCleaning up demo queues...")
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    queue_manager = UniversalQueueManager(redis_client)

    for queue_name in queue_names:
        if queue_manager.queue_exists(queue_name):
            queue_manager.remove_queue(queue_name)
            print(f"   Removed {queue_name}")


def main():
    print("Rate Limiting System Demo")
    print("Make sure Redis is running before starting this demo!")

    try:
        # Basic functionality demo
        queue_names = demo_basic_functionality()

        # Rate limiting behavior demo
        test_queue = demo_rate_limiting_behavior()
        queue_names.append(test_queue)

        # Ask user if they want to cleanup
        try:
            response = input(f"\nCleanup demo queues? (y/n): ").lower().strip()
            if response == 'y':
                cleanup_demo_queues(queue_names)
            else:
                print(f"Demo queues left for inspection. Clean up manually with:")
                for queue_name in queue_names:
                    print(f"  python scripts/queue_manager_cli.py remove {queue_name}")
        except EOFError:
            print(f"\nDemo queues left for inspection. Clean up manually with:")
            for queue_name in queue_names:
                print(f"  python scripts/queue_manager_cli.py remove {queue_name}")

    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
