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

    queue = queue_manager.create_queue("1/1s")  # 10 tasks per 10 seconds
    print(f"\tQueue: {queue}")

    # Submit some test tasks
    print(f"\n5. Submitting test tasks to fast queue...")
    for i in range(10):
        task_data = {
            "task_number": i + 1,
            "message": f"Test task {i + 1}",
            "submitted_at": datetime.now().isoformat()
        }
        submitted = task_submitter.submit_task(queue, task_data)
        if submitted:
            print(f"\tTask {i + 1}: Submitted for immediate processing")
        else:
            print(f"\tTask {i + 1}: Queued (rate limited)")


    print(f"  Terminal 3: python scripts/monitor.py")

    return [queue]


def cleanup_demo_queues(queue_names):
    """Clean up demo queues."""
    print(f"\nCleaning up demo queues...")
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
    queue_manager = UniversalQueueManager(redis_client)

    for queue_name in queue_names:
        if queue_manager.queue_exists(queue_name):
            queue_manager.remove_queue(queue_name)
            print(f"\tRemoved {queue_name}")


def main():
    print("Rate Limiting System Demo")
    print("Make sure Redis is running before starting this demo!")

    try:
        # Basic functionality demo
        queue_names = demo_basic_functionality()

        # Ask user if they want to cleanup
        try:
            response = input(f"\nCleanup demo queues? (y/n): ").lower().strip()
            if response == 'y':
                cleanup_demo_queues(queue_names)
            else:
                print(f"Demo queues left for inspection. Clean up manually with:")
                for queue_name in queue_names:
                    print(f"\tpython scripts/queue_manager_cli.py remove {queue_name}")
        except EOFError:
            print(f"\nDemo queues left for inspection. Clean up manually with:")
            for queue_name in queue_names:
                print(f"\tpython scripts/queue_manager_cli.py remove {queue_name}")

    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
