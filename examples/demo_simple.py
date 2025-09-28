import time
from datetime import datetime
from celery_throttle import CeleryThrottle


def demo_basic_functionality():
    # Initialize CeleryThrottle
    throttle = CeleryThrottle()

    try:
        throttle.redis.ping()
        print("✓ Connected to Redis")
    except:
        print("✗ Redis connection failed - make sure Redis is running!")
        return

    # Create test queues with different rate limits
    print("\n1. Creating test queues...")

    queue = throttle.create_queue("10/10s:5")  # 10 tasks per 10 seconds
    print(f"\tQueue: {queue}")

    # Submit some test tasks
    print(f"\n2. Submitting test tasks to queue...")
    for i in range(10):
        task_data = {
            "task_number": i + 1,
            "message": f"Test task {i + 1}",
            "submitted_at": datetime.now().isoformat()
        }
        submitted = throttle.submit_task(queue, task_data)
        if submitted:
            print(f"\tTask {i + 1}: Submitted for immediate processing")
        else:
            print(f"\tTask {i + 1}: Queued (rate limited)")

    print(f"\n3. To process tasks, run:")
    print(f"\tTerminal 1: celery-throttle worker")
    print(f"\tTerminal 2: celery-throttle dispatcher")
    print(f"\tTerminal 3: celery-throttle monitor")

    return [queue]


def cleanup_demo_queues(queue_names):
    """Clean up demo queues."""
    print(f"\nCleaning up demo queues...")
    throttle = CeleryThrottle()

    for queue_name in queue_names:
        if throttle.queue_manager.queue_exists(queue_name):
            throttle.remove_queue(queue_name)
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
                    print(f"\tcelery-throttle queue remove {queue_name}")
        except EOFError:
            print(f"\nDemo queues left for inspection. Clean up manually with:")
            for queue_name in queue_names:
                print(f"\tcelery-throttle queue remove {queue_name}")

    except Exception as e:
        print(f"Demo failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
