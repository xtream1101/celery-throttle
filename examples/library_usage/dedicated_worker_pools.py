#!/usr/bin/env python3
"""
Example showing how to use dedicated worker pools for rate-limited tasks.

This example demonstrates:
1. Setting up dedicated workers that only process rate-limited tasks
2. Using custom queue names and prefixes for isolation
3. Running multiple isolated setups in the same Redis instance
"""

from celery import Celery
from celery_throttle import CeleryThrottle, CeleryThrottleConfig
import redis
from datetime import datetime


def setup_main_app():
    """Setup for main application (existing Celery tasks)."""
    print("Setting up main application...")

    # Your existing Celery app
    main_app = Celery('main_app')
    main_app.conf.update(
        broker_url='redis://localhost:6379/0',
        result_backend='redis://localhost:6379/0',
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
    )

    # Your existing tasks
    @main_app.task
    def regular_task(data):
        """Regular task that processes normally."""
        print(f"Processing regular task: {data}")
        return {"status": "completed", "data": data}

    return main_app, regular_task


def setup_rate_limited_service_a():
    """Setup for rate-limited service A (e.g., API calls to service A)."""
    print("Setting up rate-limited service A...")

    config = CeleryThrottleConfig(
        app_name="service_a_throttle",
        target_queue="service_a_rate_limited",  # Dedicated queue for service A
        queue_prefix="service_a",  # Redis prefix for isolation
        redis={"db": 0},  # Can use same Redis instance
        celery={
            "broker_url": "redis://localhost:6379/0",
            "result_backend": "redis://localhost:6379/0"
        }
    )

    throttle_a = CeleryThrottle(config=config)
    return throttle_a


def setup_rate_limited_service_b():
    """Setup for rate-limited service B (e.g., API calls to service B)."""
    print("Setting up rate-limited service B...")

    config = CeleryThrottleConfig(
        app_name="service_b_throttle",
        target_queue="service_b_rate_limited",  # Dedicated queue for service B
        queue_prefix="service_b",  # Different Redis prefix for isolation
        redis={"db": 0},  # Same Redis instance, different namespace
        celery={
            "broker_url": "redis://localhost:6379/0",
            "result_backend": "redis://localhost:6379/0"
        }
    )

    throttle_b = CeleryThrottle(config=config)
    return throttle_b


def main():
    print("Dedicated Worker Pools Example")
    print("=" * 50)

    # Setup all services
    main_app, regular_task = setup_main_app()
    throttle_a = setup_rate_limited_service_a()
    throttle_b = setup_rate_limited_service_b()

    # Create rate-limited queues for each service
    service_a_queue = throttle_a.create_queue("100/60s")  # Service A: 100 requests per minute
    service_b_queue = throttle_b.create_queue("10/60s")   # Service B: 10 requests per minute

    print(f"Created Service A queue: {service_a_queue}")
    print(f"Created Service B queue: {service_b_queue}")

    # Submit tasks to different services
    print("\nSubmitting tasks...")

    # Regular tasks (no rate limiting)
    regular_task.delay({"message": "Regular task 1"})
    regular_task.delay({"message": "Regular task 2"})

    # Service A tasks (rate limited)
    for i in range(5):
        task_data = {"api_call": f"service_a_request_{i}", "timestamp": datetime.now().isoformat()}
        throttle_a.submit_task(service_a_queue, task_data)

    # Service B tasks (rate limited)
    for i in range(3):
        task_data = {"api_call": f"service_b_request_{i}", "timestamp": datetime.now().isoformat()}
        throttle_b.submit_task(service_b_queue, task_data)

    print("\nTo run this example with dedicated workers:")
    print("\n1. Terminal 1 - Main app worker (regular tasks only):")
    print("   celery -A main_app worker --queues=celery --loglevel=info")

    print("\n2. Terminal 2 - Service A dedicated worker:")
    print("   # Start Python and run:")
    print("   from examples.library_usage.dedicated_worker_pools import setup_rate_limited_service_a")
    print("   throttle_a = setup_rate_limited_service_a()")
    print("   throttle_a.run_dedicated_worker()  # Only processes service_a_rate_limited queue")

    print("\n3. Terminal 3 - Service B dedicated worker:")
    print("   # Start Python and run:")
    print("   from examples.library_usage.dedicated_worker_pools import setup_rate_limited_service_b")
    print("   throttle_b = setup_rate_limited_service_b()")
    print("   throttle_b.run_dedicated_worker()  # Only processes service_b_rate_limited queue")

    print("\n4. Terminal 4 - Service A dispatcher:")
    print("   # Start Python and run:")
    print("   from examples.library_usage.dedicated_worker_pools import setup_rate_limited_service_a")
    print("   throttle_a = setup_rate_limited_service_a()")
    print("   throttle_a.run_dispatcher()")

    print("\n5. Terminal 5 - Service B dispatcher:")
    print("   # Start Python and run:")
    print("   from examples.library_usage.dedicated_worker_pools import setup_rate_limited_service_b")
    print("   throttle_b = setup_rate_limited_service_b()")
    print("   throttle_b.run_dispatcher()")

    print("\nBenefits of this setup:")
    print("- Regular tasks are not affected by rate limiting")
    print("- Service A and B have dedicated worker pools")
    print("- Each service has isolated Redis namespaces")
    print("- Can scale workers independently per service")
    print("- No queue name conflicts between services")

    # Show queue information
    print(f"\nService A queues:")
    for queue in throttle_a.list_queues():
        print(f"  {queue.name}: {queue.rate_limit}")

    print(f"\nService B queues:")
    for queue in throttle_b.list_queues():
        print(f"  {queue.name}: {queue.rate_limit}")

    # Clean up
    print(f"\nCleaning up...")
    throttle_a.remove_queue(service_a_queue)
    throttle_b.remove_queue(service_b_queue)
    print("Example completed!")


def run_service_a_worker():
    """Helper function to start Service A worker."""
    throttle = setup_rate_limited_service_a()
    print("Starting Service A dedicated worker...")
    throttle.run_dedicated_worker()


def run_service_b_worker():
    """Helper function to start Service B worker."""
    throttle = setup_rate_limited_service_b()
    print("Starting Service B dedicated worker...")
    throttle.run_dedicated_worker()


def run_service_a_dispatcher():
    """Helper function to start Service A dispatcher."""
    throttle = setup_rate_limited_service_a()
    print("Starting Service A dispatcher...")
    throttle.run_dispatcher()


def run_service_b_dispatcher():
    """Helper function to start Service B dispatcher."""
    throttle = setup_rate_limited_service_b()
    print("Starting Service B dispatcher...")
    throttle.run_dispatcher()


if __name__ == "__main__":
    main()