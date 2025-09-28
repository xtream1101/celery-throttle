#!/usr/bin/env python3
"""
Example showing how to customize the task processor for your specific needs.

This example demonstrates how to override the default task processing behavior
to implement custom business logic while maintaining rate limiting.
"""

import time
import random
from typing import Any, Dict
from datetime import datetime
from celery import Celery
from celery_throttle import CeleryThrottle
from celery_throttle.tasks.processor import RateLimitedTaskProcessor
import logging

# Set up logger for this example
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
import redis


class CustomTaskProcessor(RateLimitedTaskProcessor):
    """Custom task processor with specialized business logic."""

    def _register_task(self):
        """Register a custom rate-limited task with specialized processing."""

        @self.app.task(bind=True)
        def process_rate_limited_task(task_self, queue_name: str, task_data: Dict[str, Any]):
            """Custom task processor with different behavior based on task type."""
            task_id = task_self.request.id
            start_time = time.time()

            logger.info(f"Starting custom task {task_id} for queue {queue_name}")

            # Mark task as processing
            processing_key = f"processing:{queue_name}"
            self.redis.sadd(processing_key, task_id)
            self.queue_manager.increment_stat(queue_name, "tasks_processing", 1)

            try:
                # Custom processing based on task type
                task_type = task_data.get("type", "default")

                if task_type == "data_processing":
                    result = self._process_data_task(task_data)
                elif task_type == "api_call":
                    result = self._process_api_task(task_data)
                elif task_type == "file_processing":
                    result = self._process_file_task(task_data)
                else:
                    result = self._process_default_task(task_data)

                end_time = time.time()
                logger.info(f"Custom task {task_id} completed in {end_time - start_time:.2f}s")

                # Update stats
                self.queue_manager.increment_stat(queue_name, "tasks_completed", 1)
                self.queue_manager.increment_stat(queue_name, "tasks_processing", -1)

                return {
                    "status": "success",
                    "queue_name": queue_name,
                    "task_id": task_id,
                    "task_type": task_type,
                    "duration": end_time - start_time,
                    "result": result,
                    "completed_at": datetime.now().isoformat()
                }

            except Exception as e:
                logger.error(f"Custom task {task_id} failed: {e}")

                # Update stats
                self.queue_manager.increment_stat(queue_name, "tasks_failed", 1)
                self.queue_manager.increment_stat(queue_name, "tasks_processing", -1)

                raise
            finally:
                # Remove from processing set
                self.redis.srem(processing_key, task_id)

        # Store reference to the task
        self.process_rate_limited_task = process_rate_limited_task

    def _process_data_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a data analysis task."""
        data_size = task_data.get("data_size", 100)
        processing_time = data_size * 0.01  # Simulate processing time based on data size

        logger.info(f"Processing data task with {data_size} records...")
        time.sleep(processing_time)

        return {
            "records_processed": data_size,
            "processing_time": processing_time,
            "output_file": f"processed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        }

    def _process_api_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process an API call task with retry logic."""
        endpoint = task_data.get("endpoint", "/default")
        max_retries = task_data.get("max_retries", 3)

        logger.info(f"Making API call to {endpoint}...")

        for attempt in range(max_retries):
            try:
                # Simulate API call with random failure
                time.sleep(random.uniform(0.5, 2.0))

                if random.random() < 0.3:  # 30% chance of failure
                    raise Exception(f"API call failed (attempt {attempt + 1})")

                return {
                    "endpoint": endpoint,
                    "status_code": 200,
                    "attempt": attempt + 1,
                    "response_time": random.uniform(0.5, 2.0)
                }

            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                logger.warning(f"API call attempt {attempt + 1} failed, retrying...")
                time.sleep(1)  # Wait before retry

    def _process_file_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a file processing task."""
        file_path = task_data.get("file_path", "default.txt")
        file_size = task_data.get("file_size", 1024)

        logger.info(f"Processing file {file_path} ({file_size} bytes)...")

        # Simulate file processing time based on size
        processing_time = file_size / 10000  # Simulate 10KB/s processing
        time.sleep(processing_time)

        return {
            "file_path": file_path,
            "file_size": file_size,
            "processing_time": processing_time,
            "checksum": f"md5_{hash(file_path) % 1000000:06d}"
        }

    def _process_default_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a default task."""
        duration = random.uniform(0.1, 1.0)
        logger.info(f"Processing default task for {duration:.2f}s...")
        time.sleep(duration)

        return {
            "message": task_data.get("message", "Default task completed"),
            "processing_duration": duration
        }


def main():
    print("Custom Task Processor Example")
    print("=" * 40)

    # Create Celery app
    app = Celery('custom-task-app')
    app.conf.update(
        broker_url='redis://localhost:6379/0',
        result_backend='redis://localhost:6379/0',
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        worker_concurrency=1,
    )

    # Create Redis client
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)

    # Initialize CeleryThrottle with custom processor
    throttle = CeleryThrottle(celery_app=app, redis_client=redis_client)

    # Instantiate custom processor with the throttle's queue manager and set it:
    custom_processor = CustomTaskProcessor(app, redis_client, throttle.queue_manager)
    throttle.set_task_processor(custom_processor)

    # Create different queues for different task types
    data_queue = throttle.create_queue("2/10s")    # 2 data tasks per 10 seconds
    api_queue = throttle.create_queue("5/60s")     # 5 API calls per minute
    file_queue = throttle.create_queue("1/5s")     # 1 file task per 5 seconds

    print(f"Created data processing queue: {data_queue}")
    print(f"Created API call queue: {api_queue}")
    print(f"Created file processing queue: {file_queue}")

    # Submit different types of tasks
    print("\nSubmitting data processing tasks...")
    for i in range(3):
        task_data = {
            "type": "data_processing",
            "data_size": random.randint(50, 200),
            "task_id": f"data_{i}"
        }
        submitted = throttle.submit_task(data_queue, task_data)
        print(f"  Data task {i}: {'immediate' if submitted else 'queued'}")

    print("\nSubmitting API call tasks...")
    for i in range(4):
        task_data = {
            "type": "api_call",
            "endpoint": f"/api/endpoint_{i}",
            "max_retries": 3,
            "task_id": f"api_{i}"
        }
        submitted = throttle.submit_task(api_queue, task_data)
        print(f"  API task {i}: {'immediate' if submitted else 'queued'}")

    print("\nSubmitting file processing tasks...")
    for i in range(2):
        task_data = {
            "type": "file_processing",
            "file_path": f"input_file_{i}.txt",
            "file_size": random.randint(1000, 10000),
            "task_id": f"file_{i}"
        }
        submitted = throttle.submit_task(file_queue, task_data)
        print(f"  File task {i}: {'immediate' if submitted else 'queued'}")

    # Show queue statistics
    print("\nQueue Statistics:")
    for queue_name in [data_queue, api_queue, file_queue]:
        stats = throttle.get_queue_stats(queue_name)
        if stats:
            print(f"  {queue_name[:12]}... - Waiting: {stats.tasks_waiting}, "
                  f"Processing: {stats.tasks_processing}")

    print("\nTo process tasks with custom logic:")
    print("  # Start worker")
    print("  celery -A custom-task-app worker --loglevel=info")
    print("  ")
    print("  # Start dispatcher")
    print("  celery-throttle dispatcher")

    # Clean up
    print(f"\nCleaning up...")
    throttle.remove_queue(data_queue)
    throttle.remove_queue(api_queue)
    throttle.remove_queue(file_queue)
    print("Custom task processor example completed!")


if __name__ == "__main__":
    main()
