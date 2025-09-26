"""
Universal queue routing system that handles dynamic queues without worker restarts

The key insight: Instead of creating actual Celery queues dynamically,
route all tasks through a single 'universal' queue but maintain logical
separation through task metadata and smart dispatching.
"""

import time
from typing import Dict, Any, List
from redis import Redis
from celery import Celery
from loguru import logger

from rate_limit_registry import RateLimitRegistry


class UniversalQueueRouter:
    """
    Routes tasks from dynamic logical queues through a single physical Celery queue
    while maintaining rate limiting per logical queue
    """

    def __init__(self, celery_app: Celery, redis_connection: Redis,
                 universal_queue_name: str = "universal_queue"):
        self.celery_app = celery_app
        self.redis = redis_connection
        self.universal_queue = universal_queue_name
        self.registry = RateLimitRegistry(redis_connection)

    def dispatch_to_logical_queue(self, task_func, task_args: list,
                                logical_queue_name: str, priority: int = 5, **kwargs):
        """
        Dispatch a task to a logical queue (routed through universal queue)

        Args:
            task_func: The Celery task function
            task_args: Arguments for the task
            logical_queue_name: The logical queue name (for rate limiting)
            priority: Task priority
            **kwargs: Additional arguments for apply_async
        """

        # Ensure the logical queue is registered with rate limiting
        if not self.registry.get_config(logical_queue_name):
            # Auto-register with default rate limit if not found
            logger.warning(f"Auto-registering logical queue '{logical_queue_name}' with default rate limit")
            from rate_limit_registry import RateLimitConfig
            default_config = RateLimitConfig(
                queue_name=logical_queue_name,
                refill_frequency=1.0,  # 1 request per second default
            )
            self.registry.register_queue(default_config)

        # Calculate optimal execution time
        next_execution_time = self.registry.calculate_next_execution_time(logical_queue_name)
        current_time = time.time()
        delay = max(0, next_execution_time - current_time)

        # Wrap the original task args with routing metadata
        wrapped_args = [{
            'original_args': task_args,
            'logical_queue': logical_queue_name,
            'scheduled_time': next_execution_time,
            'routing_metadata': {
                'dispatch_time': current_time,
                'calculated_delay': delay
            }
        }]

        # Prepare dispatch arguments
        dispatch_kwargs = {
            'queue': self.universal_queue,  # Always use universal queue
            'priority': priority,
            **kwargs
        }

        # Schedule with ETA if there's significant delay
        if delay > 0.1:
            from datetime import datetime
            eta = datetime.fromtimestamp(next_execution_time)
            dispatch_kwargs['eta'] = eta
            logger.debug(f"Scheduling task for logical queue '{logical_queue_name}' "
                        f"at {eta.strftime('%H:%M:%S.%f')[:-3]} (delay: {delay:.3f}s)")
        else:
            logger.debug(f"Dispatching task for logical queue '{logical_queue_name}' immediately")

        # Record the intended execution time
        self.registry.record_execution(logical_queue_name, next_execution_time)

        # Dispatch to universal queue with wrapped args
        result = task_func.apply_async(args=wrapped_args, **dispatch_kwargs)

        return result

    def batch_dispatch_to_logical_queues(self, tasks: List[Dict[str, Any]]) -> List[Any]:
        """
        Batch dispatch tasks to logical queues through universal queue

        Args:
            tasks: List of task dictionaries with keys:
                - task_func: Celery task function
                - args: Original task arguments
                - logical_queue: Logical queue name
                - priority: Optional priority
        """
        results = []

        # Group by logical queue for optimal scheduling
        queue_groups = {}
        for task in tasks:
            logical_queue = task['logical_queue']
            if logical_queue not in queue_groups:
                queue_groups[logical_queue] = []
            queue_groups[logical_queue].append(task)

        # Process each logical queue's tasks
        for logical_queue, queue_tasks in queue_groups.items():
            logger.info(f"Processing {len(queue_tasks)} tasks for logical queue '{logical_queue}'")

            for task in queue_tasks:
                result = self.dispatch_to_logical_queue(
                    task_func=task['task_func'],
                    task_args=task['args'],
                    logical_queue_name=logical_queue,
                    priority=task.get('priority', 5)
                )
                results.append(result)

        return results

    def register_logical_queue(self, logical_queue_name: str, refill_frequency: float,
                             capacity: int = 1, refill_amount: int = 1):
        """Register a logical queue with its rate limit configuration"""
        from rate_limit_registry import RateLimitConfig

        config = RateLimitConfig(
            queue_name=logical_queue_name,
            refill_frequency=refill_frequency,
            capacity=capacity,
            refill_amount=refill_amount
        )
        self.registry.register_queue(config)
        logger.info(f"Registered logical queue '{logical_queue_name}' "
                   f"with rate limit {1/refill_frequency:.2f}/sec")

    def get_logical_queue_stats(self, logical_queue_name: str) -> Dict[str, Any]:
        """Get statistics for a logical queue"""
        return self.registry.get_queue_stats(logical_queue_name)

    def list_logical_queues(self) -> List[str]:
        """List all registered logical queues"""
        return self.registry.list_registered_queues()

    def get_system_overview(self) -> Dict[str, Any]:
        """Get overview of all logical queues"""
        logical_queues = self.list_logical_queues()
        overview = {
            'universal_queue': self.universal_queue,
            'total_logical_queues': len(logical_queues),
            'logical_queues': {}
        }

        for queue_name in logical_queues:
            overview['logical_queues'][queue_name] = self.get_logical_queue_stats(queue_name)

        return overview


# Create the universal task that handles all routed tasks
def create_universal_task(celery_app: Celery, redis_conn: Redis):
    """Create the universal task handler"""

    @celery_app.task(bind=True, name='universal_task_handler')
    def universal_task_handler(self, wrapped_data):
        """
        Universal task handler that processes tasks from any logical queue
        with appropriate rate limiting
        """
        # Extract routing information
        original_args = wrapped_data['original_args']
        logical_queue = wrapped_data['logical_queue']
        scheduled_time = wrapped_data['scheduled_time']

        task_start_time = time.time()
        task_id = self.request.id

        # Simple rate limiting check (no blocking limiter needed for scheduled tasks)
        # Since tasks are pre-scheduled with ETA, they should arrive ready to execute

        # Get registry for execution recording
        registry = RateLimitRegistry(redis_conn)

        # Check if we're on schedule
        actual_delay = registry.get_delay_until_ready(logical_queue)
        if actual_delay > 0.01:
            logger.warning(f"Task for logical queue '{logical_queue}' "
                          f"has unexpected delay: {actual_delay:.3f}s")

        # Execute the task (no blocking needed since it's pre-scheduled)
        execution_time = time.time()

        # Record execution
        registry.record_execution(logical_queue, execution_time)

        # Process the original task with long processing time for demonstration
        processing_start = time.time()

        if original_args and len(original_args) > 0:
            item = original_args[0]
            logger.info(f"🔄 STARTED processing {item.get('item', 'unknown')} "
                       f"from logical queue '{logical_queue}' "
                       f"(scheduling delay: {execution_time - task_start_time:.3f}s)")

            # Simulate long-running task (5-10 seconds of work)
            import random
            work_duration = random.uniform(5.0, 10.0)

            logger.info(f"⏳ Working on {item.get('item', 'unknown')} for {work_duration:.1f} seconds...")
            time.sleep(work_duration)

            logger.info(f"✅ COMPLETED {item.get('item', 'unknown')} after {work_duration:.1f}s of work")

        processing_end = time.time()
        processing_duration = processing_end - processing_start

        # Record task completion for performance monitoring
        try:
            # Import from main to get the global performance monitor
            from main import performance_monitor
            performance_monitor.record_task_completion(logical_queue, processing_duration)
            logger.debug(f"📊 Recorded completion for {logical_queue}: {processing_duration:.2f}s")
        except Exception as e:
            # Performance monitoring is optional, but log errors for debugging
            logger.debug(f"Performance monitoring error: {e}")
            pass

        # Simple return without complex monitoring
        return {
            'logical_queue': logical_queue,
            'processing_duration': processing_duration,
            'scheduling_accuracy': abs(execution_time - scheduled_time),
            'task_id': task_id
        }

    return universal_task_handler