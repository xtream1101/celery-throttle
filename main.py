import time
from redis import Redis
from celery import Celery
from loguru import logger
from universal_queue_router import UniversalQueueRouter, create_universal_task

# Create Celery app
celery_app = Celery('main', broker='redis://localhost:6379', backend='redis://localhost:6379')

# Configure Celery for universal queue routing
celery_app.conf.update(
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_prefetch_count=1,
)

# Redis connection
redis_conn = Redis.from_url("redis://localhost:6379")

# Initialize universal queue router for dynamic queues
universal_router = UniversalQueueRouter(celery_app, redis_conn)

# Create the universal task handler
universal_task = create_universal_task(celery_app, redis_conn)

# Initialize performance monitor
from queue_performance_monitor import QueuePerformanceMonitor
performance_monitor = QueuePerformanceMonitor(redis_conn, universal_router)

# Attach monitor to registry for task completion recording
universal_router.registry._performance_monitor = performance_monitor


# Basic task processing function (for demonstration)
def process_task_item(item_data):
    """Process a single task item - replace this with your actual task logic"""
    item_name = item_data.get('item', 'unknown')

    # Simulate some work
    time.sleep(0.1)

    logger.info(f"Processed task: {item_name}")
    return f"Completed: {item_name}"


def create_dynamic_queue(queue_name: str, rate_limit_per_second: float):
    """Create a new dynamic queue with specified rate limit"""
    refill_frequency = 1.0 / rate_limit_per_second
    universal_router.register_logical_queue(
        logical_queue_name=queue_name,
        refill_frequency=refill_frequency
    )
    logger.info(f"Created queue '{queue_name}' with rate limit: {rate_limit_per_second}/sec")


def dispatch_to_queue(queue_name: str, task_items: list):
    """Dispatch tasks to a specific logical queue"""
    tasks = []
    for item in task_items:
        tasks.append({
            'task_func': universal_task,
            'args': [item],
            'logical_queue': queue_name,
            'priority': 5
        })

    results = universal_router.batch_dispatch_to_logical_queues(tasks)
    logger.info(f"Dispatched {len(results)} tasks to queue '{queue_name}'")
    return results


def get_queue_stats(queue_name: str = None):
    """Get statistics for queues"""
    if queue_name:
        return universal_router.get_logical_queue_stats(queue_name)
    else:
        return universal_router.get_system_overview()


def get_performance_stats(queue_name: str = None, time_window_seconds: int = 300):
    """Get performance statistics showing actual vs expected rates"""
    if queue_name:
        return performance_monitor.get_queue_performance(queue_name, time_window_seconds)
    else:
        return performance_monitor.get_all_queue_performance(time_window_seconds)


def log_performance_report(time_window_seconds: int = 300):
    """Log detailed performance report with actual vs expected rates"""
    performance_monitor.log_performance_report(time_window_seconds)


def check_underperformance(efficiency_threshold: float = 0.7):
    """Check for and alert on underperforming queues"""
    return performance_monitor.alert_on_underperformance(efficiency_threshold)


def cleanup_old_queues():
    """Clean up old test queues and performance data"""
    logger.info("🧹 Cleaning up old queues and performance data...")

    # Clean up Redis keys for rate limit configurations
    pattern = "rate_limit:config:*"
    keys = redis_conn.keys(pattern)
    if keys:
        redis_conn.delete(*keys)
        logger.info(f"   Deleted {len(keys)} rate limit configuration keys")

    # Clean up Redis keys for rate limit states
    pattern = "rate_limit:state:*"
    keys = redis_conn.keys(pattern)
    if keys:
        redis_conn.delete(*keys)
        logger.info(f"   Deleted {len(keys)} rate limit state keys")

    # Clean up performance monitoring data
    pattern = "queue_performance:completions:*"
    keys = redis_conn.keys(pattern)
    if keys:
        redis_conn.delete(*keys)
        logger.info(f"   Deleted {len(keys)} performance monitoring keys")

    # Clean up any other rate limit related keys
    pattern = "limiter:*"
    keys = redis_conn.keys(pattern)
    if keys:
        redis_conn.delete(*keys)
        logger.info(f"   Deleted {len(keys)} limiter keys")

    logger.info("✅ Cleanup completed - starting with fresh state")


def test_performance_monitoring():
    """Test that performance monitoring is working"""
    logger.info("🧪 Testing performance monitoring...")

    # Create a test queue
    test_queue = "performance_test"
    create_dynamic_queue(test_queue, rate_limit_per_second=1.0)

    # Manually record a few completions
    import time
    current_time = time.time()

    performance_monitor.record_task_completion(test_queue, 2.5)
    time.sleep(0.1)
    performance_monitor.record_task_completion(test_queue, 3.2)
    time.sleep(0.1)
    performance_monitor.record_task_completion(test_queue, 1.8)

    logger.info(f"✅ Manually recorded 3 test completions for {test_queue}")

    # Check if we can retrieve stats
    stats = performance_monitor.get_queue_performance(test_queue, time_window_seconds=60)
    logger.info(f"📊 Retrieved stats: {stats.total_completed} completions, {stats.actual_rate_per_sec:.3f}/sec actual rate")

    return stats


# Simple test functions
def test_single_queue():
    """Test creating a single queue and dispatching tasks"""
    logger.info("=== Testing Single Queue ===")

    # Create a queue
    create_dynamic_queue("test_queue", rate_limit_per_second=2.0)

    # Create some test tasks
    test_tasks = [
        {"item": "task-1", "data": "test data 1"},
        {"item": "task-2", "data": "test data 2"},
        {"item": "task-3", "data": "test data 3"}
    ]

    # Dispatch tasks
    results = dispatch_to_queue("test_queue", test_tasks)

    # Show stats
    stats = get_queue_stats("test_queue")
    logger.info(f"Queue stats: {stats}")

    return results


def test_multiple_queues():
    """Test creating multiple queues with different rate limits"""
    logger.info("=== Testing Multiple Queues ===")

    # Create queues with different rate limits
    queues_config = [
        ("fast_queue", 5.0),   # 5 per second
        ("medium_queue", 1.0), # 1 per second
        ("slow_queue", 0.5)    # 1 per 2 seconds
    ]

    all_results = []

    for queue_name, rate_limit in queues_config:
        # Create queue
        create_dynamic_queue(queue_name, rate_limit)

        # Create tasks for this queue
        tasks = [
            {"item": f"{queue_name}-task-{i}", "queue_type": queue_name}
            for i in range(3)
        ]

        # Dispatch tasks
        results = dispatch_to_queue(queue_name, tasks)
        all_results.extend(results)

    # Show system overview
    overview = get_queue_stats()
    logger.info(f"System overview: {overview['total_logical_queues']} queues created")

    return all_results


if __name__ == "__main__":
    logger.info("🚀 Universal Queue System - Basic Example")
    logger.info("Workers should be started with: celery -A main_clean worker --queues=universal_queue --loglevel=info")
    logger.info("="*60)

    # Run simple tests
    test_single_queue()
    time.sleep(1)

    test_multiple_queues()

    logger.info("\n✅ Basic example completed!")
    logger.info("Check the worker logs to see task processing with rate limiting.")