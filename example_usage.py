#!/usr/bin/env python3
"""
Example usage and testing scenarios for the rate-limited queue system.
Run this to see the system in action with different queue configurations.
"""

import time
import json
from queue_manager import RateLimitedQueueManager

def create_sample_queues():
    """Create several test queues with different characteristics."""
    manager = RateLimitedQueueManager()

    print("Creating sample queues for testing...\n")

    # Queue 1: Fast processing (60 requests/min)
    fast_tasks = [{"url": f"https://api.example.com/fast/{i}", "type": "fast"} for i in range(50)]
    fast_queue = manager.create_queue("fast_api_calls", 60.0, fast_tasks)
    print(f"✓ Created 'fast_api_calls': {len(fast_tasks)} tasks at 60 req/min")

    # Queue 2: Slow processing (6 requests/min)
    slow_tasks = [{"url": f"https://api.slow.com/endpoint/{i}", "type": "slow", "timeout": 30} for i in range(20)]
    slow_queue = manager.create_queue("slow_api_calls", 6.0, slow_tasks)
    print(f"✓ Created 'slow_api_calls': {len(slow_tasks)} tasks at 6 req/min")

    # Queue 3: Medium processing (30 requests/min)
    medium_tasks = [{"endpoint": f"/users/{i}", "method": "GET", "retry": True} for i in range(100)]
    medium_queue = manager.create_queue("user_data_fetch", 30.0, medium_tasks)
    print(f"✓ Created 'user_data_fetch': {len(medium_tasks)} tasks at 30 req/min")

    # Queue 4: Very strict rate limit (1 request/min)
    strict_tasks = [{"critical_api": f"endpoint_{i}", "priority": "high"} for i in range(5)]
    strict_queue = manager.create_queue("critical_api", 1.0, strict_tasks)
    print(f"✓ Created 'critical_api': {len(strict_tasks)} tasks at 1 req/min")

    print(f"\n📊 Summary:")
    print(f"Total queues created: 4")
    print(f"Total tasks queued: {len(fast_tasks) + len(slow_tasks) + len(medium_tasks) + len(strict_tasks)}")

    return {
        'fast_api_calls': fast_queue,
        'slow_api_calls': slow_queue,
        'user_data_fetch': medium_queue,
        'critical_api': strict_queue
    }

def monitor_queue_progress(queue_names, duration_seconds=60):
    """Monitor queue progress for a specified duration."""
    manager = RateLimitedQueueManager()

    print(f"\n🔍 Monitoring queues for {duration_seconds} seconds...")
    print("=" * 80)

    start_time = time.time()
    while time.time() - start_time < duration_seconds:
        print(f"\n⏰ Time: {int(time.time() - start_time)}s / {duration_seconds}s")

        for queue_name in queue_names:
            status = manager.get_queue_status(queue_name)
            health = status['health']

            progress = status['progress_percentage']
            progress_bar = "█" * int(progress / 5) + "░" * (20 - int(progress / 5))

            print(f"  {queue_name}:")
            print(f"    Progress: {progress:.1f}% [{progress_bar}]")
            print(f"    Tasks: {health['active_tasks']} active, {health['waiting_tasks']} waiting")
            print(f"    Rate: {health['current_rate']:.1f}/{health['target_rate']:.1f} req/min")
            print(f"    Health: {health['health_score']:.0f}%")

        time.sleep(10)

    print("\nMonitoring complete!")

def clear_redis_data():
    """Clear all Redis queue and rate limiting data."""
    print("\n🧹 Clearing all Redis data...")

    manager = RateLimitedQueueManager()
    r = manager.redis

    # Get counts before clearing
    queue_keys = len(r.keys('queue:*'))
    task_keys = len(r.keys('task:*'))
    bucket_keys = len(r.keys('rate_limit:*'))
    celery_keys = len(r.keys('celery-*'))

    print(f'Found:')
    print(f'  - {queue_keys} queue keys')
    print(f'  - {task_keys} task keys')
    print(f'  - {bucket_keys} rate limit keys')
    print(f'  - {celery_keys} celery keys')

    # Clear all our application data
    patterns = ['queue:*', 'task:*', 'rate_limit:*', 'celery-*', '_kombu*']
    total_deleted = 0

    for pattern in patterns:
        keys = r.keys(pattern)
        if keys:
            deleted = r.delete(*keys)
            total_deleted += deleted
            print(f'  ✓ Deleted {deleted} keys matching {pattern}')

    print(f'🗑️  Total keys deleted: {total_deleted}')
    print('✅ Redis cleanup complete!')

def test_rate_limiting_accuracy():
    """Test how accurate the rate limiting is."""
    # Clear Redis data first
    clear_redis_data()

    manager = RateLimitedQueueManager()

    print("\n🎯 Testing rate limiting accuracy...")

    # Create a queue with precise timing requirements
    test_tasks = [{"test_id": i, "timestamp": time.time()} for i in range(30)]
    test_queue = manager.create_queue("accuracy_test", 12.0, test_tasks)  # 12 per minute = 1 every 5 seconds

    print(f"Created test queue with {len(test_tasks)} tasks at 12 req/min (1 every 5 seconds)")
    print("Expected timing: 1 task every 5 seconds")

    # Monitor for 2 minutes
    start_time = time.time()
    last_completed = 0

    for i in range(24):  # 24 * 5 seconds = 2 minutes
        time.sleep(5)
        status = manager.get_queue_status("accuracy_test")
        health = status['health']

        completed_now = health['completed_tasks']
        new_completions = completed_now - last_completed
        last_completed = completed_now

        elapsed = time.time() - start_time
        expected_completions = int(elapsed / 5)
        accuracy = (completed_now / max(1, expected_completions)) * 100

        print(f"  {elapsed:3.0f}s: {completed_now:2d} completed (expected ~{expected_completions:2d}) - "
              f"{new_completions} new - {accuracy:.1f}% accuracy")

        if completed_now >= len(test_tasks):
            print("  All tasks completed!")
            break

def demonstrate_dynamic_queues():
    """Show how queues can be created and modified dynamically."""
    manager = RateLimitedQueueManager()

    print("\n🔄 Demonstrating dynamic queue management...")

    # Create initial queue
    tasks1 = [{"batch": 1, "id": i} for i in range(10)]
    queue1 = manager.create_queue("dynamic_demo", 30.0, tasks1)
    print(f"✓ Created initial queue with {len(tasks1)} tasks at 30 req/min")

    time.sleep(5)

    # Update rate limit
    manager.update_queue_rate_limit("dynamic_demo", 60.0)
    print("✓ Updated rate limit to 60 req/min")

    time.sleep(5)

    # Pause queue
    manager.pause_queue("dynamic_demo")
    print("✓ Paused queue (rate limit set to 0)")

    time.sleep(5)

    # Resume queue
    manager.resume_queue("dynamic_demo")
    print("✓ Resumed queue")

    # Show final status
    status = manager.get_queue_status("dynamic_demo")
    print(f"Final status: {status['health']['completed_tasks']} completed, "
          f"{status['health']['waiting_tasks']} waiting")

def export_test_results():
    """Export current system state for analysis."""
    manager = RateLimitedQueueManager()

    timestamp = int(time.time())
    filename = f"queue_metrics_{timestamp}.json"

    # Get comprehensive system overview
    overview = manager.get_system_overview()
    queue_details = []

    for queue_status in manager.get_all_queue_statuses():
        queue_name = queue_status['queue_name']
        task_details = manager.get_queue_task_details(queue_name)

        queue_details.append({
            'status': queue_status,
            'tasks': task_details
        })

    export_data = {
        'export_timestamp': timestamp,
        'export_time': time.ctime(timestamp),
        'system_overview': overview,
        'queue_details': queue_details
    }

    with open(filename, 'w') as f:
        json.dump(export_data, f, indent=2, default=str)

    print(f"📁 Exported detailed metrics to {filename}")
    print(f"   File size: {len(json.dumps(export_data)) / 1024:.1f} KB")

def cleanup_test_data():
    """Clean up test queues and data."""
    manager = RateLimitedQueueManager()

    print("\n🧹 Cleaning up test data...")

    # Get all queues
    all_statuses = manager.get_all_queue_statuses()

    for status in all_statuses:
        queue_name = status['queue_name']
        manager.delete_queue(queue_name)
        print(f"  ✓ Deleted queue: {queue_name}")

    print("Cleanup complete!")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Rate-limited queue system examples and tests')
    parser.add_argument('--create-samples', action='store_true',
                       help='Create sample queues for testing')
    parser.add_argument('--monitor', type=int, metavar='SECONDS',
                       help='Monitor queue progress for specified seconds')
    parser.add_argument('--test-accuracy', action='store_true',
                       help='Test rate limiting accuracy')
    parser.add_argument('--demo-dynamic', action='store_true',
                       help='Demonstrate dynamic queue management')
    parser.add_argument('--export', action='store_true',
                       help='Export current metrics to JSON')
    parser.add_argument('--cleanup', action='store_true',
                       help='Clean up all test queues')
    parser.add_argument('--clear-redis', action='store_true',
                       help='Clear all Redis queue and rate limiting data')
    parser.add_argument('--full-demo', action='store_true',
                       help='Run complete demonstration sequence')

    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    created_queues = None

    if args.create_samples or args.full_demo:
        created_queues = create_sample_queues()

    if args.monitor or args.full_demo:
        duration = args.monitor if args.monitor else 30
        if created_queues:
            monitor_queue_progress(list(created_queues.keys()), duration)
        else:
            # Monitor any existing queues
            manager = RateLimitedQueueManager()
            all_queues = [s['queue_name'] for s in manager.get_all_queue_statuses()]
            if all_queues:
                monitor_queue_progress(all_queues, duration)
            else:
                print("No queues found to monitor. Create some queues first.")

    if args.test_accuracy:
        test_rate_limiting_accuracy()

    if args.demo_dynamic:
        demonstrate_dynamic_queues()

    if args.export or args.full_demo:
        export_test_results()

    if args.clear_redis:
        clear_redis_data()

    if args.cleanup:
        cleanup_test_data()

    if args.full_demo:
        print("\n🎉 Full demonstration complete!")
        print("Check the exported JSON file for detailed metrics.")

if __name__ == '__main__':
    main()