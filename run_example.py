#!/usr/bin/env python3
"""
Universal Queue System - Practical Example

This demonstrates the key features of the universal queue system:
1. Dynamic queue creation without worker restarts
2. Independent rate limiting per logical queue
3. Single universal queue for all workers
4. Web app style usage patterns
"""

import sys
import time
import uuid
from loguru import logger
from main import (
    create_dynamic_queue,
    dispatch_to_queue,
    get_queue_stats,
    log_performance_report,
    get_performance_stats,
    check_underperformance,
    test_performance_monitoring,
    cleanup_old_queues
)

logger.remove()
logger.add(sys.stderr, format="{message}", level="DEBUG")

def demo_web_app_scenario():
    """Simulate a web application creating dynamic queues"""
    logger.info("🌐 Web App Scenario: Creating Dynamic Queues")
    logger.info("=" * 60)

    # Scenario 1: Tenant creates a batch of email tasks
    logger.info("📧 Scenario 1: Tenant Email Batch")

    tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
    email_queue = f"emails_{tenant_id}"

    # Create queue for this tenant (1 email every 3 seconds - very slow to watch)
    create_dynamic_queue(email_queue, rate_limit_per_second=0.33)

    # Batch of emails to send (fewer tasks since they take longer)
    email_tasks = [
        {"item": f"email_to_user_{i}", "tenant": tenant_id, "type": "welcome_email"}
        for i in range(4)
    ]

    dispatch_to_queue(email_queue, email_tasks)
    logger.info(f"✅ Created queue '{email_queue}' and dispatched {len(email_tasks)} emails")

    # Scenario 2: Different tenant with API calls (slower rate limit)
    logger.info("\n🔌 Scenario 2: API Integration Batch")

    api_tenant = f"tenant_{uuid.uuid4().hex[:8]}"
    api_queue = f"api_calls_{api_tenant}"

    # Create queue with very slow rate limit (1 call every 4 seconds - API limit)
    create_dynamic_queue(api_queue, rate_limit_per_second=0.25)

    api_tasks = [
        {"item": f"sync_data_{i}", "tenant": api_tenant, "endpoint": "/users/sync"}
        for i in range(3)
    ]

    dispatch_to_queue(api_queue, api_tasks)
    logger.info(f"✅ Created queue '{api_queue}' and dispatched {len(api_tasks)} API calls")

    # Scenario 3: High priority notifications (fast rate)
    logger.info("\n🚨 Scenario 3: High Priority Notifications")

    notification_queue = "urgent_notifications"

    # Create medium speed queue (1 notification every 2 seconds)
    create_dynamic_queue(notification_queue, rate_limit_per_second=0.5)

    urgent_tasks = [
        {"item": f"alert_{i}", "priority": "urgent", "type": "system_alert"}
        for i in range(3)
    ]

    dispatch_to_queue(notification_queue, urgent_tasks)
    logger.info(f"✅ Created queue '{notification_queue}' and dispatched {len(urgent_tasks)} alerts")

    return {
        "email_queue": email_queue,
        "api_queue": api_queue,
        "notification_queue": notification_queue
    }


def demo_different_rate_limits():
    """Show how different rate limits work independently"""
    logger.info("\n⚡ Rate Limit Demonstration")
    logger.info("=" * 60)

    # Create queues with very different rate limits (all slower for watching)
    rate_configs = [
        ("very_fast", 1.0, "High throughput tasks (1/sec)"),
        ("medium", 0.5, "Standard processing (1/2sec)"),
        ("slow", 0.2, "Rate-limited external service (1/5sec)"),
        ("very_slow", 0.1, "Heavy processing tasks (1/10sec)")
    ]

    for queue_name, rate, description in rate_configs:
        # Create queue
        create_dynamic_queue(queue_name, rate_limit_per_second=rate)

        # Create fewer tasks since they take much longer
        tasks = [
            {"item": f"{queue_name}_task_{i}", "description": description}
            for i in range(10)
        ]

        # Dispatch tasks
        dispatch_to_queue(queue_name, tasks)

        # Show queue stats
        stats = get_queue_stats(queue_name)
        status = "🟢 Ready" if stats['is_ready'] else f"🟡 {stats['delay_seconds']:.1f}s wait"

        logger.info(f"Queue '{queue_name}': {rate}/sec - {description} - {status}")

    return rate_configs


def demo_system_overview():
    """Show the system overview and worker information"""
    logger.info("\n📊 System Overview")
    logger.info("=" * 60)

    overview = get_queue_stats()

    logger.info(f"📈 Total logical queues: {overview['total_logical_queues']}")
    logger.info(f"🔧 Universal queue: {overview['universal_queue']}")

    logger.info("\n📋 Queue Details:")
    for queue_name, stats in overview['logical_queues'].items():
        rate = stats['rate_per_second']

        # Categorize by speed
        if rate >= 3:
            speed = "🚀 Fast"
        elif rate >= 1:
            speed = "🚗 Medium"
        else:
            speed = "🐌 Slow"

        status = "🟢 Ready" if stats['is_ready'] else f"🟡 {stats['delay_seconds']:.1f}s"

        logger.info(f"  {speed} | {queue_name}: {rate:.1f}/sec | {status}")


def demo_continuous_queue_creation():
    """Simulate continuous queue creation like a real web app"""
    logger.info("\n🔄 Continuous Queue Creation (Web App Style)")
    logger.info("=" * 60)

    created_queues = []

    for batch in range(5):
        # Generate batch ID (like a web request)
        batch_id = uuid.uuid4().hex[:8]
        queue_name = f"batch_{batch_id}"

        # Random slow rate limits (for watching)
        import random
        rate_limits = [0.1, 0.2, 0.33, 0.5, 1.0]  # Much slower rates
        rate = random.choice(rate_limits)

        logger.info(f"📦 Creating batch {batch + 1}/5: {queue_name} ({rate}/sec)")

        # Create queue
        create_dynamic_queue(queue_name, rate_limit_per_second=rate)

        # Create fewer tasks for this batch (since they take much longer)
        task_count = random.randint(2, 3)
        tasks = [
            {"item": f"batch_task_{i}", "batch_id": batch_id, "created": time.time()}
            for i in range(task_count)
        ]

        # Dispatch immediately (like a web app would)
        dispatch_to_queue(queue_name, tasks)

        created_queues.append((queue_name, rate, task_count))

        # Longer pause between batches to watch them being created
        time.sleep(2)

    logger.info(f"\n✅ Created {len(created_queues)} dynamic queues:")
    for queue_name, rate, task_count in created_queues:
        logger.info(f"  {queue_name}: {rate}/sec, {task_count} tasks")


def show_worker_info():
    """Show important worker information"""
    logger.info("\n🛠️  Worker Configuration")
    logger.info("=" * 60)

    logger.info("Workers listen to ONE queue only:")
    logger.info("  celery -A main worker --queues=universal_queue --loglevel=info")
    logger.info("")
    logger.info("✅ No worker restarts needed for new queues!")
    logger.info("✅ Scale workers by adding more instances")
    logger.info("✅ All logical queues route through universal_queue")
    logger.info("✅ Rate limiting is per logical queue")


def main():
    """Run the complete demonstration"""
    logger.info("🚀 Universal Queue System - Complete Example")
    logger.info("Make sure workers are running first:")
    logger.info("  celery -A main worker --queues=universal_queue --loglevel=info")
    logger.info("")

    try:
        # Clean up old queues first
        cleanup_old_queues()

        # Test performance monitoring first
        logger.info("\n🧪 Testing Performance Monitoring System")
        logger.info("=" * 60)
        test_performance_monitoring()

        # Web app scenarios
        # demo_web_app_scenario()

        # Rate limit demonstration
        logger.info("\n⚡ Rate Limit Demonstration")
        logger.info("=" * 60)
        demo_different_rate_limits()

        # System overview
        demo_system_overview()

        # Continuous creation (like real web app)
        # demo_continuous_queue_creation()

        # Wait much longer for slow tasks with long processing times
        logger.info("\n⏳ Waiting 30 seconds to let slow tasks with long processing times complete...")
        time.sleep(30)

        # Show updated system state
        demo_system_overview()

        # Show performance analysis
        logger.info("\n📊 Performance Analysis (Actual vs Expected Rates)")
        logger.info("=" * 60)
        log_performance_report(time_window_seconds=120)

        # Check for underperformance
        logger.info("\n🚨 Underperformance Check")
        logger.info("-" * 30)
        underperforming = check_underperformance(efficiency_threshold=0.8)

        if not underperforming:
            logger.info("✅ All queues performing within expected parameters")
        else:
            logger.info(f"⚠️  Found {len(underperforming)} underperforming queues")

        # Show worker info
        show_worker_info()

        logger.info("\n🎉 Example completed successfully!")
        logger.info("Check your worker logs to see the tasks being processed with rate limiting.")
        logger.info("Note: With single worker + long processing times, most queues will show as underperforming.")

    except Exception as e:
        logger.error(f"❌ Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
