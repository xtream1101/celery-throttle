#!/usr/bin/env python3
"""
Monitoring script to display real-time queue statistics.
Run this in a separate terminal to monitor your queues.
"""

import time
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
import redis
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.queue_manager import UniversalQueueManager


class QueueMonitor:
    """Real-time queue monitoring."""

    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
        self.queue_manager = UniversalQueueManager(self.redis)
        self.last_stats = {}  # Store previous stats for rate calculations

    def get_celery_queue_lengths(self) -> Dict[str, int]:
        """Get lengths of Celery broker queues."""
        celery_queues = {}
        try:
            # Default Celery queue
            default_queue_len = self.redis.llen('celery')
            celery_queues['default'] = default_queue_len

            # Check for any other Celery queues
            celery_keys = self.redis.keys('celery:*')
            for key in celery_keys:
                queue_name = key.decode() if isinstance(key, bytes) else key
                queue_len = self.redis.llen(queue_name)
                celery_queues[queue_name] = queue_len

        except Exception as e:
            print(f"Error getting Celery queue lengths: {e}")

        return celery_queues

    def detect_bottlenecks(self) -> List[str]:
        """Detect potential system bottlenecks."""
        issues = []
        current_time = time.time()

        # Check Celery broker queue lengths
        celery_queues = self.get_celery_queue_lengths()
        total_celery_tasks = sum(celery_queues.values())

        if total_celery_tasks > 50:
            issues.append(f"🚨 HIGH CELERY BACKLOG: {total_celery_tasks} tasks waiting for workers")
        elif total_celery_tasks > 10:
            issues.append(f"⚠️  Moderate Celery backlog: {total_celery_tasks} tasks")

        # Check for tasks that should be processable but are still waiting
        queues = self.queue_manager.get_active_queues()
        dispatcher_lag = 0

        for queue_config in queues:
            stats = self.queue_manager.get_queue_stats(queue_config.name)
            rate_status = self.queue_manager.get_rate_limit_status(queue_config.name)

            if stats and stats.tasks_waiting > 0 and rate_status:
                # If tokens are available but tasks are still waiting, dispatcher might be lagging
                if rate_status['available_tokens'] > 0:
                    dispatcher_lag += stats.tasks_waiting

        if dispatcher_lag > 20:
            issues.append(f"🚨 DISPATCHER BOTTLENECK: {dispatcher_lag} tasks could be dispatched now")
        elif dispatcher_lag > 5:
            issues.append(f"⚠️  Possible dispatcher lag: {dispatcher_lag} ready tasks waiting")

        # Check processing rates
        slow_queues = []
        for queue_config in queues:
            stats = self.queue_manager.get_queue_stats(queue_config.name)
            if stats and stats.tasks_processing == 0 and stats.tasks_waiting > 0:
                # Check if this queue has been stuck
                if queue_config.name in self.last_stats:
                    prev_waiting = self.last_stats[queue_config.name].get('tasks_waiting', 0)
                    if prev_waiting > 0 and stats.tasks_waiting >= prev_waiting:
                        slow_queues.append(queue_config.name)

        if slow_queues:
            issues.append(f"⚠️  Stalled queues (no progress): {len(slow_queues)} queues")

        return issues

    def display_bottleneck_analysis(self):
        """Display bottleneck analysis."""
        print("\nBOTTLENECK ANALYSIS")
        print("=" * 40)

        issues = self.detect_bottlenecks()
        celery_queues = self.get_celery_queue_lengths()

        if issues:
            for issue in issues:
                print(issue)
        else:
            print("✅ No bottlenecks detected")

        print(f"\nCelery Broker Status:")
        total_broker_tasks = sum(celery_queues.values())
        print(f"  Tasks in broker: {total_broker_tasks}")

        if total_broker_tasks > 0:
            print("  📝 Recommendations:")
            if total_broker_tasks > 10:
                print("    • Consider adding more Celery workers")
                print("    • Check worker logs for errors")
            print("    • Verify workers are running: python scripts/run_worker.py")

    def display_performance_metrics(self):
        """Display performance metrics and recommendations."""
        print("\nPERFORMANCE METRICS")
        print("=" * 40)

        queues = self.queue_manager.list_queues()
        active_queues = [q for q in queues if q.active]

        # Calculate total throughput potential vs actual
        total_potential_rate = 0
        total_actual_processing = 0

        for queue_config in active_queues:
            rate_limit = queue_config.rate_limit
            potential_rate = rate_limit.requests / rate_limit.period_seconds
            total_potential_rate += potential_rate

            stats = self.queue_manager.get_queue_stats(queue_config.name)
            if stats:
                total_actual_processing += stats.tasks_processing

        print(f"System Capacity: {total_potential_rate:.2f} tasks/sec theoretical max")
        print(f"Current Processing: {total_actual_processing} tasks active")
        print(f"Active Queues: {len(active_queues)}")

        # Resource recommendations
        high_rate_queues = [q for q in active_queues
                           if (q.rate_limit.requests / q.rate_limit.period_seconds) > 1.0]

        if high_rate_queues:
            print(f"\nHigh-rate queues ({len(high_rate_queues)}): Consider multiple dispatchers")

        if total_potential_rate > 10:
            print("📝 Recommendations for high-throughput:")
            print("  • Run 2-4 dispatcher processes")
            print("  • Scale Celery workers based on CPU cores")
            print("  • Monitor Redis memory usage")

    def display_queue_stats(self, clear_screen: bool = True):
        """Display current statistics for all queues."""
        if clear_screen:
            os.system('clear' if os.name == 'posix' else 'cls')

        print("=" * 80)
        print(f"QUEUE MONITORING - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        queues = self.queue_manager.list_queues()

        if not queues:
            print("No queues configured")
            return

        # Header
        print(f"{'Queue Name':<20} {'Rate Limit':<12} {'Status':<8} {'Waiting':<8} {'Processing':<10} {'Completed':<10} {'Failed':<8}")
        print("-" * 80)

        total_waiting = 0
        total_processing = 0
        total_completed = 0
        total_failed = 0

        for queue_config in queues:
            stats = self.queue_manager.get_queue_stats(queue_config.name)
            if not stats:
                continue

            status = "ACTIVE" if stats.active else "INACTIVE"

            print(f"{stats.name:<20} {str(stats.rate_limit):<12} {status:<8} "
                  f"{stats.tasks_waiting:<8} {stats.tasks_processing:<10} "
                  f"{stats.tasks_completed:<10} {stats.tasks_failed:<8}")

            total_waiting += stats.tasks_waiting
            total_processing += stats.tasks_processing
            total_completed += stats.tasks_completed
            total_failed += stats.tasks_failed

        print("-" * 80)
        print(f"{'TOTALS':<20} {'':<12} {'':<8} {total_waiting:<8} {total_processing:<10} {total_completed:<10} {total_failed:<8}")
        print()

    def display_rate_limit_status(self):
        """Display detailed rate limiting status."""
        print("RATE LIMIT STATUS")
        print("=" * 50)

        queues = self.queue_manager.get_active_queues()

        for queue_config in queues:
            status = self.queue_manager.get_rate_limit_status(queue_config.name)
            if not status:
                continue

            print(f"\nQueue: {queue_config.name}")
            print(f"  Rate Limit: {queue_config.rate_limit}")
            print(f"  Available Tokens: {status['available_tokens']:.2f}/{status['capacity']:.0f}")
            print(f"  Refill Rate: {status['refill_rate']:.4f} tokens/sec")

            if status['next_token_in'] > 0:
                print(f"  Next Token In: {status['next_token_in']:.2f} seconds")
            else:
                print(f"  Next Token: Available now")

    def display_system_overview(self):
        """Display system overview."""
        print("\nSYSTEM OVERVIEW")
        print("=" * 30)

        all_queues = self.queue_manager.list_queues()
        active_queues = self.queue_manager.get_active_queues()

        print(f"Total Queues: {len(all_queues)}")
        print(f"Active Queues: {len(active_queues)}")
        print(f"Inactive Queues: {len(all_queues) - len(active_queues)}")

        # Redis connection status
        try:
            self.redis.ping()
            print("Redis: Connected")
        except:
            print("Redis: DISCONNECTED")

    def run_continuous_monitor(self, interval: float = 2.0):
        """Run continuous monitoring display."""
        try:
            while True:
                # Store current stats for bottleneck detection
                current_stats = {}
                queues = self.queue_manager.list_queues()
                for queue in queues:
                    stats = self.queue_manager.get_queue_stats(queue.name)
                    if stats:
                        current_stats[queue.name] = {
                            'tasks_waiting': stats.tasks_waiting,
                            'tasks_processing': stats.tasks_processing,
                            'tasks_completed': stats.tasks_completed
                        }

                self.display_queue_stats()
                self.display_rate_limit_status()
                self.display_bottleneck_analysis()
                self.display_performance_metrics()
                self.display_system_overview()

                # Update last stats for next iteration
                self.last_stats = current_stats

                print(f"\nPress Ctrl+C to stop monitoring (updating every {interval}s)")
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nMonitoring stopped")

    def run_single_report(self):
        """Run a single monitoring report."""
        self.display_queue_stats(clear_screen=False)
        self.display_rate_limit_status()
        self.display_bottleneck_analysis()
        self.display_performance_metrics()
        self.display_system_overview()


if __name__ == "__main__":
    monitor = QueueMonitor()

    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        monitor.run_single_report()
    else:
        interval = 2.0
        if len(sys.argv) > 2:
            try:
                interval = float(sys.argv[2])
            except ValueError:
                print("Invalid interval, using default 2.0 seconds")

        monitor.run_continuous_monitor(interval)