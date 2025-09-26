#!/usr/bin/env python3
"""
Monitoring script to display real-time queue statistics.
Run this in a separate terminal to monitor your queues.
"""

import time
import json
from datetime import datetime
from typing import Dict, Any
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
                self.display_queue_stats()
                self.display_rate_limit_status()
                self.display_system_overview()

                print(f"\nPress Ctrl+C to stop monitoring (updating every {interval}s)")
                time.sleep(interval)

        except KeyboardInterrupt:
            print("\nMonitoring stopped")

    def run_single_report(self):
        """Run a single monitoring report."""
        self.display_queue_stats(clear_screen=False)
        self.display_rate_limit_status()
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