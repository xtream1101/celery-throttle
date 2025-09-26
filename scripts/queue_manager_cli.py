#!/usr/bin/env python3
"""
CLI tool for managing queues dynamically.
"""

import sys
import os
import json
from datetime import datetime
import redis

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.queue_manager import UniversalQueueManager
from src.tasks import RateLimitedTaskSubmitter


class QueueManagerCLI:
    """Command-line interface for queue management."""

    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0, decode_responses=False)
        self.queue_manager = UniversalQueueManager(self.redis)
        self.task_submitter = RateLimitedTaskSubmitter()

    def create_queue(self, rate_limit_str: str) -> str:
        """Create a new queue with specified rate limit."""
        queue_name = self.queue_manager.create_queue(rate_limit_str)
        print(f"Created queue: {queue_name} with rate limit {rate_limit_str}")
        return queue_name

    def list_queues(self):
        """List all queues."""
        queues = self.queue_manager.list_queues()
        if not queues:
            print("No queues found")
            return

        print(f"{'Queue Name':<25} {'Rate Limit':<12} {'Status':<10} {'Created':<20}")
        print("-" * 70)

        for queue in queues:
            status = "ACTIVE" if queue.active else "INACTIVE"
            created = queue.created_at.strftime('%Y-%m-%d %H:%M:%S')
            print(f"{queue.name:<25} {str(queue.rate_limit):<12} {status:<10} {created:<20}")

    def remove_queue(self, queue_name: str):
        """Remove a queue."""
        if self.queue_manager.remove_queue(queue_name):
            print(f"Removed queue: {queue_name}")
        else:
            print(f"Queue {queue_name} not found")

    def update_rate_limit(self, queue_name: str, rate_limit_str: str):
        """Update rate limit for a queue."""
        if self.queue_manager.update_rate_limit(queue_name, rate_limit_str):
            print(f"Updated {queue_name} rate limit to {rate_limit_str}")
        else:
            print(f"Queue {queue_name} not found")

    def activate_queue(self, queue_name: str):
        """Activate a queue."""
        if self.queue_manager.activate_queue(queue_name):
            print(f"Activated queue: {queue_name}")
        else:
            print(f"Queue {queue_name} not found")

    def deactivate_queue(self, queue_name: str):
        """Deactivate a queue."""
        if self.queue_manager.deactivate_queue(queue_name):
            print(f"Deactivated queue: {queue_name}")
        else:
            print(f"Queue {queue_name} not found")

    def show_queue_details(self, queue_name: str):
        """Show detailed information about a queue."""
        config = self.queue_manager.get_queue_config(queue_name)
        if not config:
            print(f"Queue {queue_name} not found")
            return

        stats = self.queue_manager.get_queue_stats(queue_name)
        rate_status = self.queue_manager.get_rate_limit_status(queue_name)

        print(f"\nQueue Details: {queue_name}")
        print("=" * 50)
        print(f"Rate Limit: {config.rate_limit}")
        print(f"Status: {'ACTIVE' if config.active else 'INACTIVE'}")
        print(f"Created: {config.created_at.strftime('%Y-%m-%d %H:%M:%S')}")

        if stats:
            print(f"\nTask Statistics:")
            print(f"  Waiting: {stats.tasks_waiting}")
            print(f"  Processing: {stats.tasks_processing}")
            print(f"  Completed: {stats.tasks_completed}")
            print(f"  Failed: {stats.tasks_failed}")

        if rate_status:
            print(f"\nRate Limit Status:")
            print(f"  Available Tokens: {rate_status['available_tokens']:.2f}/{rate_status['capacity']:.0f}")
            print(f"  Refill Rate: {rate_status['refill_rate']:.4f} tokens/sec")
            if rate_status['next_token_in'] > 0:
                print(f"  Next Token In: {rate_status['next_token_in']:.2f} seconds")
            else:
                print(f"  Next Token: Available now")

    def submit_test_tasks(self, queue_name: str, count: int):
        """Submit test tasks to a queue."""
        if not self.queue_manager.queue_exists(queue_name):
            print(f"Queue {queue_name} not found")
            return

        print(f"Submitting {count} test tasks to {queue_name}...")

        submitted = 0
        queued = 0

        for i in range(count):
            task_data = {
                "task_id": i + 1,
                "message": f"Test task {i + 1}",
                "timestamp": datetime.now().isoformat()
            }

            if self.task_submitter.submit_task(queue_name, task_data):
                submitted += 1
            else:
                queued += 1

        print(f"Results: {submitted} submitted immediately, {queued} queued for later")

    def cleanup_all_queues(self):
        """Remove all queues and their data."""
        queues = self.queue_manager.list_queues()
        if not queues:
            print("No queues to clean up")
            return

        print(f"Found {len(queues)} queues to remove:")
        for queue in queues:
            print(f"  - {queue.name} ({queue.rate_limit})")

        try:
            response = input("\nAre you sure you want to remove ALL queues? (yes/no): ").lower().strip()
            if response != 'yes':
                print("Cleanup cancelled")
                return
        except EOFError:
            print("Cleanup cancelled (non-interactive mode)")
            return

        removed_count = 0
        for queue in queues:
            if self.queue_manager.remove_queue(queue.name):
                removed_count += 1
                print(f"  ✓ Removed {queue.name}")
            else:
                print(f"  ✗ Failed to remove {queue.name}")

        print(f"\nCleanup complete: {removed_count}/{len(queues)} queues removed")

    def cleanup_empty_queues(self):
        """Remove queues that have no tasks (waiting, processing, completed, or failed)."""
        queues = self.queue_manager.list_queues()
        if not queues:
            print("No queues found")
            return

        empty_queues = []
        for queue in queues:
            stats = self.queue_manager.get_queue_stats(queue.name)
            if stats:
                total_tasks = (stats.tasks_waiting + stats.tasks_processing +
                             stats.tasks_completed + stats.tasks_failed)
                if total_tasks == 0:
                    empty_queues.append(queue)

        if not empty_queues:
            print("No empty queues found")
            return

        print(f"Found {len(empty_queues)} empty queues:")
        for queue in empty_queues:
            print(f"  - {queue.name} ({queue.rate_limit}) - {'ACTIVE' if queue.active else 'INACTIVE'}")

        try:
            response = input(f"\nRemove these {len(empty_queues)} empty queues? (yes/no): ").lower().strip()
            if response != 'yes':
                print("Cleanup cancelled")
                return
        except EOFError:
            print("Cleanup cancelled (non-interactive mode)")
            return

        removed_count = 0
        for queue in empty_queues:
            if self.queue_manager.remove_queue(queue.name):
                removed_count += 1
                print(f"  ✓ Removed {queue.name}")
            else:
                print(f"  ✗ Failed to remove {queue.name}")

        print(f"\nCleanup complete: {removed_count}/{len(empty_queues)} empty queues removed")

    def print_help(self):
        """Print help information."""
        print("""
Queue Manager CLI

Usage: python queue_manager_cli.py [command] [arguments]

Commands:
  create <rate_limit>              Create new queue (e.g., "10/60s")
  list                            List all queues
  remove <queue_name>             Remove a queue
  update <queue_name> <rate_limit> Update queue rate limit
  activate <queue_name>           Activate a queue
  deactivate <queue_name>         Deactivate a queue
  show <queue_name>               Show queue details
  test <queue_name> <count>       Submit test tasks
  cleanup-all                     Remove ALL queues (with confirmation)
  cleanup-empty                   Remove queues with no tasks
  help                           Show this help

Examples:
  python queue_manager_cli.py create "5/60s"
  python queue_manager_cli.py list
  python queue_manager_cli.py show batch_12345678-1234-1234-1234-123456789012
  python queue_manager_cli.py test batch_12345678-1234-1234-1234-123456789012 10
  python queue_manager_cli.py cleanup-empty
  python queue_manager_cli.py cleanup-all
        """)


def main():
    cli = QueueManagerCLI()

    if len(sys.argv) < 2:
        cli.print_help()
        return

    command = sys.argv[1].lower()

    try:
        if command == "create":
            if len(sys.argv) != 3:
                print("Usage: create <rate_limit>")
                return
            cli.create_queue(sys.argv[2])

        elif command == "list":
            cli.list_queues()

        elif command == "remove":
            if len(sys.argv) != 3:
                print("Usage: remove <queue_name>")
                return
            cli.remove_queue(sys.argv[2])

        elif command == "update":
            if len(sys.argv) != 4:
                print("Usage: update <queue_name> <rate_limit>")
                return
            cli.update_rate_limit(sys.argv[2], sys.argv[3])

        elif command == "activate":
            if len(sys.argv) != 3:
                print("Usage: activate <queue_name>")
                return
            cli.activate_queue(sys.argv[2])

        elif command == "deactivate":
            if len(sys.argv) != 3:
                print("Usage: deactivate <queue_name>")
                return
            cli.deactivate_queue(sys.argv[2])

        elif command == "show":
            if len(sys.argv) != 3:
                print("Usage: show <queue_name>")
                return
            cli.show_queue_details(sys.argv[2])

        elif command == "test":
            if len(sys.argv) != 4:
                print("Usage: test <queue_name> <count>")
                return
            cli.submit_test_tasks(sys.argv[2], int(sys.argv[3]))

        elif command == "cleanup-all":
            cli.cleanup_all_queues()

        elif command == "cleanup-empty":
            cli.cleanup_empty_queues()

        elif command == "help":
            cli.print_help()

        else:
            print(f"Unknown command: {command}")
            cli.print_help()

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()