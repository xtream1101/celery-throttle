#!/usr/bin/env python3
"""
Real-time monitoring dashboard for rate-limited queues.
Run this to get live updates on queue performance and health.
"""

import time
import json
import argparse
from datetime import datetime
from queue_manager import RateLimitedQueueManager

class QueueMonitor:
    """Real-time monitoring and display for queue system."""

    def __init__(self, refresh_interval: int = 5):
        self.manager = RateLimitedQueueManager()
        self.refresh_interval = refresh_interval

    def print_system_overview(self):
        """Print system-wide overview."""
        try:
            overview = self.manager.get_system_overview()
        except Exception as e:
            print(f"❌ Error getting system overview: {e}")
            return

        print("\n" + "="*80)
        print(f"RATE-LIMITED QUEUE SYSTEM OVERVIEW - {datetime.now().strftime('%H:%M:%S')}")
        print("="*80)

        print(f"Total Queues: {overview['total_queues']}")
        print(f"Healthy/Unhealthy/Stuck: {overview['queue_health']['healthy_queues']}/"
              f"{overview['queue_health']['unhealthy_queues']}/{overview['queue_health']['stuck_queues']}")

        health = overview['system_health']
        print(f"\nSystem Totals:")
        print(f"  Tasks: {health['total_tasks']} total, {health['completed_tasks']} completed, "
              f"{health['failed_tasks']} failed")
        print(f"  Active: {health['active_tasks']}, Waiting: {health['waiting_tasks']}")
        print(f"  Success Rate: {health['success_rate']:.1f}%, "
              f"Progress: {health['overall_progress']:.1f}%")

        if overview['problem_queues']:
            print(f"\n⚠️  Problem Queues:")
            for pq in overview['problem_queues']:
                print(f"  - {pq['name']}: Health {pq['health_score']:.0f}% - {pq['issue']}")

    def print_queue_details(self, queue_names=None):
        """Print detailed view of specific queues or all queues."""
        statuses = self.manager.get_all_queue_statuses()

        if queue_names:
            statuses = [s for s in statuses if s['queue_name'] in queue_names]

        for status in statuses:
            self._print_single_queue(status)

    def _print_single_queue(self, status):
        """Print detailed status for a single queue."""
        try:
            name = status['queue_name']
            health = status['health']
            rate_limiter = status.get('rate_limiter', {})
        except Exception as e:
            print(f"❌ Error printing queue status: {e}")
            return

        print(f"\n📊 Queue: {name}")
        print("-" * 60)

        # Basic stats
        print(f"Tasks: {health['total_tasks']} total | "
              f"{health['completed_tasks']} ✓ | {health['failed_tasks']} ✗ | "
              f"{health['active_tasks']} 🔄 | {health['waiting_tasks']} ⏳")

        # Progress
        progress = status['progress_percentage']
        progress_bar = self._create_progress_bar(progress)
        print(f"Progress: {progress:.1f}% {progress_bar}")

        # Rate limiting
        print(f"Rate: {health['current_rate']:.1f}/{health['target_rate']:.1f} req/min "
              f"({health['rate_accuracy']:.1f}% accurate)")

        if rate_limiter:
            tokens = rate_limiter.get('current_tokens', 0)
            capacity = rate_limiter.get('bucket_capacity', 1)
            utilization = rate_limiter.get('utilization_percent', 0)
            print(f"Token Bucket: {tokens:.2f}/{capacity:.1f} tokens ({utilization:.1f}% utilized)")

        # Health and timing
        health_emoji = "🟢" if health['health_score'] > 80 else "🟡" if health['health_score'] > 60 else "🔴"
        print(f"Health: {health_emoji} {health['health_score']:.0f}%")

        if health['avg_processing_time'] > 0:
            print(f"Avg Processing Time: {health['avg_processing_time']:.2f}s")

        if status['estimated_time_remaining']:
            remaining_mins = status['estimated_time_remaining'] / 60
            print(f"Est. Remaining: {remaining_mins:.1f} minutes")

        if status['is_stuck']:
            print("⚠️  Queue appears stuck!")

        # Recommendations
        if status['recommendations'] and status['recommendations'][0] != "Queue is healthy":
            print(f"💡 {', '.join(status['recommendations'])}")

    def _create_progress_bar(self, percentage, width=20):
        """Create a text-based progress bar."""
        filled = int(width * percentage / 100)
        bar = "█" * filled + "░" * (width - filled)
        return f"[{bar}]"

    def monitor_continuous(self, queue_names=None, show_details=True):
        """Run continuous monitoring with regular updates."""
        print("Starting continuous monitoring... Press Ctrl+C to stop")
        print(f"Refresh interval: {self.refresh_interval} seconds")

        try:
            while True:
                # Clear screen (works on most terminals)
                print("\033[2J\033[H")

                self.print_system_overview()

                if show_details:
                    print("\n" + "="*80)
                    print("QUEUE DETAILS")
                    print("="*80)
                    self.print_queue_details(queue_names)

                print(f"\nNext update in {self.refresh_interval} seconds... (Ctrl+C to stop)")
                time.sleep(self.refresh_interval)

        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")

    def export_metrics(self, output_file: str):
        """Export current metrics to JSON file."""
        overview = self.manager.get_system_overview()
        queue_statuses = self.manager.get_all_queue_statuses()

        export_data = {
            'timestamp': datetime.now().isoformat(),
            'system_overview': overview,
            'queue_details': queue_statuses
        }

        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)

        print(f"Metrics exported to {output_file}")

    def check_queue_health(self, queue_name: str):
        """Deep health check for a specific queue."""
        status = self.manager.get_queue_status(queue_name)
        task_details = self.manager.get_queue_task_details(queue_name, limit=50)

        print(f"\n🔍 Deep Health Check: {queue_name}")
        print("="*60)

        self._print_single_queue(status)

        print(f"\n📋 Recent Tasks (last 50):")
        print("-" * 40)

        status_counts = task_details['task_breakdown']
        for status_type, count in status_counts.items():
            emoji = {"waiting": "⏳", "active": "🔄", "success": "✓", "failed": "✗"}
            print(f"{emoji.get(status_type, '?')} {status_type.title()}: {count}")

        # Show some sample tasks
        recent_tasks = sorted(task_details['tasks'],
                            key=lambda x: x.get('start_time', 0), reverse=True)[:10]

        if recent_tasks:
            print(f"\n📝 Most Recent Tasks:")
            for task in recent_tasks:
                status_emoji = {"success": "✓", "failed": "✗", "processing": "🔄", "pending": "⏳"}
                emoji = status_emoji.get(task['status'], '?')

                if task.get('processing_time'):
                    timing = f" ({task['processing_time']:.2f}s)"
                else:
                    timing = ""

                print(f"  {emoji} {task['task_id'][:12]}... - {task['status']}{timing}")

                if task.get('error_message'):
                    print(f"      Error: {task['error_message']}")

def main():
    parser = argparse.ArgumentParser(description='Monitor rate-limited queues')
    parser.add_argument('--continuous', '-c', action='store_true',
                       help='Run continuous monitoring')
    parser.add_argument('--interval', '-i', type=int, default=5,
                       help='Refresh interval for continuous monitoring (seconds)')
    parser.add_argument('--queues', '-q', nargs='+',
                       help='Specific queue names to monitor')
    parser.add_argument('--export', '-e', type=str,
                       help='Export metrics to JSON file')
    parser.add_argument('--health-check', type=str,
                       help='Run deep health check on specific queue')
    parser.add_argument('--overview-only', action='store_true',
                       help='Show only system overview (no queue details)')

    args = parser.parse_args()

    monitor = QueueMonitor(refresh_interval=args.interval)

    if args.health_check:
        monitor.check_queue_health(args.health_check)
    elif args.export:
        monitor.export_metrics(args.export)
    elif args.continuous:
        monitor.monitor_continuous(
            queue_names=args.queues,
            show_details=not args.overview_only
        )
    else:
        # One-time snapshot
        monitor.print_system_overview()
        if not args.overview_only:
            monitor.print_queue_details(args.queues)

if __name__ == '__main__':
    main()