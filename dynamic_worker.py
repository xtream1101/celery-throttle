#!/usr/bin/env python3
"""
Advanced dynamic worker that can add/remove queues without restart.
Uses Celery's control API to manage queue consumption dynamically.
"""

import time
import threading
import signal
import sys
import subprocess
import os
from celery_app import app, update_worker_queues
from queue_manager import RateLimitedQueueManager

class AdvancedDynamicWorker:
    """Advanced worker that can dynamically add/remove queues without restart."""

    def __init__(self, concurrency: int = 1, loglevel: str = 'info'):
        self.concurrency = concurrency
        self.loglevel = loglevel
        self.manager = RateLimitedQueueManager()
        self.running = False
        self.queue_discovery_thread = None
        self.worker_process = None
        self.current_queues = set(['default'])

    def start(self):
        """Start the advanced dynamic worker."""
        self.running = True

        print(f"Starting advanced dynamic rate-limited worker...")
        print(f"Concurrency: {self.concurrency}")
        print(f"This worker will automatically consume from new queues without restart!")
        print("Press Ctrl+C to stop\n")

        # Start queue discovery in background
        self.queue_discovery_thread = threading.Thread(
            target=self._queue_discovery_loop,
            daemon=True
        )
        self.queue_discovery_thread.start()

        # Start Celery worker process
        self._start_worker_process()

    def _start_worker_process(self):
        """Start the Celery worker as a subprocess."""
        # Build initial queue list
        queue_list = list(self.current_queues)
        queue_args = []
        for queue in queue_list:
            queue_args.extend(['-Q', queue])

        cmd = [
            'celery', '-A', 'celery_app', 'worker',
            f'--loglevel={self.loglevel}',
            f'--concurrency={self.concurrency}',
            '--prefetch-multiplier=1',
            '--without-gossip',
            '--without-mingle',
            '--without-heartbeat',
            '--task-events',
            '--pool=threads',
        ] + queue_args

        print(f"Starting worker with initial queues: {', '.join(queue_list)}")

        try:
            self.worker_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )

            # Monitor worker output
            self._monitor_worker_output()

        except Exception as e:
            print(f"Error starting worker: {e}")

    def _monitor_worker_output(self):
        """Monitor and display worker output."""
        try:
            while self.running and self.worker_process:
                output = self.worker_process.stdout.readline()
                if output:
                    print(f"[WORKER] {output.strip()}")
                elif self.worker_process.poll() is not None:
                    break
        except Exception as e:
            print(f"Error monitoring worker: {e}")

    def _queue_discovery_loop(self):
        """Discover new queues and dynamically add them to the worker."""
        while self.running:
            try:
                discovered_queues = set(update_worker_queues())

                if discovered_queues != self.current_queues:
                    new_queues = discovered_queues - self.current_queues
                    removed_queues = self.current_queues - discovered_queues

                    if new_queues:
                        print(f"📥 Adding new queues: {', '.join(sorted(new_queues))}")
                        for queue in new_queues:
                            self._add_queue_to_worker(queue)

                    if removed_queues:
                        print(f"📤 Removing inactive queues: {', '.join(sorted(removed_queues))}")
                        for queue in removed_queues:
                            self._remove_queue_from_worker(queue)

                    self.current_queues = discovered_queues
                    print(f"🔄 Active queues: {', '.join(sorted(self.current_queues))}")

                time.sleep(5)

            except Exception as e:
                print(f"Error in queue discovery: {e}")
                time.sleep(10)

    def _add_queue_to_worker(self, queue_name: str):
        """Add a new queue to the running worker."""
        try:
            # Use Celery's control API to add queue consumption
            from celery import current_app

            # Get worker nodes
            inspect = current_app.control.inspect()
            workers = inspect.active_queues()

            if workers:
                worker_name = list(workers.keys())[0]  # Get first worker

                # Add queue consumption
                current_app.control.add_consumer(
                    queue_name,
                    destination=[worker_name]
                )
                print(f"✅ Successfully added queue '{queue_name}' to worker")
            else:
                print(f"⚠️  No active workers found to add queue '{queue_name}'")

        except Exception as e:
            print(f"❌ Failed to add queue '{queue_name}': {e}")

    def _remove_queue_from_worker(self, queue_name: str):
        """Remove a queue from the running worker."""
        try:
            # Use Celery's control API to remove queue consumption
            from celery import current_app

            # Get worker nodes
            inspect = current_app.control.inspect()
            workers = inspect.active_queues()

            if workers:
                worker_name = list(workers.keys())[0]  # Get first worker

                # Remove queue consumption
                current_app.control.cancel_consumer(
                    queue_name,
                    destination=[worker_name]
                )
                print(f"✅ Successfully removed queue '{queue_name}' from worker")
            else:
                print(f"⚠️  No active workers found to remove queue '{queue_name}'")

        except Exception as e:
            print(f"❌ Failed to remove queue '{queue_name}': {e}")

    def get_worker_status(self):
        """Get current worker status."""
        try:
            inspect = app.control.inspect()

            # Get active queues
            active_queues = inspect.active_queues()

            # Get stats
            stats = inspect.stats()

            return {
                'active_queues': active_queues,
                'stats': stats,
                'discovered_queues': list(self.current_queues),
                'worker_alive': self.worker_process.poll() is None if self.worker_process else False
            }

        except Exception as e:
            print(f"Error getting worker status: {e}")
            return {}

    def stop(self):
        """Stop the worker gracefully."""
        print("\nStopping advanced dynamic worker...")
        self.running = False

        if self.worker_process:
            self.worker_process.terminate()
            try:
                self.worker_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.worker_process.kill()

        print("Worker stopped.")

def signal_handler(signum, _frame):
    """Handle shutdown signals."""
    print(f"\nReceived signal {signum}, shutting down...")
    sys.exit(0)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Advanced dynamic rate-limited worker')
    parser.add_argument('--concurrency', '-c', type=int, default=1,
                       help='Number of concurrent worker processes (default: 1)')
    parser.add_argument('--loglevel', '-l', default='info',
                       choices=['debug', 'info', 'warning', 'error'],
                       help='Log level (default: info)')
    parser.add_argument('--test-redis', action='store_true',
                       help='Test Redis connection before starting')
    parser.add_argument('--status', action='store_true',
                       help='Show current worker status and exit')

    args = parser.parse_args()

    # Test Redis connection if requested
    if args.test_redis:
        try:
            manager = RateLimitedQueueManager()
            manager.redis.ping()
            print("✓ Redis connection successful")
        except Exception as e:
            print(f"✗ Redis connection failed: {e}")
            print("Make sure Redis is running on localhost:6379")
            sys.exit(1)

    # Show status if requested
    if args.status:
        try:
            worker = AdvancedDynamicWorker()
            status = worker.get_worker_status()
            print("Worker Status:")
            print(f"  Discovered queues: {status.get('discovered_queues', [])}")
            print(f"  Worker alive: {status.get('worker_alive', False)}")
            if status.get('active_queues'):
                print(f"  Active queues: {list(status['active_queues'].keys())}")
        except Exception as e:
            print(f"Error getting status: {e}")
        return

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start worker
    worker = AdvancedDynamicWorker(
        concurrency=args.concurrency,
        loglevel=args.loglevel
    )

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()
    except Exception as e:
        print(f"Worker error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()