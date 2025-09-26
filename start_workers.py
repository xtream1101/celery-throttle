#!/usr/bin/env python3
"""
Dynamic worker startup script that automatically discovers and listens to all queues.
Workers will automatically pick up new queues without needing to be restarted.
"""

import time
import threading
import signal
import sys
from celery_app import app, update_worker_queues
from queue_manager import RateLimitedQueueManager

class DynamicWorkerManager:
    """Manages Celery workers with dynamic queue discovery."""

    def __init__(self, concurrency: int = 1, loglevel: str = 'info'):
        self.concurrency = concurrency
        self.loglevel = loglevel
        self.manager = RateLimitedQueueManager()
        self.running = False
        self.worker_thread = None
        self.queue_discovery_thread = None

    def start(self):
        """Start the worker with dynamic queue discovery."""
        self.running = True

        print(f"Starting dynamic rate-limited workers...")
        print(f"Concurrency: {self.concurrency}")
        print(f"Prefetch multiplier: 1 (strict rate limiting)")
        print("Workers will automatically discover new queues")
        print("Press Ctrl+C to stop\n")

        # Start queue discovery in background
        self.queue_discovery_thread = threading.Thread(
            target=self._queue_discovery_loop,
            daemon=True
        )
        self.queue_discovery_thread.start()

        # Configure and start Celery worker
        self._start_celery_worker()

    def _queue_discovery_loop(self):
        """Continuously discover new queues and notify worker to restart if needed."""
        last_queues = set()

        while self.running:
            try:
                current_queues = set(update_worker_queues())

                if current_queues != last_queues:
                    new_queues = current_queues - last_queues
                    removed_queues = last_queues - current_queues

                    if new_queues:
                        print(f"📥 Discovered new queues: {', '.join(sorted(new_queues))}")
                        print("⚠️  NOTE: For new queues to be processed, restart the worker:")
                        print("   1. Stop worker with Ctrl+C")
                        print("   2. Restart with: uv run python start_workers.py")

                    if removed_queues:
                        print(f"📤 Queues no longer active: {', '.join(sorted(removed_queues))}")

                    print(f"🔄 Total queues discovered: {len(current_queues)} - {', '.join(sorted(current_queues))}")
                    last_queues = current_queues

                time.sleep(5)  # Check for new queues every 5 seconds

            except Exception as e:
                print(f"Error in queue discovery: {e}")
                time.sleep(10)

    def _start_celery_worker(self):
        """Start the Celery worker with optimal settings."""
        # Initial queue setup
        update_worker_queues()

        # Start worker
        app.worker_main([
            'worker',
            f'--loglevel={self.loglevel}',
            f'--concurrency={self.concurrency}',
            '--prefetch-multiplier=1',  # Critical for rate limiting accuracy
            '--without-gossip',         # Reduce overhead
            '--without-mingle',         # Reduce startup time
            '--without-heartbeat',      # We handle health via Redis
            '--task-events',           # Enable monitoring
            '--pool=threads',          # Thread pool for better rate limiting
        ])

    def stop(self):
        """Stop the worker gracefully."""
        print("\nStopping workers gracefully...")
        self.running = False

        if self.worker_thread:
            self.worker_thread.join(timeout=10)

        print("Workers stopped.")

def signal_handler(signum, _frame):
    """Handle shutdown signals."""
    print(f"\nReceived signal {signum}, shutting down...")
    sys.exit(0)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Start dynamic rate-limited workers')
    parser.add_argument('--concurrency', '-c', type=int, default=1,
                       help='Number of concurrent worker processes (default: 1)')
    parser.add_argument('--loglevel', '-l', default='info',
                       choices=['debug', 'info', 'warning', 'error'],
                       help='Log level (default: info)')
    parser.add_argument('--test-redis', action='store_true',
                       help='Test Redis connection before starting')

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

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start worker manager
    worker_manager = DynamicWorkerManager(
        concurrency=args.concurrency,
        loglevel=args.loglevel
    )

    try:
        worker_manager.start()
    except KeyboardInterrupt:
        worker_manager.stop()
    except Exception as e:
        print(f"Worker error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()