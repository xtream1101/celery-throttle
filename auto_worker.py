#!/usr/bin/env python3
"""
Auto-restarting worker that monitors for new queues and restarts itself when needed.
This provides true dynamic queue support without complex control mechanisms.
"""

import os
import sys
import time
import signal
import subprocess
from queue_manager import RateLimitedQueueManager

class AutoRestartingWorker:
    """Worker that automatically restarts when new queues are detected."""

    def __init__(self, concurrency: int = 1, loglevel: str = 'info'):
        self.concurrency = concurrency
        self.loglevel = loglevel
        self.manager = RateLimitedQueueManager()
        self.running = False
        self.worker_process = None
        self.last_queues = set()

    def get_all_queues(self):
        """Get all active queues."""
        try:
            # Get all queue config keys
            queue_keys = self.manager.redis.keys("queue:*:config")
            queues = set()

            for key in queue_keys:
                if isinstance(key, bytes):
                    key = key.decode()
                queue_name = key.replace("queue:", "").replace(":config", "")
                queues.add(queue_name)

            # Always include default queue
            queues.add('default')
            return queues

        except Exception as e:
            print(f"Error getting queues: {e}")
            return {'default'}

    def start(self):
        """Start the auto-restarting worker."""
        self.running = True
        self.last_queues = self.get_all_queues()

        print("🚀 Starting Auto-Restarting Rate-Limited Worker")
        print("✨ This worker automatically restarts when new queues are detected")
        print(f"📊 Concurrency: {self.concurrency}, Log Level: {self.loglevel}")
        print("🛑 Press Ctrl+C to stop\n")

        try:
            while self.running:
                current_queues = self.get_all_queues()

                # Check if we need to restart due to queue changes
                if current_queues != self.last_queues:
                    new_queues = current_queues - self.last_queues
                    removed_queues = self.last_queues - current_queues

                    print(f"\n🔄 Queue configuration changed!")
                    if new_queues:
                        print(f"📥 New queues: {', '.join(sorted(new_queues))}")
                    if removed_queues:
                        print(f"📤 Removed queues: {', '.join(sorted(removed_queues))}")

                    # Stop current worker
                    if self.worker_process:
                        print("⏹️  Stopping current worker...")
                        self._stop_worker()

                    self.last_queues = current_queues

                # Start or restart worker if needed
                if not self.worker_process or self.worker_process.poll() is not None:
                    self._start_worker()

                # Monitor worker and check for queue changes every 5 seconds
                for _ in range(50):  # Check every 0.1 seconds, 50 times = 5 seconds
                    if not self.running:
                        break

                    # Check if worker died
                    if self.worker_process and self.worker_process.poll() is not None:
                        print("⚠️  Worker process died, restarting...")
                        break

                    time.sleep(0.1)

        except KeyboardInterrupt:
            print("\n🛑 Shutdown requested...")
        finally:
            self.stop()

    def _start_worker(self):
        """Start a new worker process."""
        queue_list = sorted(self.last_queues)

        # Build queue arguments
        queue_args = []
        for queue in queue_list:
            queue_args.extend(['-Q', queue])

        cmd = [
            'uv', 'run', 'celery', '-A', 'celery_app', 'worker',
            f'--loglevel={self.loglevel}',
            f'--concurrency={self.concurrency}',
            '--prefetch-multiplier=1',
            '--without-gossip',
            '--without-mingle',
            '--without-heartbeat',
            '--task-events',
            '--pool=threads',
        ] + queue_args

        print(f"🎯 Starting worker for queues: {', '.join(queue_list)}")

        try:
            # Start worker process
            self.worker_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                preexec_fn=os.setsid  # Create new process group for clean shutdown
            )

            print(f"✅ Worker started (PID: {self.worker_process.pid})")

            # Show first few lines of output to confirm startup
            startup_timeout = time.time() + 10  # 10 second timeout
            while time.time() < startup_timeout:
                if self.worker_process.stdout.readable():
                    line = self.worker_process.stdout.readline()
                    if line and ('ready' in line.lower() or 'started' in line.lower()):
                        print(f"[WORKER] {line.strip()}")
                        break
                    elif line:
                        print(f"[WORKER] {line.strip()}")

                if self.worker_process.poll() is not None:
                    print("❌ Worker failed to start")
                    break

                time.sleep(0.1)

        except Exception as e:
            print(f"❌ Error starting worker: {e}")

    def _stop_worker(self):
        """Stop the current worker process."""
        if self.worker_process:
            try:
                # Try graceful shutdown first
                print("⏹️  Sending SIGTERM to worker...")
                os.killpg(os.getpgid(self.worker_process.pid), signal.SIGTERM)

                # Wait up to 10 seconds for graceful shutdown
                try:
                    self.worker_process.wait(timeout=10)
                    print("✅ Worker stopped gracefully")
                except subprocess.TimeoutExpired:
                    print("⚠️  Worker didn't stop gracefully, forcing...")
                    os.killpg(os.getpgid(self.worker_process.pid), signal.SIGKILL)
                    self.worker_process.wait()
                    print("✅ Worker stopped forcefully")

            except Exception as e:
                print(f"❌ Error stopping worker: {e}")
            finally:
                self.worker_process = None

    def stop(self):
        """Stop the auto-restarting worker."""
        print("🛑 Stopping auto-restarting worker...")
        self.running = False
        self._stop_worker()
        print("✅ Auto-restarting worker stopped")

    def get_status(self):
        """Get current status."""
        return {
            'running': self.running,
            'worker_alive': self.worker_process is not None and self.worker_process.poll() is None,
            'worker_pid': self.worker_process.pid if self.worker_process else None,
            'current_queues': sorted(self.last_queues),
            'queue_count': len(self.last_queues)
        }

def signal_handler(signum, _frame):
    """Handle shutdown signals."""
    print(f"\n🛑 Received signal {signum}")
    sys.exit(0)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Auto-restarting rate-limited worker')
    parser.add_argument('--concurrency', '-c', type=int, default=1,
                       help='Number of concurrent worker processes (default: 1)')
    parser.add_argument('--loglevel', '-l', default='info',
                       choices=['debug', 'info', 'warning', 'error'],
                       help='Log level (default: info)')
    parser.add_argument('--test-redis', action='store_true',
                       help='Test Redis connection before starting')
    parser.add_argument('--status', action='store_true',
                       help='Show current status')

    args = parser.parse_args()

    # Test Redis connection if requested
    if args.test_redis:
        try:
            manager = RateLimitedQueueManager()
            manager.redis.ping()
            print("✅ Redis connection successful")

            # Show current queues
            queues = manager.get_all_queue_statuses()
            if queues:
                print(f"📊 Found {len(queues)} existing queues:")
                for queue in queues:
                    print(f"   - {queue['queue_name']}: {queue['health']['total_tasks']} tasks")
            else:
                print("📭 No existing queues found")

        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            print("Make sure Redis is running on localhost:6379")
            sys.exit(1)

    # Show status if requested
    if args.status:
        try:
            worker = AutoRestartingWorker()
            status = worker.get_status()
            print("📊 Worker Status:")
            print(f"   Running: {status['running']}")
            print(f"   Worker Alive: {status['worker_alive']}")
            print(f"   Worker PID: {status['worker_pid']}")
            print(f"   Current Queues ({status['queue_count']}): {', '.join(status['current_queues'])}")
        except Exception as e:
            print(f"❌ Error getting status: {e}")
        return

    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create and start worker
    worker = AutoRestartingWorker(
        concurrency=args.concurrency,
        loglevel=args.loglevel
    )

    try:
        worker.start()
    except KeyboardInterrupt:
        worker.stop()
    except Exception as e:
        print(f"❌ Worker error: {e}")
        worker.stop()
        sys.exit(1)

if __name__ == '__main__':
    main()