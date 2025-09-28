"""
Worker inspection module for monitoring Celery worker queue sizes and status.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from celery import Celery
import logging

logger = logging.getLogger(__name__)


@dataclass
class WorkerInfo:
    """Information about a Celery worker."""
    name: str
    status: str
    total_tasks: int
    active_tasks: int
    reserved_tasks: int
    queue_sizes: Dict[str, int]


@dataclass
class QueueInfo:
    """Information about a queue across all workers."""
    name: str
    total_size: int
    workers_listening: List[str]


class WorkerInspector:
    """Inspects Celery workers to get queue size and worker status information."""

    def __init__(self, celery_app: Celery):
        """
        Initialize the worker inspector.

        Args:
            celery_app: The Celery application instance
        """
        self.app = celery_app
        self.inspect = celery_app.control.inspect()

    def get_worker_queue_sizes(self) -> Dict[str, WorkerInfo]:
        """
        Get queue sizes and task counts for all active workers.

        Returns:
            Dictionary mapping worker names to WorkerInfo objects
        """
        workers_info = {}

        try:
            # Get worker status and stats
            stats = self.inspect.stats()
            active_queues = self.inspect.active_queues()
            reserved = self.inspect.reserved()
            active = self.inspect.active()

            if not stats:
                logger.warning("No worker stats available - no workers running?")
                return workers_info

            for worker_name, worker_stats in stats.items():
                # Get queue information for this worker
                worker_queues = active_queues.get(worker_name, []) if active_queues else []
                worker_reserved = reserved.get(worker_name, []) if reserved else []
                worker_active = active.get(worker_name, []) if active else []

                # Count tasks by queue
                queue_sizes = {}
                for queue_info in worker_queues:
                    queue_name = queue_info.get('name', 'unknown')
                    # For Celery, we need to get the actual queue length from broker
                    # This is approximated by reserved + active tasks for now
                    queue_sizes[queue_name] = 0  # Will be updated below

                # Count reserved tasks by queue
                for task in worker_reserved:
                    queue_name = task.get('delivery_info', {}).get('routing_key', 'default')
                    queue_sizes[queue_name] = queue_sizes.get(queue_name, 0) + 1

                workers_info[worker_name] = WorkerInfo(
                    name=worker_name,
                    status='online',
                    total_tasks=worker_stats.get('total', {}).get('tasks.received', 0),
                    active_tasks=len(worker_active),
                    reserved_tasks=len(worker_reserved),
                    queue_sizes=queue_sizes
                )

        except Exception as e:
            logger.error(f"Error inspecting workers: {e}")

        return workers_info

    def get_queue_summary(self) -> Dict[str, QueueInfo]:
        """
        Get a summary of all queues across all workers.

        Returns:
            Dictionary mapping queue names to QueueInfo objects
        """
        worker_infos = self.get_worker_queue_sizes()
        queue_summary = {}

        # Aggregate queue information across all workers
        for worker_name, worker_info in worker_infos.items():
            for queue_name, queue_size in worker_info.queue_sizes.items():
                if queue_name not in queue_summary:
                    queue_summary[queue_name] = QueueInfo(
                        name=queue_name,
                        total_size=0,
                        workers_listening=[]
                    )

                queue_summary[queue_name].total_size += queue_size
                if worker_name not in queue_summary[queue_name].workers_listening:
                    queue_summary[queue_name].workers_listening.append(worker_name)

        return queue_summary

    def get_broker_queue_length(self, queue_name: str) -> Optional[int]:
        """
        Get the actual queue length from the broker.
        This is broker-specific and may not work with all brokers.

        Args:
            queue_name: Name of the queue

        Returns:
            Queue length if available, None otherwise
        """
        try:
            # This method varies by broker type
            # For Redis broker, we could potentially query Redis directly
            # For RabbitMQ, we'd need to use management API
            # For now, we'll return None and rely on worker-reported sizes
            return None
        except Exception as e:
            logger.debug(f"Could not get broker queue length for {queue_name}: {e}")
            return None

    def is_healthy(self) -> bool:
        """
        Check if the worker infrastructure is healthy.

        Returns:
            True if workers are available and responding
        """
        try:
            stats = self.inspect.stats()
            return stats is not None and len(stats) > 0
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def get_worker_count(self) -> int:
        """
        Get the number of active workers.

        Returns:
            Number of active workers
        """
        try:
            stats = self.inspect.stats()
            return len(stats) if stats else 0
        except Exception as e:
            logger.error(f"Error getting worker count: {e}")
            return 0