import redis
import time
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

@dataclass
class QueueHealth:
    """Health status of a queue."""
    queue_name: str
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    active_tasks: int
    waiting_tasks: int
    avg_processing_time: float
    current_rate: float  # tasks per minute
    target_rate: float   # configured rate limit
    rate_accuracy: float  # percentage of how close actual rate is to target
    oldest_waiting_task: Optional[float]  # timestamp of oldest waiting task
    health_score: float  # 0-100, overall health score

@dataclass
class TaskMetrics:
    """Metrics for an individual task."""
    task_id: str
    queue_name: str
    start_time: float
    end_time: Optional[float]
    status: str  # pending, processing, completed, failed
    processing_time: Optional[float]
    error_message: Optional[str] = None

class QueueMetricsCollector:
    """Collects and analyzes queue performance metrics."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    def record_task_start(self, queue_name: str, task_id: str, start_time: float) -> None:
        """Record when a task starts processing."""
        task_key = f"task:{queue_name}:{task_id}"
        self.redis.hset(task_key, mapping={
            'queue_name': queue_name,
            'task_id': task_id,
            'start_time': start_time,
            'status': 'processing',
            'created_at': time.time()
        })
        self.redis.expire(task_key, 86400)  # 24 hour TTL

        # Add to active tasks set
        active_key = f"queue:{queue_name}:active"
        self.redis.sadd(active_key, task_id)
        self.redis.expire(active_key, 86400)

        # Remove from waiting if it was there
        waiting_key = f"queue:{queue_name}:waiting"
        self.redis.srem(waiting_key, task_id)

    def record_task_completion(self, queue_name: str, task_id: str, start_time: float,
                             end_time: float, status: str, error_message: str = None) -> None:
        """Record task completion (success or failure)."""
        processing_time = end_time - start_time

        task_key = f"task:{queue_name}:{task_id}"
        self.redis.hset(task_key, mapping={
            'end_time': end_time,
            'status': status,
            'processing_time': processing_time,
            'error_message': error_message or ''
        })

        # Remove from active tasks
        active_key = f"queue:{queue_name}:active"
        self.redis.srem(active_key, task_id)

        # Add to completed/failed sets
        result_key = f"queue:{queue_name}:{status}"
        self.redis.sadd(result_key, task_id)
        self.redis.expire(result_key, 86400)

        # Update queue statistics
        self._update_queue_stats(queue_name, processing_time, status, end_time)

    def record_task_queued(self, queue_name: str, task_id: str, rate_limit: float) -> None:
        """Record when a task is added to the queue."""
        queue_time = time.time()

        # Add to waiting tasks
        waiting_key = f"queue:{queue_name}:waiting"
        self.redis.sadd(waiting_key, task_id)
        self.redis.expire(waiting_key, 86400)

        # Record queue config and task count
        config_key = f"queue:{queue_name}:config"
        self.redis.hset(config_key, mapping={
            'rate_limit': rate_limit,
            'created_at': queue_time,
            'last_task_added': queue_time
        })
        self.redis.expire(config_key, 86400)

        # Increment total task counter
        total_key = f"queue:{queue_name}:total_tasks"
        self.redis.incr(total_key)
        self.redis.expire(total_key, 86400)

    def get_queue_health(self, queue_name: str) -> QueueHealth:
        """Get comprehensive health metrics for a queue."""
        # Basic counts
        total_tasks = int(self.redis.get(f"queue:{queue_name}:total_tasks") or 0)
        completed_tasks = self.redis.scard(f"queue:{queue_name}:success")
        failed_tasks = self.redis.scard(f"queue:{queue_name}:failed")
        active_tasks = self.redis.scard(f"queue:{queue_name}:active")
        waiting_tasks = self.redis.scard(f"queue:{queue_name}:waiting")

        # Get queue configuration
        config = self.redis.hgetall(f"queue:{queue_name}:config")
        target_rate = float(config.get(b'rate_limit', 0)) if config else 0

        # Calculate processing stats
        stats_key = f"queue:{queue_name}:stats"
        stats = self.redis.hgetall(stats_key)

        avg_processing_time = float(stats.get(b'avg_processing_time', 0)) if stats else 0
        last_completion_time = float(stats.get(b'last_completion_time', 0)) if stats else 0

        # Calculate current rate (tasks per minute)
        current_rate = self._calculate_current_rate(queue_name)

        # Calculate rate accuracy
        rate_accuracy = self._calculate_rate_accuracy(current_rate, target_rate)

        # Find oldest waiting task
        oldest_waiting_task = self._get_oldest_waiting_task(queue_name)

        # Calculate health score
        health_score = self._calculate_health_score(
            completed_tasks, failed_tasks, active_tasks, waiting_tasks,
            rate_accuracy, oldest_waiting_task
        )

        return QueueHealth(
            queue_name=queue_name,
            total_tasks=total_tasks,
            completed_tasks=completed_tasks,
            failed_tasks=failed_tasks,
            active_tasks=active_tasks,
            waiting_tasks=waiting_tasks,
            avg_processing_time=avg_processing_time,
            current_rate=current_rate,
            target_rate=target_rate,
            rate_accuracy=rate_accuracy,
            oldest_waiting_task=oldest_waiting_task,
            health_score=health_score
        )

    def get_all_queue_health(self) -> List[QueueHealth]:
        """Get health metrics for all active queues."""
        queue_keys = self.redis.keys("queue:*:config")
        queues = []

        for key in queue_keys:
            if isinstance(key, bytes):
                key = key.decode()
            queue_name = key.replace("queue:", "").replace(":config", "")
            queues.append(self.get_queue_health(queue_name))

        return queues

    def get_task_details(self, queue_name: str, task_id: str) -> Optional[TaskMetrics]:
        """Get detailed metrics for a specific task."""
        task_key = f"task:{queue_name}:{task_id}"
        task_data = self.redis.hgetall(task_key)

        if not task_data:
            return None

        return TaskMetrics(
            task_id=task_id,
            queue_name=queue_name,
            start_time=float(task_data.get(b'start_time', 0)),
            end_time=float(task_data.get(b'end_time', 0)) if task_data.get(b'end_time') else None,
            status=task_data.get(b'status', b'unknown').decode(),
            processing_time=float(task_data.get(b'processing_time', 0)) if task_data.get(b'processing_time') else None,
            error_message=task_data.get(b'error_message', b'').decode() or None
        )

    def _update_queue_stats(self, queue_name: str, processing_time: float,
                          status: str, completion_time: float) -> None:
        """Update running statistics for a queue."""
        stats_key = f"queue:{queue_name}:stats"

        # Get current stats
        current_stats = self.redis.hgetall(stats_key)
        current_avg = float(current_stats.get(b'avg_processing_time', 0)) if current_stats else 0
        completed_count = int(current_stats.get(b'completed_count', 0)) if current_stats else 0

        # Calculate new running average
        if status == 'success':
            completed_count += 1
            new_avg = ((current_avg * (completed_count - 1)) + processing_time) / completed_count
        else:
            new_avg = current_avg

        # Update stats
        self.redis.hset(stats_key, mapping={
            'avg_processing_time': new_avg,
            'completed_count': completed_count,
            'last_completion_time': completion_time
        })
        self.redis.expire(stats_key, 86400)

    def _calculate_current_rate(self, queue_name: str) -> float:
        """Calculate current processing rate in tasks per minute."""
        # Look at completions in the last minute
        now = time.time()
        one_minute_ago = now - 60

        # Get recently completed tasks
        success_tasks = self.redis.smembers(f"queue:{queue_name}:success")
        failed_tasks = self.redis.smembers(f"queue:{queue_name}:failed")

        recent_completions = 0
        for task_id in success_tasks.union(failed_tasks):
            if isinstance(task_id, bytes):
                task_id = task_id.decode()

            task_key = f"task:{queue_name}:{task_id}"
            end_time = self.redis.hget(task_key, 'end_time')

            if end_time and float(end_time) > one_minute_ago:
                recent_completions += 1

        return recent_completions  # Already per minute

    def _calculate_rate_accuracy(self, current_rate: float, target_rate: float) -> float:
        """Calculate how accurate the current rate is compared to target."""
        if target_rate == 0:
            return 100.0 if current_rate == 0 else 0.0

        accuracy = max(0, 100 - abs(current_rate - target_rate) / target_rate * 100)
        return accuracy

    def _get_oldest_waiting_task(self, queue_name: str) -> Optional[float]:
        """Find the timestamp of the oldest waiting task."""
        waiting_tasks = self.redis.smembers(f"queue:{queue_name}:waiting")

        if not waiting_tasks:
            return None

        oldest_time = None
        for task_id in waiting_tasks:
            if isinstance(task_id, bytes):
                task_id = task_id.decode()

            task_key = f"task:{queue_name}:{task_id}"
            created_at = self.redis.hget(task_key, 'created_at')

            if created_at:
                timestamp = float(created_at)
                if oldest_time is None or timestamp < oldest_time:
                    oldest_time = timestamp

        return oldest_time

    def _calculate_health_score(self, completed: int, failed: int, active: int,
                              waiting: int, rate_accuracy: float, oldest_waiting: Optional[float]) -> float:
        """Calculate overall health score (0-100)."""
        total_processed = completed + failed

        if total_processed == 0:
            return 50.0  # Neutral score for new queues

        # Success rate component (0-40 points)
        success_rate = completed / total_processed if total_processed > 0 else 0
        success_score = success_rate * 40

        # Rate accuracy component (0-30 points)
        rate_score = (rate_accuracy / 100) * 30

        # Queue backlog component (0-20 points)
        if waiting == 0:
            backlog_score = 20
        else:
            # Penalize large backlogs
            backlog_ratio = waiting / (active + waiting + 1)
            backlog_score = max(0, 20 - (backlog_ratio * 20))

        # Responsiveness component (0-10 points)
        if oldest_waiting is None:
            responsiveness_score = 10
        else:
            wait_time = time.time() - oldest_waiting
            # Penalize wait times > 5 minutes
            responsiveness_score = max(0, 10 - (wait_time / 300) * 10)

        return min(100, success_score + rate_score + backlog_score + responsiveness_score)

    def clear_queue_metrics(self, queue_name: str) -> None:
        """Clear all metrics for a queue (useful for testing)."""
        keys_to_delete = [
            f"queue:{queue_name}:*",
            f"task:{queue_name}:*",
            f"rate_limit:bucket:{queue_name}"
        ]

        for pattern in keys_to_delete:
            keys = self.redis.keys(pattern)
            if keys:
                self.redis.delete(*keys)