import redis
import uuid
from typing import List, Dict, Any, Optional
from celery_app import app, process_task
from queue_metrics import QueueMetricsCollector, QueueHealth
from rate_limiter import RedisTokenBucketRateLimiter, TokenBucketConfig

class RateLimitedQueueManager:
    """Manages ad-hoc creation of rate-limited queues and task submission."""

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=False)
        self.metrics_collector = QueueMetricsCollector(self.redis)
        self.rate_limiter = RedisTokenBucketRateLimiter(self.redis)

    def create_queue(self, queue_name: str, rate_limit: float, tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a new rate-limited queue and add tasks to it.

        Args:
            queue_name: Unique name for the queue
            rate_limit: Requests per minute (strict rate limiting)
            tasks: List of task payloads to add to the queue

        Returns:
            Dictionary with queue creation summary
        """
        if not tasks:
            raise ValueError("Cannot create queue with no tasks")

        if rate_limit <= 0:
            raise ValueError("Rate limit must be positive")

        # Generate unique task IDs
        task_ids = []
        submitted_tasks = []

        for i, task_payload in enumerate(tasks):
            task_id = f"{queue_name}_{uuid.uuid4().hex[:8]}"
            task_ids.append(task_id)

            # Record task as queued
            self.metrics_collector.record_task_queued(queue_name, task_id, rate_limit)

            # Prepare task data
            task_data = {
                'queue_name': queue_name,
                'rate_limit': rate_limit,
                'payload': task_payload,
                'task_id': task_id
            }

            # Submit to Celery with routing to specific queue
            celery_task = process_task.apply_async(
                args=[task_data],
                queue=queue_name,
                routing_key=queue_name,
                task_id=task_id
            )

            submitted_tasks.append({
                'task_id': task_id,
                'celery_id': celery_task.id,
                'payload': task_payload
            })

        return {
            'queue_name': queue_name,
            'rate_limit': rate_limit,
            'tasks_submitted': len(submitted_tasks),
            'task_ids': task_ids,
            'created_at': self.redis.time()[0],  # Redis server timestamp
            'estimated_completion_time': (len(tasks) / rate_limit) * 60,  # seconds
            'tasks': submitted_tasks
        }

    def get_queue_status(self, queue_name: str) -> Dict[str, Any]:
        """Get current status of a queue."""
        health = self.metrics_collector.get_queue_health(queue_name)
        rate_config = TokenBucketConfig(health.target_rate, queue_name)
        bucket_metrics = self.rate_limiter.get_bucket_metrics(rate_config) if health.target_rate > 0 else {}

        return {
            'queue_name': queue_name,
            'health': health.__dict__,
            'rate_limiter': bucket_metrics,
            'progress_percentage': (health.completed_tasks + health.failed_tasks) / max(1, health.total_tasks) * 100,
            'estimated_time_remaining': self._estimate_remaining_time(health),
            'is_stuck': self._is_queue_stuck(health),
            'recommendations': self._get_recommendations(health)
        }

    def get_all_queue_statuses(self) -> List[Dict[str, Any]]:
        """Get status for all active queues."""
        all_health = self.metrics_collector.get_all_queue_health()
        return [self.get_queue_status(health.queue_name) for health in all_health]

    def get_queue_task_details(self, queue_name: str, limit: int = 100) -> Dict[str, Any]:
        """Get detailed information about tasks in a queue."""
        # Get task IDs from different states
        waiting_tasks = list(self.redis.smembers(f"queue:{queue_name}:waiting"))
        active_tasks = list(self.redis.smembers(f"queue:{queue_name}:active"))
        success_tasks = list(self.redis.smembers(f"queue:{queue_name}:success"))
        failed_tasks = list(self.redis.smembers(f"queue:{queue_name}:failed"))

        # Limit results
        all_task_ids = (waiting_tasks + active_tasks + success_tasks + failed_tasks)[:limit]

        task_details = []
        for task_id in all_task_ids:
            if isinstance(task_id, bytes):
                task_id = task_id.decode()

            details = self.metrics_collector.get_task_details(queue_name, task_id)
            if details:
                task_details.append(details.__dict__)

        return {
            'queue_name': queue_name,
            'total_tasks_shown': len(task_details),
            'task_breakdown': {
                'waiting': len(waiting_tasks),
                'active': len(active_tasks),
                'success': len(success_tasks),
                'failed': len(failed_tasks)
            },
            'tasks': task_details
        }

    def pause_queue(self, queue_name: str) -> bool:
        """Pause processing of a queue by setting rate limit to 0."""
        config_key = f"queue:{queue_name}:config"
        original_rate = self.redis.hget(config_key, 'rate_limit')

        if not original_rate:
            return False

        # Store original rate and set to 0
        self.redis.hset(config_key, 'original_rate_limit', original_rate)
        self.redis.hset(config_key, 'rate_limit', 0)
        self.redis.hset(config_key, 'paused', 1)

        return True

    def resume_queue(self, queue_name: str) -> bool:
        """Resume a paused queue."""
        config_key = f"queue:{queue_name}:config"
        original_rate = self.redis.hget(config_key, 'original_rate_limit')

        if not original_rate:
            return False

        # Restore original rate
        self.redis.hset(config_key, 'rate_limit', original_rate)
        self.redis.hdel(config_key, 'original_rate_limit', 'paused')

        return True

    def update_queue_rate_limit(self, queue_name: str, new_rate_limit: float) -> bool:
        """Update the rate limit for an existing queue."""
        config_key = f"queue:{queue_name}:config"

        if not self.redis.exists(config_key):
            return False

        self.redis.hset(config_key, 'rate_limit', new_rate_limit)

        # Reset token bucket to apply new rate immediately
        self.rate_limiter.reset_bucket(queue_name)

        return True

    def delete_queue(self, queue_name: str) -> bool:
        """Delete a queue and all its metrics (careful!)."""
        # Check if queue exists
        config_key = f"queue:{queue_name}:config"
        if not self.redis.exists(config_key):
            return False

        # Clear all queue data
        self.metrics_collector.clear_queue_metrics(queue_name)

        return True

    def get_system_overview(self) -> Dict[str, Any]:
        """Get high-level overview of the entire system."""
        all_queues = self.get_all_queue_statuses()

        total_tasks = sum(q['health']['total_tasks'] for q in all_queues)
        completed_tasks = sum(q['health']['completed_tasks'] for q in all_queues)
        failed_tasks = sum(q['health']['failed_tasks'] for q in all_queues)
        active_tasks = sum(q['health']['active_tasks'] for q in all_queues)
        waiting_tasks = sum(q['health']['waiting_tasks'] for q in all_queues)

        stuck_queues = [q for q in all_queues if q['is_stuck']]
        healthy_queues = [q for q in all_queues if q['health']['health_score'] > 80]
        unhealthy_queues = [q for q in all_queues if q['health']['health_score'] < 60]

        return {
            'timestamp': self.redis.time()[0],
            'total_queues': len(all_queues),
            'system_health': {
                'total_tasks': total_tasks,
                'completed_tasks': completed_tasks,
                'failed_tasks': failed_tasks,
                'active_tasks': active_tasks,
                'waiting_tasks': waiting_tasks,
                'success_rate': completed_tasks / max(1, total_tasks) * 100,
                'overall_progress': (completed_tasks + failed_tasks) / max(1, total_tasks) * 100
            },
            'queue_health': {
                'healthy_queues': len(healthy_queues),
                'unhealthy_queues': len(unhealthy_queues),
                'stuck_queues': len(stuck_queues)
            },
            'problem_queues': [
                {
                    'name': q['queue_name'],
                    'health_score': q['health']['health_score'],
                    'issue': q['recommendations'][0] if q['recommendations'] else 'Unknown'
                }
                for q in unhealthy_queues
            ]
        }

    def _estimate_remaining_time(self, health: QueueHealth) -> Optional[float]:
        """Estimate remaining processing time in seconds."""
        remaining_tasks = health.waiting_tasks + health.active_tasks

        if remaining_tasks == 0 or health.current_rate == 0:
            return 0

        # Use current rate if available, otherwise use target rate
        processing_rate = max(health.current_rate, health.target_rate) / 60  # convert to per second

        return remaining_tasks / processing_rate if processing_rate > 0 else None

    def _is_queue_stuck(self, health: QueueHealth) -> bool:
        """Determine if a queue appears to be stuck."""
        # Queue is stuck if:
        # 1. Has waiting tasks but no active tasks for > 2 minutes
        # 2. Has active tasks but no completions in last 5 minutes
        # 3. Health score is very low

        if health.health_score < 20:
            return True

        if health.waiting_tasks > 0 and health.active_tasks == 0:
            # Check if oldest waiting task is > 2 minutes old
            if health.oldest_waiting_task:
                import time
                wait_time = time.time() - health.oldest_waiting_task
                return wait_time > 120  # 2 minutes

        return False

    def _get_recommendations(self, health: QueueHealth) -> List[str]:
        """Generate recommendations based on queue health."""
        recommendations = []

        if health.health_score < 60:
            if health.failed_tasks > health.completed_tasks:
                recommendations.append("High failure rate - check task implementation")

            if health.rate_accuracy < 80:
                recommendations.append("Rate limiting not accurate - consider adjusting worker settings")

            if health.waiting_tasks > health.active_tasks * 10:
                recommendations.append("Large backlog - consider increasing rate limit or adding workers")

        if health.oldest_waiting_task:
            import time
            wait_time = time.time() - health.oldest_waiting_task
            if wait_time > 300:  # 5 minutes
                recommendations.append("Tasks waiting too long - check worker availability")

        if health.current_rate == 0 and health.waiting_tasks > 0:
            recommendations.append("No processing activity - check if workers are running")

        if not recommendations:
            recommendations.append("Queue is healthy")

        return recommendations