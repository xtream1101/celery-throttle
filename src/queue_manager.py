import uuid
import time
import json
from typing import Dict, List, Optional, Any
from datetime import datetime
import redis
from loguru import logger
from pydantic import BaseModel

from .rate_limiter import RateLimit, TokenBucketRateLimiter


class QueueConfig(BaseModel):
    name: str
    rate_limit: RateLimit
    created_at: datetime
    active: bool = True

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class QueueStats(BaseModel):
    name: str
    rate_limit: RateLimit
    tasks_waiting: int
    tasks_processing: int
    tasks_completed: int
    tasks_failed: int
    created_at: datetime
    active: bool


class UniversalQueueManager:
    """Manages dynamic queues with individual rate limiting."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.rate_limiter = TokenBucketRateLimiter(redis_client)
        self._queues_key = "queues:config"
        self._stats_key_prefix = "queues:stats"

    def create_queue(self, rate_limit_str: str) -> str:
        """Create a new dynamic queue with specified rate limit."""
        queue_name = f"batch_{uuid.uuid4()}"
        rate_limit = RateLimit.from_string(rate_limit_str)

        queue_config = QueueConfig(
            name=queue_name,
            rate_limit=rate_limit,
            created_at=datetime.now(),
            active=True
        )

        # Store queue configuration
        self.redis.hset(
            self._queues_key,
            queue_name,
            queue_config.model_dump_json()
        )

        # Initialize stats
        self._init_queue_stats(queue_name, queue_config)

        logger.info(f"Created queue {queue_name} with rate limit {rate_limit_str}")
        return queue_name

    def remove_queue(self, queue_name: str) -> bool:
        """Remove a queue and all its data."""
        if not self.queue_exists(queue_name):
            return False

        # Mark as inactive first
        self.deactivate_queue(queue_name)

        # Remove configuration
        self.redis.hdel(self._queues_key, queue_name)

        # Remove stats
        stats_key = f"{self._stats_key_prefix}:{queue_name}"
        self.redis.delete(stats_key)

        # Remove rate limit bucket
        bucket_key = f"rate_limit:{queue_name}"
        self.redis.delete(bucket_key)

        # Remove task queues (pending tasks)
        task_queue_key = f"queue:{queue_name}"
        processing_key = f"processing:{queue_name}"
        self.redis.delete(task_queue_key, processing_key)

        logger.info(f"Removed queue {queue_name}")
        return True

    def update_rate_limit(self, queue_name: str, rate_limit_str: str) -> bool:
        """Update the rate limit of an existing queue."""
        if not self.queue_exists(queue_name):
            return False

        rate_limit = RateLimit.from_string(rate_limit_str)
        queue_config_json = self.redis.hget(self._queues_key, queue_name)
        queue_config = QueueConfig.model_validate_json(queue_config_json)

        # Update rate limit
        queue_config.rate_limit = rate_limit

        # Store updated configuration
        self.redis.hset(
            self._queues_key,
            queue_name,
            queue_config.model_dump_json()
        )

        logger.info(f"Updated queue {queue_name} rate limit to {rate_limit_str}")
        return True

    def activate_queue(self, queue_name: str) -> bool:
        """Activate a queue."""
        return self._set_queue_active(queue_name, True)

    def deactivate_queue(self, queue_name: str) -> bool:
        """Deactivate a queue (stops processing but keeps data)."""
        return self._set_queue_active(queue_name, False)

    def _set_queue_active(self, queue_name: str, active: bool) -> bool:
        """Set queue active status."""
        if not self.queue_exists(queue_name):
            return False

        queue_config_json = self.redis.hget(self._queues_key, queue_name)
        queue_config = QueueConfig.model_validate_json(queue_config_json)

        queue_config.active = active
        self.redis.hset(
            self._queues_key,
            queue_name,
            queue_config.model_dump_json()
        )

        action = "activated" if active else "deactivated"
        logger.info(f"Queue {queue_name} {action}")
        return True

    def queue_exists(self, queue_name: str) -> bool:
        """Check if a queue exists."""
        return self.redis.hexists(self._queues_key, queue_name)

    def get_queue_config(self, queue_name: str) -> Optional[QueueConfig]:
        """Get configuration for a specific queue."""
        if not self.queue_exists(queue_name):
            return None

        queue_config_json = self.redis.hget(self._queues_key, queue_name)
        return QueueConfig.model_validate_json(queue_config_json)

    def list_queues(self) -> List[QueueConfig]:
        """List all queues."""
        queue_configs = []
        for queue_name, config_json in self.redis.hgetall(self._queues_key).items():
            queue_config = QueueConfig.model_validate_json(config_json.decode())
            queue_configs.append(queue_config)

        return sorted(queue_configs, key=lambda q: q.created_at)

    def get_active_queues(self) -> List[QueueConfig]:
        """Get list of active queues."""
        return [q for q in self.list_queues() if q.active]

    def _init_queue_stats(self, queue_name: str, queue_config: QueueConfig):
        """Initialize stats for a new queue."""
        stats_key = f"{self._stats_key_prefix}:{queue_name}"
        stats_data = {
            "tasks_waiting": 0,
            "tasks_processing": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "created_at": queue_config.created_at.isoformat()
        }
        self.redis.hmset(stats_key, stats_data)

    def get_queue_stats(self, queue_name: str) -> Optional[QueueStats]:
        """Get current statistics for a queue."""
        queue_config = self.get_queue_config(queue_name)
        if not queue_config:
            return None

        stats_key = f"{self._stats_key_prefix}:{queue_name}"
        stats_data = self.redis.hgetall(stats_key)

        if not stats_data:
            # Initialize stats if they don't exist
            self._init_queue_stats(queue_name, queue_config)
            stats_data = self.redis.hgetall(stats_key)

        # Also get current queue lengths
        task_queue_key = f"queue:{queue_name}"
        processing_key = f"processing:{queue_name}"

        tasks_waiting = self.redis.llen(task_queue_key)
        tasks_processing = self.redis.scard(processing_key)

        return QueueStats(
            name=queue_name,
            rate_limit=queue_config.rate_limit,
            tasks_waiting=tasks_waiting,
            tasks_processing=tasks_processing,
            tasks_completed=int(stats_data.get(b'tasks_completed', 0)),
            tasks_failed=int(stats_data.get(b'tasks_failed', 0)),
            created_at=queue_config.created_at,
            active=queue_config.active
        )

    def increment_stat(self, queue_name: str, stat_name: str, amount: int = 1):
        """Increment a specific statistic for a queue."""
        stats_key = f"{self._stats_key_prefix}:{queue_name}"
        self.redis.hincrby(stats_key, stat_name, amount)

    def can_process_task(self, queue_name: str) -> tuple[bool, float]:
        """Check if a task can be processed immediately (has available token)."""
        queue_config = self.get_queue_config(queue_name)
        if not queue_config or not queue_config.active:
            return False, 0.0

        return self.rate_limiter.try_acquire(queue_name, queue_config.rate_limit)

    def get_rate_limit_status(self, queue_name: str) -> Optional[Dict[str, Any]]:
        """Get current rate limit status for a queue."""
        queue_config = self.get_queue_config(queue_name)
        if not queue_config:
            return None

        return self.rate_limiter.get_status(queue_name, queue_config.rate_limit)