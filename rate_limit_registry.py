import time
import json
from typing import Optional, Dict, Any
from redis import Redis
from dataclasses import dataclass, asdict
from loguru import logger


@dataclass
class RateLimitConfig:
    """Configuration for a queue's rate limit"""
    queue_name: str
    refill_frequency: float  # seconds between tokens
    capacity: int = 1
    refill_amount: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RateLimitConfig':
        return cls(**data)


@dataclass
class QueueState:
    """Current state of a queue's rate limiting"""
    last_execution: Optional[float] = None
    tokens_available: int = 1

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'QueueState':
        return cls(**data)


class RateLimitRegistry:
    """Redis-backed registry for managing rate limit states across dynamic queues"""

    def __init__(self, redis_connection: Redis, key_prefix: str = "rate_limit"):
        self.redis = redis_connection
        self.key_prefix = key_prefix

    def _config_key(self, queue_name: str) -> str:
        return f"{self.key_prefix}:config:{queue_name}"

    def _state_key(self, queue_name: str) -> str:
        return f"{self.key_prefix}:state:{queue_name}"

    def register_queue(self, config: RateLimitConfig) -> None:
        """Register a new queue with its rate limit configuration"""
        config_key = self._config_key(config.queue_name)
        state_key = self._state_key(config.queue_name)

        # Store configuration
        self.redis.set(config_key, json.dumps(config.to_dict()))

        # Initialize state if it doesn't exist
        if not self.redis.exists(state_key):
            initial_state = QueueState(tokens_available=config.capacity)
            self.redis.set(state_key, json.dumps(initial_state.to_dict()))

        logger.info(f"Registered queue '{config.queue_name}' with rate limit {1/config.refill_frequency:.2f}/sec")

    def get_config(self, queue_name: str) -> Optional[RateLimitConfig]:
        """Get rate limit configuration for a queue"""
        config_key = self._config_key(queue_name)
        config_data = self.redis.get(config_key)

        if config_data:
            return RateLimitConfig.from_dict(json.loads(config_data))
        return None

    def get_state(self, queue_name: str) -> Optional[QueueState]:
        """Get current state for a queue"""
        state_key = self._state_key(queue_name)
        state_data = self.redis.get(state_key)

        if state_data:
            return QueueState.from_dict(json.loads(state_data))
        return None

    def update_state(self, queue_name: str, state: QueueState) -> None:
        """Update state for a queue"""
        state_key = self._state_key(queue_name)
        self.redis.set(state_key, json.dumps(state.to_dict()))

    def calculate_next_execution_time(self, queue_name: str) -> float:
        """Calculate when the next task for this queue should be executed"""
        config = self.get_config(queue_name)
        state = self.get_state(queue_name)

        if not config or not state:
            logger.warning(f"Queue '{queue_name}' not registered, executing immediately")
            return time.time()

        current_time = time.time()

        # If no previous execution, execute now
        if state.last_execution is None:
            return current_time

        # Calculate when next execution should be allowed
        next_allowed_time = state.last_execution + config.refill_frequency

        # If we can execute now, return current time
        if current_time >= next_allowed_time:
            return current_time

        # Otherwise, return the next allowed time
        return next_allowed_time

    def record_execution(self, queue_name: str, execution_time: Optional[float] = None) -> None:
        """Record that a task was executed for this queue"""
        if execution_time is None:
            execution_time = time.time()

        state = self.get_state(queue_name)
        if state:
            state.last_execution = execution_time
            self.update_state(queue_name, state)
            logger.debug(f"Recorded execution for '{queue_name}' at {execution_time}")

    def get_delay_until_ready(self, queue_name: str) -> float:
        """Get the delay in seconds until the queue is ready for next execution"""
        next_execution_time = self.calculate_next_execution_time(queue_name)
        delay = max(0, next_execution_time - time.time())
        return delay

    def list_registered_queues(self) -> list[str]:
        """List all registered queue names"""
        pattern = f"{self.key_prefix}:config:*"
        keys = self.redis.keys(pattern)
        return [key.decode().split(':')[-1] for key in keys]

    def get_queue_stats(self, queue_name: str) -> Dict[str, Any]:
        """Get comprehensive stats for a queue"""
        config = self.get_config(queue_name)
        state = self.get_state(queue_name)

        if not config or not state:
            return {}

        current_time = time.time()
        next_execution = self.calculate_next_execution_time(queue_name)
        delay = max(0, next_execution - current_time)

        return {
            "queue_name": queue_name,
            "rate_per_second": 1 / config.refill_frequency,
            "last_execution": state.last_execution,
            "next_execution": next_execution,
            "delay_seconds": delay,
            "is_ready": delay == 0
        }