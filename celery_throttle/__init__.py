"""
Celery Throttle - Advanced rate limiting and queue management for Celery workers.

A robust Redis-based rate-limiting system for processing tasks with strict rate controls
using Celery workers and token bucket algorithms.
"""

from .core.rate_limiter import RateLimit, TokenBucketRateLimiter
from .main import CeleryThrottle
from .queue.manager import QueueConfig, QueueStats, UniversalQueueManager
from .tasks.processor import (
    RateLimitedTaskDispatcher,
    RateLimitedTaskProcessor,
    RateLimitedTaskSubmitter,
)

__version__ = "0.1.0b1"
__all__ = [
    "CeleryThrottle",
    "RateLimit",
    "TokenBucketRateLimiter",
    "UniversalQueueManager",
    "QueueConfig",
    "QueueStats",
    "RateLimitedTaskProcessor",
    "RateLimitedTaskSubmitter",
    "RateLimitedTaskDispatcher",
]
