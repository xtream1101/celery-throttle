"""
Celery Throttle - Advanced rate limiting and queue management for Celery workers.

A robust Redis-based rate-limiting system for processing tasks with strict rate controls
using Celery workers and token bucket algorithms.
"""

from .core.rate_limiter import RateLimit, TokenBucketRateLimiter
from .queue.manager import UniversalQueueManager, QueueConfig, QueueStats
from .tasks.processor import RateLimitedTaskProcessor, RateLimitedTaskSubmitter, RateLimitedTaskDispatcher
from .main import CeleryThrottle

__version__ = "0.1.0"
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