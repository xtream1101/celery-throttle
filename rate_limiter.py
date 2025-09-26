import redis
import time
import json
from typing import Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class TokenBucketConfig:
    """Configuration for a token bucket rate limiter."""
    requests_per_minute: float
    queue_name: str

    @property
    def tokens_per_second(self) -> float:
        """Convert requests per minute to tokens per second."""
        return self.requests_per_minute / 60.0

class RedisTokenBucketRateLimiter:
    """Redis-based token bucket rate limiter with strict rate enforcement."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

        # Lua script for atomic token bucket operations
        self.acquire_token_script = self.redis.register_script("""
            local key = KEYS[1]
            local tokens_per_second = tonumber(ARGV[1])
            local bucket_size = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])

            -- Get current bucket state
            local bucket_data = redis.call('HMGET', key, 'tokens', 'last_refill')
            local current_tokens = tonumber(bucket_data[1]) or bucket_size
            local last_refill = tonumber(bucket_data[2]) or now

            -- Calculate tokens to add based on elapsed time
            local time_elapsed = now - last_refill
            local tokens_to_add = time_elapsed * tokens_per_second

            -- Update token count (cap at bucket size)
            current_tokens = math.min(bucket_size, current_tokens + tokens_to_add)

            -- Check if we can acquire a token
            if current_tokens >= 1 then
                current_tokens = current_tokens - 1

                -- Update bucket state
                redis.call('HMSET', key, 'tokens', current_tokens, 'last_refill', now)
                redis.call('EXPIRE', key, 3600) -- 1 hour TTL

                -- Return success with time until next token
                local time_until_next = 1.0 / tokens_per_second
                return {1, time_until_next}
            else
                -- Update last_refill time even on failure
                redis.call('HMSET', key, 'tokens', current_tokens, 'last_refill', now)
                redis.call('EXPIRE', key, 3600)

                -- Calculate wait time for next token
                local wait_time = (1.0 - current_tokens) / tokens_per_second
                return {0, wait_time}
            end
        """)

        # Lua script for getting bucket metrics
        self.get_metrics_script = self.redis.register_script("""
            local key = KEYS[1]
            local tokens_per_second = tonumber(ARGV[1])
            local bucket_size = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])

            local bucket_data = redis.call('HMGET', key, 'tokens', 'last_refill')
            local current_tokens = tonumber(bucket_data[1]) or bucket_size
            local last_refill = tonumber(bucket_data[2]) or now

            -- Calculate updated token count
            local time_elapsed = now - last_refill
            local tokens_to_add = time_elapsed * tokens_per_second
            current_tokens = math.min(bucket_size, current_tokens + tokens_to_add)

            return {current_tokens, last_refill, tokens_per_second, bucket_size}
        """)

    def acquire_token(self, config: TokenBucketConfig) -> tuple[bool, float]:
        """
        Try to acquire a token from the bucket.

        Returns:
            (success: bool, wait_time: float) - wait_time is seconds until next token available
        """
        bucket_key = f"rate_limit:bucket:{config.queue_name}"
        bucket_size = max(1.0, config.requests_per_minute / 60.0 * 2)  # 2 second worth of tokens max
        now = time.time()

        result = self.acquire_token_script(
            keys=[bucket_key],
            args=[config.tokens_per_second, bucket_size, now]
        )

        success = bool(result[0])
        wait_time = float(result[1])

        return success, wait_time

    def get_bucket_metrics(self, config: TokenBucketConfig) -> Dict[str, Any]:
        """Get current bucket state and metrics."""
        bucket_key = f"rate_limit:bucket:{config.queue_name}"
        bucket_size = max(1.0, config.requests_per_minute / 60.0 * 2)
        now = time.time()

        result = self.get_metrics_script(
            keys=[bucket_key],
            args=[config.tokens_per_second, bucket_size, now]
        )

        current_tokens = float(result[0])
        last_refill = float(result[1])
        tokens_per_second = float(result[2])
        bucket_capacity = float(result[3])

        return {
            'current_tokens': current_tokens,
            'bucket_capacity': bucket_capacity,
            'tokens_per_second': tokens_per_second,
            'requests_per_minute': config.requests_per_minute,
            'last_refill_timestamp': last_refill,
            'last_refill_datetime': datetime.fromtimestamp(last_refill).isoformat(),
            'utilization_percent': ((bucket_capacity - current_tokens) / bucket_capacity) * 100,
            'estimated_wait_for_token': max(0, (1.0 - current_tokens) / tokens_per_second) if current_tokens < 1 and tokens_per_second > 0 else 0
        }

    def reset_bucket(self, queue_name: str) -> None:
        """Reset a token bucket (useful for testing)."""
        bucket_key = f"rate_limit:bucket:{queue_name}"
        self.redis.delete(bucket_key)

    def list_active_buckets(self) -> list[str]:
        """List all active rate limit buckets."""
        keys = self.redis.keys("rate_limit:bucket:*")
        return [key.decode().replace("rate_limit:bucket:", "") for key in keys]