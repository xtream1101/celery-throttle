import time
import uuid
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
import redis
import logging

logger = logging.getLogger(__name__)
from pydantic import BaseModel
import re


class RateLimit(BaseModel):
    requests: int
    period_seconds: int
    burst_allowance: int = 1

    @classmethod
    def from_string(cls, rate_string: str) -> "RateLimit":
        """Parse rate limit string like '10/60s', '10/5m', '4000/3h' or '10/60s:5' into RateLimit object."""
        # Pattern supports optional burst allowance and time units (s/m/h): '10/60s', '10/5m', '4000/3h', or '10/60s:5'
        pattern = r'^(\d+)/(\d+)([smh])(?::(\d+))?$'
        match = re.match(pattern, rate_string)
        if not match:
            raise ValueError(f"Invalid rate limit format: {rate_string}. Expected format: '10/60s', '10/5m', '4000/3h', or '10/60s:5'")

        requests = int(match.group(1))
        period_value = int(match.group(2))
        time_unit = match.group(3)
        burst_allowance = int(match.group(4)) if match.group(4) else 1

        # Validate values
        if requests <= 0:
            raise ValueError(f"Requests must be positive, got: {requests}")
        if period_value <= 0:
            raise ValueError(f"Period must be positive, got: {period_value}")
        if burst_allowance <= 0:
            raise ValueError(f"Burst allowance must be positive, got: {burst_allowance}")

        # Convert time period to seconds
        if time_unit == 's':
            period_seconds = period_value
        elif time_unit == 'm':
            period_seconds = period_value * 60
        elif time_unit == 'h':
            period_seconds = period_value * 3600

        return cls(requests=requests, period_seconds=period_seconds, burst_allowance=burst_allowance)

    def __str__(self) -> str:
        # Choose the most appropriate time unit for display
        if self.period_seconds % 3600 == 0:
            # Use hours if evenly divisible by 3600
            period_value = self.period_seconds // 3600
            time_unit = 'h'
        elif self.period_seconds % 60 == 0:
            # Use minutes if evenly divisible by 60
            period_value = self.period_seconds // 60
            time_unit = 'm'
        else:
            # Use seconds
            period_value = self.period_seconds
            time_unit = 's'

        if self.burst_allowance == 1:
            return f"{self.requests}/{period_value}{time_unit}"
        else:
            return f"{self.requests}/{period_value}{time_unit}:{self.burst_allowance}"


class TokenBucketRateLimiter:
    """Redis-based token bucket rate limiter with Lua scripts for atomic operations."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self._acquire_script = self._register_acquire_script()
        self._status_script = self._register_status_script()

    def _register_acquire_script(self) -> redis.client.Script:
        """Register Lua script for atomic token acquisition."""
        lua_script = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local current_time = tonumber(ARGV[3])

        -- Get current bucket state
        local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
        local tokens = tonumber(bucket[1]) or capacity
        local last_refill = tonumber(bucket[2]) or current_time

        -- Calculate tokens to add based on time elapsed
        local time_elapsed = current_time - last_refill
        local tokens_to_add = time_elapsed * refill_rate

        -- Update tokens (capped at capacity)
        tokens = math.min(capacity, tokens + tokens_to_add)

        -- Check if we can consume a token
        if tokens >= 1 then
            tokens = tokens - 1
            -- Update bucket state
            redis.call('HMSET', key, 'tokens', tokens, 'last_refill', current_time)
            redis.call('EXPIRE', key, 3600) -- Expire after 1 hour of inactivity
            return {1, tokens, 0} -- success, remaining tokens, wait time
        else
            -- Calculate wait time for next token
            local wait_time = math.ceil((1 - tokens) / refill_rate)
            -- Update last_refill time
            redis.call('HMSET', key, 'tokens', tokens, 'last_refill', current_time)
            redis.call('EXPIRE', key, 3600)
            return {0, tokens, wait_time} -- failure, remaining tokens, wait time
        end
        """
        return self.redis.register_script(lua_script)

    def _register_status_script(self) -> redis.client.Script:
        """Register Lua script for checking bucket status."""
        lua_script = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local current_time = tonumber(ARGV[3])

        -- Get current bucket state
        local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
        local tokens = tonumber(bucket[1]) or capacity
        local last_refill = tonumber(bucket[2]) or current_time

        -- Calculate tokens to add based on time elapsed
        local time_elapsed = current_time - last_refill
        local tokens_to_add = time_elapsed * refill_rate

        -- Update tokens (capped at capacity)
        tokens = math.min(capacity, tokens + tokens_to_add)

        -- Calculate next token availability
        local next_token_time = 0
        if tokens < capacity then
            next_token_time = current_time + math.ceil((1 - (tokens % 1)) / refill_rate)
        end

        return {tokens, next_token_time}
        """
        return self.redis.register_script(lua_script)

    def try_acquire(self, queue_name: str, rate_limit: RateLimit) -> Tuple[bool, float]:
        """
        Try to acquire a token for the given queue.

        Returns:
            Tuple of (success: bool, wait_time: float)
            - success: True if token was acquired, False otherwise
            - wait_time: seconds to wait before next token is available (0 if success)
        """
        bucket_key = f"rate_limit:{queue_name}"
        current_time = time.time()

        # Calculate refill rate (tokens per second)
        refill_rate = rate_limit.requests / rate_limit.period_seconds

        try:
            result = self._acquire_script(
                keys=[bucket_key],
                args=[rate_limit.burst_allowance, refill_rate, current_time]
            )
            success, remaining_tokens, wait_time = result

            logger.debug(f"Token acquisition for {queue_name}: success={bool(success)}, "
                        f"remaining={remaining_tokens}, wait={wait_time}s")

            return bool(success), float(wait_time)

        except redis.RedisError as e:
            logger.error(f"Redis error during token acquisition for {queue_name}: {e}")
            # In case of Redis error, allow the operation (fail open)
            return True, 0.0

    def get_status(self, queue_name: str, rate_limit: RateLimit) -> Dict[str, float]:
        """Get current status of the token bucket."""
        bucket_key = f"rate_limit:{queue_name}"
        current_time = time.time()

        # Calculate refill rate (tokens per second)
        refill_rate = rate_limit.requests / rate_limit.period_seconds

        try:
            result = self._status_script(
                keys=[bucket_key],
                args=[rate_limit.burst_allowance, refill_rate, current_time]
            )
            available_tokens, next_token_time = result

            return {
                "available_tokens": float(available_tokens),
                "capacity": float(rate_limit.burst_allowance),
                "refill_rate": refill_rate,
                "next_token_at": float(next_token_time) if next_token_time > 0 else None,
                "next_token_in": max(0, float(next_token_time) - current_time) if next_token_time > 0 else 0
            }

        except redis.RedisError as e:
            logger.error(f"Redis error getting status for {queue_name}: {e}")
            return {
                "available_tokens": 0.0,
                "capacity": float(rate_limit.burst_allowance),
                "refill_rate": refill_rate,
                "next_token_at": None,
                "next_token_in": 0.0
            }