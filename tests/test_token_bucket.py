from unittest.mock import patch, MagicMock
import redis

from celery_throttle.core.rate_limiter import TokenBucketRateLimiter, RateLimit


class TestTokenBucketRateLimiter:
    """Test cases for TokenBucketRateLimiter."""

    def test_initialization(self, redis_client):
        """Test rate limiter initialization."""
        limiter = TokenBucketRateLimiter(redis_client)
        assert limiter.redis == redis_client
        assert limiter.key_prefix == ""
        assert limiter._acquire_script is not None
        assert limiter._status_script is not None

    def test_initialization_with_prefix(self, redis_client):
        """Test rate limiter initialization with key prefix."""
        limiter = TokenBucketRateLimiter(redis_client, key_prefix="myprefix:")
        assert limiter.key_prefix == "myprefix:"

    def test_try_acquire_success_initial(self, rate_limiter, rate_limit):
        """Test successful token acquisition on first request."""
        with patch("time.time", return_value=1000.0):
            success, wait_time = rate_limiter.try_acquire("test_queue", rate_limit)

        assert success is True
        assert wait_time == 0.0

    def test_try_acquire_multiple_tokens(self, rate_limiter):
        """Test acquiring multiple tokens in succession."""
        rate_limit = RateLimit.from_string("10/10s")  # 1 token per second

        with patch("time.time", return_value=1000.0):
            # Should succeed - bucket starts full with 1 token
            success1, wait_time1 = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success1 is True

            # Should fail - no tokens left
            success2, wait_time2 = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success2 is False
            assert wait_time2 > 0

    def test_try_acquire_with_refill(self, rate_limiter):
        """Test token refill after time passes."""
        rate_limit = RateLimit.from_string("10/10s")  # 1 token per second

        # First acquisition
        with patch("time.time", return_value=1000.0):
            success1, _ = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success1 is True

        # Try immediately - should fail
        with patch("time.time", return_value=1000.0):
            success2, wait_time = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success2 is False
            assert wait_time > 0

        # Wait 1 second - should succeed
        with patch("time.time", return_value=1001.0):
            success3, _ = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success3 is True

    def test_try_acquire_with_burst_allowance(self, rate_limiter):
        """Test token acquisition with burst allowance."""
        rate_limit = RateLimit.from_string("10/10s:5")  # 1 token/sec, burst of 5

        with patch("time.time", return_value=1000.0):
            # Should be able to acquire 5 tokens (burst allowance)
            for i in range(5):
                success, wait_time = rate_limiter.try_acquire("test_queue", rate_limit)
                assert success is True, f"Failed on token {i + 1}"

            # 6th should fail
            success, wait_time = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success is False
            assert wait_time > 0

    def test_try_acquire_different_queues(self, rate_limiter, rate_limit):
        """Test that different queues have independent token buckets."""
        with patch("time.time", return_value=1000.0):
            success1, _ = rate_limiter.try_acquire("queue1", rate_limit)
            success2, _ = rate_limiter.try_acquire("queue2", rate_limit)

        assert success1 is True
        assert success2 is True

    def test_try_acquire_redis_error_fail_open(self, redis_client, rate_limit):
        """Test that Redis errors fail open (allow the operation)."""
        limiter = TokenBucketRateLimiter(redis_client)

        # Mock the script to raise a Redis error
        limiter._acquire_script = MagicMock(
            side_effect=redis.RedisError("Connection error")
        )

        success, wait_time = limiter.try_acquire("test_queue", rate_limit)

        # Should fail open (allow operation)
        assert success is True
        assert wait_time == 0.0

    def test_get_status_initial(self, rate_limiter):
        """Test getting status of a new bucket."""
        rate_limit = RateLimit.from_string("10/60s:5")

        with patch("time.time", return_value=1000.0):
            status = rate_limiter.get_status("test_queue", rate_limit)

        assert status["available_tokens"] == 5.0  # Full capacity
        assert status["capacity"] == 5.0
        assert abs(status["refill_rate"] - (10 / 60)) < 1e-10
        assert status["next_token_in"] == 0

    def test_get_status_after_consumption(self, rate_limiter):
        """Test getting status after consuming tokens."""
        rate_limit = RateLimit.from_string("10/10s:3")  # 1 token/sec, burst of 3

        # Consume 2 tokens
        with patch("time.time", return_value=1000.0):
            rate_limiter.try_acquire("test_queue", rate_limit)
            rate_limiter.try_acquire("test_queue", rate_limit)

            status = rate_limiter.get_status("test_queue", rate_limit)

        assert status["available_tokens"] == 1.0
        assert status["capacity"] == 3.0

    def test_get_status_redis_error(self, redis_client):
        """Test get_status handles Redis errors gracefully."""
        rate_limit = RateLimit.from_string("10/60s:5")
        limiter = TokenBucketRateLimiter(redis_client)

        # Mock the script to raise a Redis error
        limiter._status_script = MagicMock(
            side_effect=redis.RedisError("Connection error")
        )

        status = limiter.get_status("test_queue", rate_limit)

        # Should return default error values
        assert status["available_tokens"] == 0.0
        assert status["capacity"] == 5.0
        assert status["next_token_at"] is None

    def test_key_prefix_in_bucket_key(self, redis_client):
        """Test that key prefix is correctly applied to bucket keys."""
        limiter = TokenBucketRateLimiter(redis_client, key_prefix="myapp:")
        rate_limit = RateLimit.from_string("10/60s")

        # Don't patch time.time as it breaks fakeredis Lua scripts
        limiter.try_acquire("test_queue", rate_limit)

        # Check that the key with prefix exists in Redis
        keys = redis_client.keys(b"myapp:rate_limit:*")
        assert len(keys) == 1
        assert b"myapp:rate_limit:test_queue" in keys

    def test_token_refill_rate_calculation(self, rate_limiter):
        """Test various refill rate calculations."""
        test_cases = [
            ("10/60s", 10 / 60),  # 0.1667 tokens/sec
            ("100/1m", 100 / 60),  # 1.6667 tokens/sec
            ("4000/3h", 4000 / 10800),  # ~0.37 tokens/sec
            ("1/1s", 1.0),  # 1 token/sec
        ]

        for rate_string, expected_rate in test_cases:
            rate_limit = RateLimit.from_string(rate_string)
            actual_rate = rate_limit.requests / rate_limit.period_seconds
            assert abs(actual_rate - expected_rate) < 1e-10

    def test_concurrent_token_acquisition(self, rate_limiter):
        """Test that Lua scripts ensure atomic token acquisition."""
        rate_limit = RateLimit.from_string("1/10s")  # Very slow rate

        with patch("time.time", return_value=1000.0):
            # First acquire should succeed
            success1, _ = rate_limiter.try_acquire("test_queue", rate_limit)

            # Second acquire at same time should fail
            success2, wait_time = rate_limiter.try_acquire("test_queue", rate_limit)

        assert success1 is True
        assert success2 is False
        assert wait_time > 0

    def test_bucket_expiry(self, rate_limiter, rate_limit):
        """Test that bucket keys have TTL set."""
        # Don't patch time.time as it breaks fakeredis Lua scripts
        rate_limiter.try_acquire("test_queue", rate_limit)

        # Check that TTL is set on the bucket key (no prefix in rate_limiter fixture)
        ttl = rate_limiter.redis.ttl("rate_limit:test_queue")
        assert ttl > 0
        assert ttl <= 3600  # Should be <= 1 hour

    def test_fractional_tokens(self, rate_limiter):
        """Test handling of fractional tokens during refill."""
        rate_limit = RateLimit.from_string("10/100s")  # 0.1 tokens per second

        # First acquisition
        with patch("time.time", return_value=1000.0):
            success1, _ = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success1 is True

        # After 5 seconds, only 0.5 tokens refilled - should fail
        with patch("time.time", return_value=1005.0):
            success2, _ = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success2 is False

        # After 10 seconds, 1 token refilled - should succeed
        with patch("time.time", return_value=1010.0):
            success3, _ = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success3 is True

    def test_high_frequency_rate_limit(self, rate_limiter):
        """Test high frequency rate limiting."""
        rate_limit = RateLimit.from_string("1000/1s:100")  # 1000 tokens/sec, burst 100

        with patch("time.time", return_value=1000.0):
            # Should handle 100 rapid acquisitions
            for i in range(100):
                success, _ = rate_limiter.try_acquire("test_queue", rate_limit)
                assert success is True, f"Failed on acquisition {i + 1}"

            # 101st should fail
            success, _ = rate_limiter.try_acquire("test_queue", rate_limit)
            assert success is False
