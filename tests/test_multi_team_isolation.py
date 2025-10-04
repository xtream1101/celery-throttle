import pytest

from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig


class TestMultiTeamIsolation:
    """Test that multiple teams can use isolated worker queues without interference."""

    @pytest.fixture(autouse=True)
    def setup(self, redis_client):
        """Provide redis_client for all tests in this class."""
        self.redis_client = redis_client
        yield
        # Clean up after each test
        for prefix in ["team_a", "team_b", "throttle"]:
            keys = self.redis_client.keys(f"{prefix}:*")
            if keys:
                self.redis_client.delete(*keys)

    def test_separate_teams_have_isolated_queues(self, celery_app):
        """Test that two teams with same queue name don't interfere with each other."""
        # Create two CeleryThrottle instances with different prefixes
        throttle_team_a = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_a"),
        )
        throttle_team_b = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_b"),
        )

        # Both teams create a queue with the same name but different rate limits
        queue_name = "processing_queue"
        throttle_team_a.create_queue("10/60s", queue_name)
        throttle_team_b.create_queue("20/60s", queue_name)

        # Verify both queues exist independently
        team_a_queues = throttle_team_a.list_queues()
        team_b_queues = throttle_team_b.list_queues()

        assert len(team_a_queues) == 1
        assert len(team_b_queues) == 1

        assert team_a_queues[0].name == queue_name
        assert team_b_queues[0].name == queue_name

        # Verify different rate limits
        assert str(team_a_queues[0].rate_limit) == "10/1m"
        assert str(team_b_queues[0].rate_limit) == "20/1m"

        # Clean up
        throttle_team_a.remove_queue(queue_name)
        throttle_team_b.remove_queue(queue_name)

    def test_rate_limiters_are_isolated(self, celery_app):
        """Test that rate limiters don't share state between teams."""
        # Create two teams with same queue name and same rate limit
        throttle_team_a = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_a"),
        )
        throttle_team_b = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_b"),
        )

        queue_name = "rate_limited_queue"
        # Both teams: 2 requests per 10 seconds (very restrictive for testing)
        throttle_team_a.create_queue("2/10s", queue_name)
        throttle_team_b.create_queue("2/10s", queue_name)

        # Check rate limit status for both teams - should be independent
        team_a_status = throttle_team_a.get_rate_limit_status(queue_name)
        team_b_status = throttle_team_b.get_rate_limit_status(queue_name)

        # Both should start with full capacity
        assert team_a_status["available_tokens"] == 1.0  # burst_allowance defaults to 1
        assert team_b_status["available_tokens"] == 1.0

        # Consume a token from team A
        can_process_a, _ = throttle_team_a.queue_manager.can_process_task(queue_name)
        assert can_process_a is True

        # Check status again
        team_a_status_after = throttle_team_a.get_rate_limit_status(queue_name)
        team_b_status_after = throttle_team_b.get_rate_limit_status(queue_name)

        # Team A should have consumed a token
        assert (
            team_a_status_after["available_tokens"] < team_a_status["available_tokens"]
        )

        # Team B should still have full capacity (isolated)
        assert (
            team_b_status_after["available_tokens"] == team_b_status["available_tokens"]
        )

        # Clean up
        throttle_team_a.remove_queue(queue_name)
        throttle_team_b.remove_queue(queue_name)

    def test_redis_keys_are_namespaced(self, celery_app):
        """Test that Redis keys use proper namespacing for isolation."""
        throttle_team_a = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_a"),
        )
        throttle_team_b = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_b"),
        )

        queue_name = "test_queue"
        throttle_team_a.create_queue("5/60s", queue_name)
        throttle_team_b.create_queue("5/60s", queue_name)

        # Check that keys are properly namespaced
        team_a_keys = set(k.decode() for k in self.redis_client.keys("team_a:*"))
        team_b_keys = set(k.decode() for k in self.redis_client.keys("team_b:*"))

        # Verify no overlap
        assert len(team_a_keys & team_b_keys) == 0, "Team keys should not overlap"

        # Verify expected keys exist for team A
        assert any("team_a:queues:config" in k for k in team_a_keys)
        assert any("team_a:queues:stats" in k for k in team_a_keys)

        # Verify expected keys exist for team B
        assert any("team_b:queues:config" in k for k in team_b_keys)
        assert any("team_b:queues:stats" in k for k in team_b_keys)

        # Clean up
        throttle_team_a.remove_queue(queue_name)
        throttle_team_b.remove_queue(queue_name)

    def test_removing_queue_from_one_team_does_not_affect_other(self, celery_app):
        """Test that removing a queue from one team doesn't affect the other team."""
        throttle_team_a = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_a"),
        )
        throttle_team_b = CeleryThrottle(
            celery_app=celery_app,
            redis_client=self.redis_client,
            config=CeleryThrottleConfig(queue_prefix="team_b"),
        )

        queue_name = "shared_name_queue"
        throttle_team_a.create_queue("5/60s", queue_name)
        throttle_team_b.create_queue("5/60s", queue_name)

        # Verify both exist
        assert len(throttle_team_a.list_queues()) == 1
        assert len(throttle_team_b.list_queues()) == 1

        # Remove from team A
        assert throttle_team_a.remove_queue(queue_name) is True

        # Team A should have no queues
        assert len(throttle_team_a.list_queues()) == 0

        # Team B should still have their queue
        assert len(throttle_team_b.list_queues()) == 1
        assert throttle_team_b.list_queues()[0].name == queue_name

        # Clean up
        throttle_team_b.remove_queue(queue_name)

    def test_default_prefix_still_works(self, celery_app):
        """Test that the default prefix (no team isolation) still works."""
        # Create with default prefix
        throttle_default = CeleryThrottle(
            celery_app=celery_app, redis_client=self.redis_client
        )

        queue_name = "default_queue"
        throttle_default.create_queue("5/60s", queue_name)

        # Verify queue exists
        queues = throttle_default.list_queues()
        assert len(queues) == 1
        assert queues[0].name == queue_name

        # Verify keys use default "throttle" prefix
        default_keys = set(k.decode() for k in self.redis_client.keys("throttle:*"))
        assert len(default_keys) > 0
        assert any("throttle:queues:config" in k for k in default_keys)

        # Clean up
        throttle_default.remove_queue(queue_name)
