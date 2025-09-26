import pytest
from src.rate_limiter import RateLimit


class TestRateLimitParsing:
    """Test cases for rate limit string parsing with different time units."""

    def test_seconds_parsing(self):
        """Test parsing with seconds unit."""
        rate_limit = RateLimit.from_string('10/60s')
        assert rate_limit.requests == 10
        assert rate_limit.period_seconds == 60
        assert rate_limit.burst_allowance == 1

    def test_minutes_parsing(self):
        """Test parsing with minutes unit."""
        rate_limit = RateLimit.from_string('10/5m')
        assert rate_limit.requests == 10
        assert rate_limit.period_seconds == 300  # 5 * 60
        assert rate_limit.burst_allowance == 1

    def test_hours_parsing(self):
        """Test parsing with hours unit."""
        rate_limit = RateLimit.from_string('4000/3h')
        assert rate_limit.requests == 4000
        assert rate_limit.period_seconds == 10800  # 3 * 3600
        assert rate_limit.burst_allowance == 1

    def test_burst_allowance_seconds(self):
        """Test parsing with burst allowance and seconds."""
        rate_limit = RateLimit.from_string('10/60s:5')
        assert rate_limit.requests == 10
        assert rate_limit.period_seconds == 60
        assert rate_limit.burst_allowance == 5

    def test_burst_allowance_hours(self):
        """Test parsing with burst allowance and hours."""
        rate_limit = RateLimit.from_string('4000/3h:50')
        assert rate_limit.requests == 4000
        assert rate_limit.period_seconds == 10800
        assert rate_limit.burst_allowance == 50

    @pytest.mark.parametrize("rate_string,expected_requests,expected_seconds,expected_burst", [
        ('10/60s', 10, 60, 1),
        ('10/5m', 10, 300, 1),
        ('100/1h', 100, 3600, 1),
        ('5/30m', 5, 1800, 1),
        ('4000/3h', 4000, 10800, 1),
        ('10/60s:5', 10, 60, 5),
        ('4000/3h:50', 4000, 10800, 50),
        ('1/1s', 1, 1, 1),
        ('3600/1h', 3600, 3600, 1),
        ('60/1m', 60, 60, 1),
    ])
    def test_various_valid_formats(self, rate_string, expected_requests, expected_seconds, expected_burst):
        """Test various valid rate limit formats."""
        rate_limit = RateLimit.from_string(rate_string)
        assert rate_limit.requests == expected_requests
        assert rate_limit.period_seconds == expected_seconds
        assert rate_limit.burst_allowance == expected_burst

    @pytest.mark.parametrize("invalid_string", [
        '10/60',        # Missing time unit
        '10/60x',       # Invalid time unit
        'invalid',      # Completely invalid
        '10/60s:',      # Missing burst value
        '10/60s:0',     # Zero burst (should be positive)
        '0/60s',        # Zero requests
        '10/0s',        # Zero period
        '/60s',         # Missing requests
        '10/',          # Missing period and unit
        '10/60s:abc',   # Non-numeric burst
        'abc/60s',      # Non-numeric requests
        '10/abc/s',     # Non-numeric period
    ])
    def test_invalid_formats(self, invalid_string):
        """Test that invalid formats raise ValueError."""
        with pytest.raises(ValueError):
            RateLimit.from_string(invalid_string)


class TestRateLimitStringRepresentation:
    """Test cases for rate limit string representation."""

    def test_seconds_representation(self):
        """Test string representation for periods that don't divide evenly into minutes/hours."""
        rate_limit = RateLimit(requests=10, period_seconds=45)
        assert str(rate_limit) == "10/45s"

    def test_minutes_representation(self):
        """Test string representation for periods that divide evenly into minutes."""
        rate_limit = RateLimit(requests=10, period_seconds=300)  # 5 minutes
        assert str(rate_limit) == "10/5m"

    def test_hours_representation(self):
        """Test string representation for periods that divide evenly into hours."""
        rate_limit = RateLimit(requests=4000, period_seconds=10800)  # 3 hours
        assert str(rate_limit) == "4000/3h"

    def test_burst_allowance_representation(self):
        """Test string representation with burst allowance."""
        rate_limit = RateLimit(requests=10, period_seconds=60, burst_allowance=5)
        assert str(rate_limit) == "10/1m:5"

    def test_round_trip_conversion(self):
        """Test that parsing and string representation are consistent."""
        original_strings = ['10/60s', '10/5m', '4000/3h', '10/60s:5', '4000/3h:50']

        for original in original_strings:
            rate_limit = RateLimit.from_string(original)
            # Parse again from string representation
            rate_limit2 = RateLimit.from_string(str(rate_limit))

            # Should have same values
            assert rate_limit.requests == rate_limit2.requests
            assert rate_limit.period_seconds == rate_limit2.period_seconds
            assert rate_limit.burst_allowance == rate_limit2.burst_allowance


class TestRateLimitCalculations:
    """Test rate limit calculations and edge cases."""

    def test_large_hour_window_calculation(self):
        """Test the example case: 4000 tasks over 3 hours."""
        rate_limit = RateLimit.from_string('4000/3h')

        # Verify basic properties
        assert rate_limit.requests == 4000
        assert rate_limit.period_seconds == 10800  # 3 * 3600

        # Verify rate calculations
        requests_per_second = rate_limit.requests / rate_limit.period_seconds
        time_between_tasks = rate_limit.period_seconds / rate_limit.requests

        assert abs(requests_per_second - (4000 / 10800)) < 1e-10
        assert abs(time_between_tasks - 2.7) < 1e-10  # 10800 / 4000 = 2.7 seconds

    def test_one_per_hour(self):
        """Test very slow rate limit: 1 task per hour."""
        rate_limit = RateLimit.from_string('1/1h')

        assert rate_limit.requests == 1
        assert rate_limit.period_seconds == 3600
        assert str(rate_limit) == "1/1h"

    def test_high_frequency(self):
        """Test high frequency rate limit."""
        rate_limit = RateLimit.from_string('1000/1m')

        assert rate_limit.requests == 1000
        assert rate_limit.period_seconds == 60
        assert str(rate_limit) == "1000/1m"

        # Should be about 16.67 requests per second
        requests_per_second = rate_limit.requests / rate_limit.period_seconds
        assert abs(requests_per_second - (1000 / 60)) < 1e-10