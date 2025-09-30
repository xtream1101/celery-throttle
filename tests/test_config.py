from unittest.mock import Mock, patch

from celery_throttle.config import RedisConfig, CeleryConfig, CeleryThrottleConfig


class TestRedisConfig:
    """Test cases for RedisConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = RedisConfig()

        assert config.host == "localhost"
        assert config.port == 6379
        assert config.db == 0
        assert config.password is None
        assert config.decode_responses is False

    def test_custom_values(self):
        """Test custom configuration values."""
        config = RedisConfig(
            host="redis.example.com",
            port=6380,
            db=1,
            password="secret",
            decode_responses=True,
        )

        assert config.host == "redis.example.com"
        assert config.port == 6380
        assert config.db == 1
        assert config.password == "secret"
        assert config.decode_responses is True

    def test_create_client(self):
        """Test creating Redis client from config."""
        config = RedisConfig(
            host="localhost", port=6379, db=0, password="test_password"
        )

        with patch("celery_throttle.config.redis.Redis") as mock_redis_cls:
            mock_client = Mock()
            mock_redis_cls.return_value = mock_client

            client = config.create_client()

            mock_redis_cls.assert_called_once_with(
                host="localhost",
                port=6379,
                db=0,
                password="test_password",
                decode_responses=False,
            )
            assert client == mock_client

    def test_create_client_without_password(self):
        """Test creating Redis client without password."""
        config = RedisConfig(host="localhost", port=6379, db=0)

        with patch("celery_throttle.config.redis.Redis") as mock_redis_cls:
            mock_client = Mock()
            mock_redis_cls.return_value = mock_client

            config.create_client()

            mock_redis_cls.assert_called_once_with(
                host="localhost", port=6379, db=0, password=None, decode_responses=False
            )

    def test_environment_variable_loading(self):
        """Test loading config from environment variables."""
        with patch.dict(
            "os.environ",
            {
                "CELERY_THROTTLE_REDIS_HOST": "env-redis.example.com",
                "CELERY_THROTTLE_REDIS_PORT": "6380",
                "CELERY_THROTTLE_REDIS_DB": "2",
                "CELERY_THROTTLE_REDIS_PASSWORD": "env_secret",
            },
        ):
            config = RedisConfig()

            assert config.host == "env-redis.example.com"
            assert config.port == 6380
            assert config.db == 2
            assert config.password == "env_secret"


class TestCeleryConfig:
    """Test cases for CeleryConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = CeleryConfig()

        assert config.broker_url == "redis://localhost:6379/0"
        assert config.result_backend == "redis://localhost:6379/0"
        assert config.task_serializer == "json"
        assert config.accept_content == ["json"]
        assert config.result_serializer == "json"
        assert config.timezone == "UTC"
        assert config.enable_utc is True
        assert config.worker_prefetch_multiplier == 1
        assert config.task_acks_late is True
        assert config.worker_concurrency == 1

    def test_custom_values(self):
        """Test custom configuration values."""
        config = CeleryConfig(
            broker_url="redis://custom:6379/1",
            result_backend="redis://custom:6379/2",
            worker_concurrency=4,
        )

        assert config.broker_url == "redis://custom:6379/1"
        assert config.result_backend == "redis://custom:6379/2"
        assert config.worker_concurrency == 4

    def test_apply_to_app(self, celery_app):
        """Test applying config to Celery app."""
        config = CeleryConfig(
            broker_url="redis://test:6379/0",
            result_backend="redis://test:6379/1",
            worker_concurrency=2,
        )

        config.apply_to_app(celery_app)

        assert celery_app.conf.broker_url == "redis://test:6379/0"
        assert celery_app.conf.result_backend == "redis://test:6379/1"
        assert celery_app.conf.worker_concurrency == 2

    def test_apply_to_app_all_settings(self, celery_app):
        """Test that all config settings are applied to app."""
        config = CeleryConfig(
            broker_url="redis://test:6379/0",
            result_backend="redis://test:6379/1",
            task_serializer="json",
            accept_content=["json", "pickle"],
            result_serializer="json",
            timezone="America/New_York",
            enable_utc=False,
            worker_prefetch_multiplier=4,
            task_acks_late=False,
            worker_concurrency=8,
        )

        config.apply_to_app(celery_app)

        assert celery_app.conf.task_serializer == "json"
        assert celery_app.conf.accept_content == ["json", "pickle"]
        assert celery_app.conf.timezone == "America/New_York"
        assert celery_app.conf.enable_utc is False
        assert celery_app.conf.worker_prefetch_multiplier == 4
        assert celery_app.conf.task_acks_late is False

    def test_environment_variable_loading(self):
        """Test loading Celery config from environment variables."""
        with patch.dict(
            "os.environ",
            {
                "CELERY_THROTTLE_BROKER_URL": "redis://env:6379/0",
                "CELERY_THROTTLE_WORKER_CONCURRENCY": "5",
            },
        ):
            config = CeleryConfig()

            assert config.broker_url == "redis://env:6379/0"
            assert config.worker_concurrency == 5


class TestCeleryThrottleConfig:
    """Test cases for CeleryThrottleConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = CeleryThrottleConfig()

        assert isinstance(config.redis, RedisConfig)
        assert isinstance(config.celery, CeleryConfig)
        assert config.app_name == "celery-throttle"
        assert config.target_queue == "rate-limited-queue"
        assert config.queue_prefix == "throttle"

    def test_custom_values(self):
        """Test custom configuration values."""
        config = CeleryThrottleConfig(
            app_name="my-app", target_queue="my-queue", queue_prefix="myprefix"
        )

        assert config.app_name == "my-app"
        assert config.target_queue == "my-queue"
        assert config.queue_prefix == "myprefix"

    def test_nested_redis_config(self):
        """Test nested Redis configuration."""
        config = CeleryThrottleConfig(
            redis={"host": "custom-redis", "port": 6380, "db": 2}
        )

        assert config.redis.host == "custom-redis"
        assert config.redis.port == 6380
        assert config.redis.db == 2

    def test_nested_celery_config(self):
        """Test nested Celery configuration."""
        config = CeleryThrottleConfig(
            celery={"broker_url": "redis://custom:6379/0", "worker_concurrency": 5}
        )

        assert config.celery.broker_url == "redis://custom:6379/0"
        assert config.celery.worker_concurrency == 5

    def test_from_dict(self):
        """Test creating config from dictionary."""
        config_dict = {
            "app_name": "dict-app",
            "target_queue": "dict-queue",
            "queue_prefix": "dictprefix",
            "redis": {"host": "dict-redis", "port": 6380},
            "celery": {"broker_url": "redis://dict:6379/0", "worker_concurrency": 3},
        }

        config = CeleryThrottleConfig.from_dict(config_dict)

        assert config.app_name == "dict-app"
        assert config.target_queue == "dict-queue"
        assert config.queue_prefix == "dictprefix"
        assert config.redis.host == "dict-redis"
        assert config.redis.port == 6380
        assert config.celery.broker_url == "redis://dict:6379/0"
        assert config.celery.worker_concurrency == 3

    def test_from_dict_partial(self):
        """Test creating config from partial dictionary."""
        config_dict = {"app_name": "partial-app", "redis": {"host": "partial-redis"}}

        config = CeleryThrottleConfig.from_dict(config_dict)

        assert config.app_name == "partial-app"
        assert config.redis.host == "partial-redis"
        # Defaults should be used for missing values
        assert config.target_queue == "rate-limited-queue"
        assert config.redis.port == 6379

    def test_environment_variable_loading(self):
        """Test loading main config from environment variables."""
        with patch.dict(
            "os.environ",
            {
                "CELERY_THROTTLE_APP_NAME": "env-app",
                "CELERY_THROTTLE_TARGET_QUEUE": "env-queue",
                "CELERY_THROTTLE_QUEUE_PREFIX": "envprefix",
            },
        ):
            config = CeleryThrottleConfig()

            assert config.app_name == "env-app"
            assert config.target_queue == "env-queue"
            assert config.queue_prefix == "envprefix"

    def test_model_dump(self):
        """Test serializing config to dictionary."""
        config = CeleryThrottleConfig(app_name="test-app", target_queue="test-queue")

        config_dict = config.model_dump()

        assert isinstance(config_dict, dict)
        assert config_dict["app_name"] == "test-app"
        assert config_dict["target_queue"] == "test-queue"
        assert "redis" in config_dict
        assert "celery" in config_dict

    def test_redis_config_as_dict_in_init(self):
        """Test passing Redis config as dictionary in __init__."""
        config = CeleryThrottleConfig(
            redis={"host": "inline-redis", "port": 7000, "password": "inline-secret"}
        )

        assert config.redis.host == "inline-redis"
        assert config.redis.port == 7000
        assert config.redis.password == "inline-secret"

    def test_celery_config_as_dict_in_init(self):
        """Test passing Celery config as dictionary in __init__."""
        config = CeleryThrottleConfig(
            celery={"broker_url": "redis://inline:6379/0", "worker_concurrency": 10}
        )

        assert config.celery.broker_url == "redis://inline:6379/0"
        assert config.celery.worker_concurrency == 10

    def test_config_immutability_after_from_dict(self):
        """Test that config can be modified after creation from dict."""
        config_dict = {"app_name": "original-app", "target_queue": "original-queue"}

        config = CeleryThrottleConfig.from_dict(config_dict)

        # Modifying the original dict should not affect the config
        config_dict["app_name"] = "modified-app"

        assert config.app_name == "original-app"

    def test_validation_of_nested_configs(self):
        """Test that nested configs are properly validated."""
        # This should work - valid nested config
        config = CeleryThrottleConfig(
            redis=RedisConfig(host="test", port=6379),
            celery=CeleryConfig(broker_url="redis://test:6379/0"),
        )

        assert config.redis.host == "test"
        assert config.celery.broker_url == "redis://test:6379/0"

    def test_complete_config_workflow(self):
        """Test complete configuration workflow."""
        # Create from dict
        config_dict = {
            "app_name": "workflow-app",
            "target_queue": "workflow-queue",
            "queue_prefix": "workflow",
            "redis": {"host": "workflow-redis", "port": 6380, "db": 1},
            "celery": {
                "broker_url": "redis://workflow:6380/1",
                "result_backend": "redis://workflow:6380/1",
                "worker_concurrency": 4,
            },
        }

        config = CeleryThrottleConfig.from_dict(config_dict)

        # Verify all values
        assert config.app_name == "workflow-app"
        assert config.target_queue == "workflow-queue"
        assert config.queue_prefix == "workflow"
        assert config.redis.host == "workflow-redis"
        assert config.redis.port == 6380
        assert config.redis.db == 1
        assert config.celery.broker_url == "redis://workflow:6380/1"
        assert config.celery.worker_concurrency == 4

        # Dump back to dict
        output_dict = config.model_dump()
        assert output_dict["app_name"] == "workflow-app"
