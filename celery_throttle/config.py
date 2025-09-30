from typing import Optional, Dict, Any
import redis
from celery import Celery
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
import logging

logger = logging.getLogger(__name__)


class RedisConfig(BaseSettings):
    """Redis connection configuration."""

    model_config = SettingsConfigDict(env_prefix="CELERY_THROTTLE_REDIS_")

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    decode_responses: bool = False

    def create_client(self) -> redis.Redis:
        """Create a Redis client from this configuration."""
        return redis.Redis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            decode_responses=self.decode_responses,
        )


class CeleryConfig(BaseSettings):
    """Celery configuration for rate limiting."""

    model_config = SettingsConfigDict(env_prefix="CELERY_THROTTLE_")

    broker_url: str = "redis://localhost:6379/0"
    result_backend: str = "redis://localhost:6379/0"
    task_serializer: str = "json"
    accept_content: list = Field(default_factory=lambda: ["json"])
    result_serializer: str = "json"
    timezone: str = "UTC"
    enable_utc: bool = True
    worker_prefetch_multiplier: int = 1
    task_acks_late: bool = True
    worker_concurrency: int = 1

    def apply_to_app(self, app: Celery):
        """Apply this configuration to a Celery app."""
        app.conf.update(
            broker_url=self.broker_url,
            result_backend=self.result_backend,
            task_serializer=self.task_serializer,
            accept_content=self.accept_content,
            result_serializer=self.result_serializer,
            timezone=self.timezone,
            enable_utc=self.enable_utc,
            worker_prefetch_multiplier=self.worker_prefetch_multiplier,
            task_acks_late=self.task_acks_late,
            worker_concurrency=self.worker_concurrency,
        )


class CeleryThrottleConfig(BaseSettings):
    """Main configuration for CeleryThrottle."""

    model_config = SettingsConfigDict(env_prefix="CELERY_THROTTLE_")

    redis: RedisConfig = Field(default_factory=RedisConfig)
    celery: CeleryConfig = Field(default_factory=CeleryConfig)
    app_name: str = "celery-throttle"
    target_queue: str = "rate-limited-queue"
    queue_prefix: str = "throttle"

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "CeleryThrottleConfig":
        """Create configuration from a dictionary."""
        return cls.model_validate(config_dict)
