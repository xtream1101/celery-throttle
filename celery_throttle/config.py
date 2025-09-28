from typing import Optional, Dict, Any, Union
import redis
from celery import Celery
from pydantic import BaseModel, Field
from loguru import logger


class RedisConfig(BaseModel):
    """Redis connection configuration."""
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
            decode_responses=self.decode_responses
        )


class CeleryConfig(BaseModel):
    """Celery configuration for rate limiting."""
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


class CeleryThrottleConfig(BaseModel):
    """Main configuration for CeleryThrottle."""
    redis: RedisConfig = Field(default_factory=RedisConfig)
    celery: CeleryConfig = Field(default_factory=CeleryConfig)
    app_name: str = "celery-throttle"

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "CeleryThrottleConfig":
        """Create configuration from a dictionary."""
        return cls.model_validate(config_dict)

    @classmethod
    def from_env(cls, prefix: str = "CELERY_THROTTLE_") -> "CeleryThrottleConfig":
        """Create configuration from environment variables."""
        import os

        config_dict = {}

        # Redis configuration
        redis_config = {}
        if f"{prefix}REDIS_HOST" in os.environ:
            redis_config["host"] = os.environ[f"{prefix}REDIS_HOST"]
        if f"{prefix}REDIS_PORT" in os.environ:
            redis_config["port"] = int(os.environ[f"{prefix}REDIS_PORT"])
        if f"{prefix}REDIS_DB" in os.environ:
            redis_config["db"] = int(os.environ[f"{prefix}REDIS_DB"])
        if f"{prefix}REDIS_PASSWORD" in os.environ:
            redis_config["password"] = os.environ[f"{prefix}REDIS_PASSWORD"]

        if redis_config:
            config_dict["redis"] = redis_config

        # Celery configuration
        celery_config = {}
        if f"{prefix}BROKER_URL" in os.environ:
            celery_config["broker_url"] = os.environ[f"{prefix}BROKER_URL"]
        if f"{prefix}RESULT_BACKEND" in os.environ:
            celery_config["result_backend"] = os.environ[f"{prefix}RESULT_BACKEND"]
        if f"{prefix}WORKER_CONCURRENCY" in os.environ:
            celery_config["worker_concurrency"] = int(os.environ[f"{prefix}WORKER_CONCURRENCY"])

        if celery_config:
            config_dict["celery"] = celery_config

        # App name
        if f"{prefix}APP_NAME" in os.environ:
            config_dict["app_name"] = os.environ[f"{prefix}APP_NAME"]

        return cls.from_dict(config_dict)