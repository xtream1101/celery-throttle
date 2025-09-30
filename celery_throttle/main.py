import logging
from typing import Any, Dict, Optional, Type, Union

import redis
from celery import Celery

from .config import CeleryThrottleConfig
from .queue.manager import UniversalQueueManager
from .tasks.processor import (
    RateLimitedTaskDispatcher,
    RateLimitedTaskProcessor,
    RateLimitedTaskSubmitter,
)

logger = logging.getLogger(__name__)


class CeleryThrottle:
    """
    Main interface for the Celery Throttle library.

    Provides rate-limited task processing with dynamic queue management.
    """

    def __init__(
        self,
        celery_app: Celery,
        redis_client: Optional[redis.Redis] = None,
        config: Optional[CeleryThrottleConfig] = None,
        task_processor: Optional[RateLimitedTaskProcessor] = None,
        task_processor_cls: Optional[Type[RateLimitedTaskProcessor]] = None,
        **kwargs,
    ):
        """
        Initialize CeleryThrottle.

        Configuration can be provided in multiple ways (in order of precedence):
        1. **kwargs override any config object settings
        2. Explicit config object
        3. Environment variables (using CELERY_THROTTLE_ prefix)
        4. Default values

        Args:
            celery_app: Celery app instance (required).
            redis_client: Existing Redis client. If None, creates a new one from config.
            config: Configuration object. If None, loads from environment variables and defaults.
            task_processor: Optional explicit task processor instance.
            task_processor_cls: Optional task processor class to instantiate.
            **kwargs: Additional configuration options that override config settings.
                     Examples: app_name="my-app", target_queue="custom-queue", queue_prefix="myprefix"

        Examples:
            # Simple initialization (loads from environment variables)
            throttle = CeleryThrottle(celery_app=app)

            # With explicit config
            config = CeleryThrottleConfig(app_name="myapp", queue_prefix="prefix")
            throttle = CeleryThrottle(celery_app=app, config=config)

            # With kwargs override
            throttle = CeleryThrottle(celery_app=app, target_queue="my-queue")

            # From config dict
            throttle = CeleryThrottle.from_config_dict(celery_app=app, config_dict={...})
        """
        # Setup configuration
        if config is None:
            config = CeleryThrottleConfig()

        # Override config with any provided kwargs
        if kwargs:
            config_dict = config.model_dump()
            config_dict.update(kwargs)
            config = CeleryThrottleConfig.from_dict(config_dict)

        self.config = config

        # Setup Redis client
        if redis_client is None:
            self.redis = self.config.redis.create_client()
            logger.info(
                f"Created Redis client: {self.config.redis.host}:{self.config.redis.port}"
            )
        else:
            self.redis = redis_client
            logger.info("Using provided Redis client")

        # Setup Celery app
        self.app = celery_app
        logger.info("Using provided Celery app")

        # Initialize components
        self.queue_manager = UniversalQueueManager(self.redis, self.config.queue_prefix)
        # Determine processor instance: prefer an explicit instance, otherwise a provided class,
        # otherwise fallback to the default RateLimitedTaskProcessor.
        if task_processor is not None:
            processor_instance = task_processor
        else:
            processor_cls = task_processor_cls or RateLimitedTaskProcessor
            processor_instance = processor_cls(
                self.app, self.redis, self.queue_manager, self.config.target_queue
            )

        # Apply the configured processor and wire submitter/dispatcher
        self.set_task_processor(processor_instance)

        logger.info("CeleryThrottle initialized successfully")

    @classmethod
    def from_config_dict(
        cls, celery_app: Celery, config_dict: Dict[str, Any], **kwargs
    ) -> "CeleryThrottle":
        """Create CeleryThrottle from a configuration dictionary."""
        config = CeleryThrottleConfig.from_dict(config_dict)
        return cls(celery_app=celery_app, config=config, **kwargs)

    def create_queue(self, rate_limit: str, queue_name: str = None) -> str:
        """Create a new rate-limited queue."""
        return self.queue_manager.create_queue(rate_limit, queue_name)

    def remove_queue(self, queue_name: str) -> bool:
        """Remove a queue and all its data."""
        return self.queue_manager.remove_queue(queue_name)

    def submit_task(self, queue_name: str, task_name: str, *args, **kwargs) -> bool:
        """Submit a task to a queue with specified Celery task name."""
        return self.task_submitter.submit_task(queue_name, task_name, *args, **kwargs)

    def submit_multiple_tasks(
        self, queue_name: str, tasks_list: list
    ) -> Dict[str, int]:
        """Submit multiple tasks to a queue. Each task in tasks_list should be a tuple of (task_name, args, kwargs)."""
        return self.task_submitter.submit_multiple_tasks(queue_name, tasks_list)

    def get_queue_stats(self, queue_name: str):
        """Get statistics for a queue."""
        return self.queue_manager.get_queue_stats(queue_name)

    def list_queues(self):
        """List all queues."""
        return self.queue_manager.list_queues()

    def get_rate_limit_status(self, queue_name: str):
        """Get rate limit status for a queue."""
        return self.queue_manager.get_rate_limit_status(queue_name)

    def run_dispatcher(self, interval: float = 0.5):
        """Start the task dispatcher."""
        logger.info(f"Starting task dispatcher with {interval}s interval")
        self.task_dispatcher.run_dispatcher(interval)

    def update_rate_limit(self, queue_name: str, rate_limit: str) -> bool:
        """Update rate limit for an existing queue."""
        return self.queue_manager.update_rate_limit(queue_name, rate_limit)

    def activate_queue(self, queue_name: str) -> bool:
        """Activate a queue."""
        return self.queue_manager.activate_queue(queue_name)

    def deactivate_queue(self, queue_name: str) -> bool:
        """Deactivate a queue."""
        return self.queue_manager.deactivate_queue(queue_name)

    def set_task_processor(
        self, processor: Union[RateLimitedTaskProcessor, Type[RateLimitedTaskProcessor]]
    ):
        """Replace or set the task processor used by this CeleryThrottle instance.

        Accepts either an already-instantiated RateLimitedTaskProcessor or a processor
        class (subclass of RateLimitedTaskProcessor) which will be instantiated using
        the current Celery app, Redis client and queue manager.
        """
        # If a class is provided, instantiate it
        if isinstance(processor, type):
            processor = processor(
                self.app, self.redis, self.queue_manager, self.config.target_queue
            )

        # At this point processor is an instance
        self.task_processor = processor

        # Ensure task_submitter / task_dispatcher exist and reference the processor
        if hasattr(self, "task_submitter"):
            self.task_submitter.task_processor = self.task_processor
        else:
            self.task_submitter = RateLimitedTaskSubmitter(
                self.redis, self.queue_manager, self.task_processor
            )

        if hasattr(self, "task_dispatcher"):
            self.task_dispatcher.task_processor = self.task_processor
        else:
            self.task_dispatcher = RateLimitedTaskDispatcher(
                self.redis, self.queue_manager, self.task_processor
            )

        logger.info(
            "Task processor set and submitter/dispatcher wired to new processor"
        )
