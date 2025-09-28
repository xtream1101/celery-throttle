from typing import Optional, Union, Dict, Any, List, Type
import redis
from celery import Celery
import logging

logger = logging.getLogger(__name__)

from .config import CeleryThrottleConfig, RedisConfig, CeleryConfig
from .core.rate_limiter import TokenBucketRateLimiter
from .queue.manager import UniversalQueueManager
from .tasks.processor import RateLimitedTaskProcessor, RateLimitedTaskSubmitter, RateLimitedTaskDispatcher
from .monitoring.worker_inspector import WorkerInspector


class CeleryThrottle:
    """
    Main interface for the Celery Throttle library.

    Provides rate-limited task processing with dynamic queue management.
    """

    def __init__(
        self,
        celery_app: Optional[Celery] = None,
        redis_client: Optional[redis.Redis] = None,
        config: Optional[CeleryThrottleConfig] = None,
        task_processor: Optional[RateLimitedTaskProcessor] = None,
        task_processor_cls: Optional[Type[RateLimitedTaskProcessor]] = None,
        **kwargs
    ):
        """
        Initialize CeleryThrottle.

        Args:
            celery_app: Existing Celery app instance. If None, creates a new one.
            redis_client: Existing Redis client. If None, creates a new one.
            config: Configuration object. If None, uses defaults or kwargs.
            task_processor: Optional explicit task processor instance.
            task_processor_cls: Optional task processor class, used to instantiate a processor.
            **kwargs: Additional configuration options that override config.
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
            logger.info(f"Created Redis client: {self.config.redis.host}:{self.config.redis.port}")
        else:
            self.redis = redis_client
            logger.info("Using provided Redis client")

        # Setup Celery app
        if celery_app is None:
            self.app = Celery(self.config.app_name)
            self.config.celery.apply_to_app(self.app)
            logger.info(f"Created Celery app: {self.config.app_name}")
        else:
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
            processor_instance = processor_cls(self.app, self.redis, self.queue_manager, self.config.target_queue)

        # Apply the configured processor and wire submitter/dispatcher
        self.set_task_processor(processor_instance)
        self.worker_inspector = WorkerInspector(self.app)

        logger.info("CeleryThrottle initialized successfully")

    @classmethod
    def from_config_dict(cls, config_dict: Dict[str, Any], **kwargs) -> "CeleryThrottle":
        """Create CeleryThrottle from a configuration dictionary."""
        config = CeleryThrottleConfig.from_dict(config_dict)
        return cls(config=config, **kwargs)

    @classmethod
    def from_env(cls, prefix: str = "CELERY_THROTTLE_", **kwargs) -> "CeleryThrottle":
        """Create CeleryThrottle from environment variables."""
        config = CeleryThrottleConfig.from_env(prefix)
        return cls(config=config, **kwargs)

    def create_queue(self, rate_limit: str) -> str:
        """Create a new rate-limited queue."""
        return self.queue_manager.create_queue(rate_limit)

    def remove_queue(self, queue_name: str) -> bool:
        """Remove a queue and all its data."""
        return self.queue_manager.remove_queue(queue_name)

    def submit_task(self, queue_name: str, task_data: Dict[str, Any]) -> bool:
        """Submit a task to a queue."""
        return self.task_submitter.submit_task(queue_name, task_data)

    def submit_multiple_tasks(self, queue_name: str, tasks_data: list) -> Dict[str, int]:
        """Submit multiple tasks to a queue."""
        return self.task_submitter.submit_multiple_tasks(queue_name, tasks_data)

    def get_queue_stats(self, queue_name: str):
        """Get statistics for a queue."""
        return self.queue_manager.get_queue_stats(queue_name)

    def list_queues(self):
        """List all queues."""
        return self.queue_manager.list_queues()

    def get_rate_limit_status(self, queue_name: str):
        """Get rate limit status for a queue."""
        return self.queue_manager.get_rate_limit_status(queue_name)

    def run_worker(self, queues: Optional[Union[str, List[str]]] = None, **worker_kwargs):
        """Start a Celery worker with rate limiting configuration."""
        # Default options
        defaults = {
            'loglevel': 'info',
            'concurrency': 1,
            'prefetch-multiplier': 1,
        }

        # Override defaults with provided kwargs
        defaults.update(worker_kwargs)

        # Build worker options list
        worker_options = ['worker']

        # Add queue specification if provided
        if queues:
            if isinstance(queues, str):
                worker_options.extend(['--queues', queues])
            else:
                worker_options.extend(['--queues', ','.join(queues)])

        for key, value in defaults.items():
            # Convert underscores to hyphens for CLI compatibility
            cli_key = key.replace('_', '-')
            worker_options.append(f"--{cli_key}={value}")

        # Add required options
        worker_options.extend(['--without-mingle', '--without-gossip'])

        logger.info(f"Starting Celery worker with options: {worker_options}")
        self.app.worker_main(worker_options)

    def run_dedicated_worker(self, **worker_kwargs):
        """Start a Celery worker that only processes rate-limited tasks."""
        logger.info(f"Starting dedicated worker for queue: {self.config.target_queue}")
        return self.run_worker(queues=self.config.target_queue, **worker_kwargs)

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

    def get_worker_info(self):
        """Get information about all workers including queue sizes."""
        return self.worker_inspector.get_worker_queue_sizes()

    def get_worker_queue_summary(self):
        """Get summary of all queues across workers."""
        return self.worker_inspector.get_queue_summary()

    def get_worker_count(self) -> int:
        """Get the number of active workers."""
        return self.worker_inspector.get_worker_count()

    def is_worker_infrastructure_healthy(self) -> bool:
        """Check if worker infrastructure is healthy."""
        return self.worker_inspector.is_healthy()

    def set_task_processor(self, processor: Union[RateLimitedTaskProcessor, Type[RateLimitedTaskProcessor]]):
        """Replace or set the task processor used by this CeleryThrottle instance.

        Accepts either an already-instantiated RateLimitedTaskProcessor or a processor
        class (subclass of RateLimitedTaskProcessor) which will be instantiated using
        the current Celery app, Redis client and queue manager.
        """
        # If a class is provided, instantiate it
        if isinstance(processor, type):
            processor = processor(self.app, self.redis, self.queue_manager, self.config.target_queue)

        # At this point processor is an instance
        self.task_processor = processor

        # Ensure task_submitter / task_dispatcher exist and reference the processor
        if hasattr(self, "task_submitter"):
            self.task_submitter.task_processor = self.task_processor
        else:
            self.task_submitter = RateLimitedTaskSubmitter(self.redis, self.queue_manager, self.task_processor)

        if hasattr(self, "task_dispatcher"):
            self.task_dispatcher.task_processor = self.task_processor
        else:
            self.task_dispatcher = RateLimitedTaskDispatcher(self.redis, self.queue_manager, self.task_processor)

        logger.info("Task processor set and submitter/dispatcher wired to new processor")
