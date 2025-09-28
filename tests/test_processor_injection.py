import pytest
from celery import Celery

from celery_throttle import CeleryThrottle, RateLimitedTaskProcessor


class DummyRedis:
    """Minimal fake Redis to satisfy TokenBucketRateLimiter's register_script calls."""

    def register_script(self, script):
        def dummy_script(keys=None, args=None, **kwargs):
            # Return a plausible result for acquire/status scripts if called.
            return [1, 1, 0]

        return dummy_script


class DummyProcessor(RateLimitedTaskProcessor):
    def _register_task(self):
        @self.app.task(bind=True, queue=self.target_queue)
        def process_rate_limited_task(task_self, queue_name, task_data):
            return {"status": "ok", "queue": queue_name}

        self.process_rate_limited_task = process_rate_limited_task


def test_constructor_accepts_processor_class_and_wires_components():
    app = Celery("test_app")
    redis_client = DummyRedis()

    throttle = CeleryThrottle(celery_app=app, redis_client=redis_client, task_processor_cls=DummyProcessor)

    assert isinstance(throttle.task_processor, DummyProcessor)
    assert throttle.task_submitter.task_processor is throttle.task_processor
    assert throttle.task_dispatcher.task_processor is throttle.task_processor
    assert hasattr(throttle.task_processor, "process_rate_limited_task")


def test_constructor_accepts_processor_instance_and_wires_components():
    app = Celery("test_app")
    redis_client = DummyRedis()

    # Create throttle with default processor first so we have a queue_manager
    throttle = CeleryThrottle(celery_app=app, redis_client=redis_client)

    # Instantiate a custom processor using the throttle's queue_manager and inject it
    custom_proc = DummyProcessor(app, redis_client, throttle.queue_manager)
    throttle.set_task_processor(custom_proc)

    assert throttle.task_processor is custom_proc
    assert throttle.task_submitter.task_processor is custom_proc
    assert throttle.task_dispatcher.task_processor is custom_proc


def test_set_task_processor_accepts_class_and_instance_and_replaces():
    app = Celery("test_app")
    redis_client = DummyRedis()

    throttle = CeleryThrottle(celery_app=app, redis_client=redis_client)

    # Replace with class
    throttle.set_task_processor(DummyProcessor)
    proc1 = throttle.task_processor
    assert isinstance(proc1, DummyProcessor)

    # Replace with instance
    custom_proc = DummyProcessor(app, redis_client, throttle.queue_manager)
    throttle.set_task_processor(custom_proc)
    assert throttle.task_processor is custom_proc
    assert throttle.task_submitter.task_processor is custom_proc
    assert throttle.task_dispatcher.task_processor is custom_proc
