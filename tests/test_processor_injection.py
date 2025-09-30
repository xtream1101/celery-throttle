from celery_throttle import CeleryThrottle, RateLimitedTaskProcessor


class DummyProcessor(RateLimitedTaskProcessor):
    """A custom processor for testing injection."""

    def execute_task(self, queue_name: str, task_name: str, args: tuple, kwargs: dict):
        """Override execute_task to demonstrate custom processing."""
        return {"status": "custom_processed", "queue": queue_name, "task": task_name}


def test_constructor_accepts_processor_class_and_wires_components(
    celery_app, redis_client
):
    throttle = CeleryThrottle(
        celery_app=celery_app,
        redis_client=redis_client,
        task_processor_cls=DummyProcessor,
    )

    assert isinstance(throttle.task_processor, DummyProcessor)
    assert throttle.task_submitter.task_processor is throttle.task_processor
    assert throttle.task_dispatcher.task_processor is throttle.task_processor
    assert hasattr(throttle.task_processor, "execute_task")


def test_constructor_accepts_processor_instance_and_wires_components(
    celery_app, redis_client
):
    # Create throttle with default processor first so we have a queue_manager
    throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

    # Instantiate a custom processor using the throttle's queue_manager and inject it
    custom_proc = DummyProcessor(
        celery_app, redis_client, throttle.queue_manager, throttle.config.target_queue
    )
    throttle.set_task_processor(custom_proc)

    assert throttle.task_processor is custom_proc
    assert throttle.task_submitter.task_processor is custom_proc
    assert throttle.task_dispatcher.task_processor is custom_proc


def test_set_task_processor_accepts_class_and_instance_and_replaces(
    celery_app, redis_client
):
    throttle = CeleryThrottle(celery_app=celery_app, redis_client=redis_client)

    # Replace with class
    throttle.set_task_processor(DummyProcessor)
    proc1 = throttle.task_processor
    assert isinstance(proc1, DummyProcessor)

    # Replace with instance
    custom_proc = DummyProcessor(
        celery_app, redis_client, throttle.queue_manager, throttle.config.target_queue
    )
    throttle.set_task_processor(custom_proc)
    assert throttle.task_processor is custom_proc
    assert throttle.task_submitter.task_processor is custom_proc
    assert throttle.task_dispatcher.task_processor is custom_proc
