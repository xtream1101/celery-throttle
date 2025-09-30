import json
import logging
import os
import sys

import click

from ..config import CeleryThrottleConfig
from ..main import CeleryThrottle
from ..queue.manager import UniversalQueueManager

logger = logging.getLogger(__name__)

# Get defaults from config class
_default_config = CeleryThrottleConfig()


def get_queue_manager(config: CeleryThrottleConfig) -> UniversalQueueManager:
    """Create a queue manager for CLI operations that don't need a Celery app."""
    redis_client = config.redis.create_client()
    return UniversalQueueManager(redis_client, config.queue_prefix)


@click.group()
@click.option("--config", type=click.Path(exists=True), help="Configuration file path")
@click.option("--redis-host", default="localhost", help="Redis host")
@click.option("--redis-port", default=6379, help="Redis port")
@click.option("--redis-db", default=0, help="Redis database")
@click.option("--app-name", default=_default_config.app_name, help="Celery app name")
@click.option(
    "--target-queue",
    default=_default_config.target_queue,
    help="Target Celery queue for rate-limited tasks",
)
@click.option(
    "--queue-prefix",
    default=_default_config.queue_prefix,
    help="Redis key prefix for isolation",
)
@click.pass_context
def cli(
    ctx, config, redis_host, redis_port, redis_db, app_name, target_queue, queue_prefix
):
    """Celery Throttle - Advanced rate limiting and queue management for Celery workers."""
    ctx.ensure_object(dict)

    # Load configuration
    if config:
        with open(config, "r") as f:
            config_dict = json.load(f)
        throttle_config = CeleryThrottleConfig.from_dict(config_dict)
    else:
        # Use command line options
        throttle_config = CeleryThrottleConfig(
            redis={"host": redis_host, "port": redis_port, "db": redis_db},
            app_name=app_name,
            target_queue=target_queue,
            queue_prefix=queue_prefix,
        )

    ctx.obj["config"] = throttle_config


def get_celery_app(celery_app: str):
    try:
        # Add current directory to sys.path so local imports can work
        sys.path.append(os.getcwd())
        module_path, app_name = celery_app.split(":")
        import importlib

        module = importlib.import_module(module_path)
        app = getattr(module, app_name)
    except Exception as e:
        click.echo(f"Error importing Celery app '{celery_app}': {e}", err=True)
        sys.exit(1)

    return app


@cli.command()
@click.option(
    "--interval", default=0.1, type=float, help="Dispatch interval in seconds"
)
@click.option(
    "--celery-app",
    required=True,
    help="Celery app module path (e.g., myapp.celery:app)",
)
@click.pass_context
def dispatcher(ctx, interval, celery_app):
    """Start the task dispatcher."""
    config = ctx.obj["config"]

    # Import and get the Celery app
    app = get_celery_app(celery_app)

    throttle = CeleryThrottle(celery_app=app, config=config)

    click.echo(f"Starting task dispatcher (interval={interval}s)")
    click.echo("Press Ctrl+C to stop")
    throttle.run_dispatcher(interval)


@cli.group()
@click.pass_context
def queue(ctx):
    """Queue management commands."""
    pass


@queue.command("create")
@click.argument("rate_limit")
@click.pass_context
def create_queue(ctx, rate_limit):
    """Create a new queue with specified rate limit (e.g., '10/60s')."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    try:
        queue_name = queue_manager.create_queue(rate_limit)
        click.echo(f"Created queue: {queue_name} with rate limit {rate_limit}")
    except Exception as e:
        click.echo(f"Error creating queue: {e}", err=True)
        sys.exit(1)


@queue.command("list")
@click.pass_context
def list_queues(ctx):
    """List all queues."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    queues = queue_manager.list_queues()
    if not queues:
        click.echo("No queues found")
        return

    click.echo(f"{'Queue Name':<25} {'Rate Limit':<12} {'Status':<10} {'Created':<20}")
    click.echo("-" * 70)

    for queue in queues:
        status = "ACTIVE" if queue.active else "INACTIVE"
        created = queue.created_at.strftime("%Y-%m-%d %H:%M:%S")
        click.echo(
            f"{queue.name:<25} {str(queue.rate_limit):<12} {status:<10} {created:<20}"
        )


@queue.command("remove")
@click.argument("queue_name")
@click.pass_context
def remove_queue(ctx, queue_name):
    """Remove a queue."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    if queue_manager.remove_queue(queue_name):
        click.echo(f"Removed queue: {queue_name}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command("show")
@click.argument("queue_name")
@click.pass_context
def show_queue(ctx, queue_name):
    """Show detailed information about a queue."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    queue_config = queue_manager.get_queue_config(queue_name)
    if not queue_config:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)

    stats = queue_manager.get_queue_stats(queue_name)
    rate_status = queue_manager.get_rate_limit_status(queue_name)

    click.echo(f"\nQueue Details: {queue_name}")
    click.echo("=" * 50)
    click.echo(f"Rate Limit: {queue_config.rate_limit}")
    click.echo(f"Status: {'ACTIVE' if queue_config.active else 'INACTIVE'}")
    click.echo(f"Created: {queue_config.created_at.strftime('%Y-%m-%d %H:%M:%S')}")

    if stats:
        click.echo("\nTask Statistics:")
        click.echo(f"  Waiting: {stats.tasks_waiting}")
        click.echo(f"  Processing: {stats.tasks_processing}")
        click.echo(f"  Completed: {stats.tasks_completed}")
        click.echo(f"  Failed: {stats.tasks_failed}")

    if rate_status:
        click.echo("\nRate Limit Status:")
        click.echo(
            f"  Available Tokens: {rate_status['available_tokens']:.2f}/{rate_status['capacity']:.0f}"
        )
        click.echo(f"  Refill Rate: {rate_status['refill_rate']:.4f} tokens/sec")
        if rate_status["next_token_in"] > 0:
            click.echo(f"  Next Token In: {rate_status['next_token_in']:.2f} seconds")
        else:
            click.echo("  Next Token: Available now")


@queue.command("update")
@click.argument("queue_name")
@click.argument("rate_limit")
@click.pass_context
def update_queue(ctx, queue_name, rate_limit):
    """Update rate limit for a queue."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    if queue_manager.update_rate_limit(queue_name, rate_limit):
        click.echo(f"Updated {queue_name} rate limit to {rate_limit}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command("activate")
@click.argument("queue_name")
@click.pass_context
def activate_queue(ctx, queue_name):
    """Activate a queue."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    if queue_manager.activate_queue(queue_name):
        click.echo(f"Activated queue: {queue_name}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command("deactivate")
@click.argument("queue_name")
@click.pass_context
def deactivate_queue(ctx, queue_name):
    """Deactivate a queue."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    if queue_manager.deactivate_queue(queue_name):
        click.echo(f"Deactivated queue: {queue_name}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command("cleanup-all")
@click.pass_context
def cleanup_all_queues(ctx):
    """Remove all queues and their data."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    queues = queue_manager.list_queues()
    if not queues:
        click.echo("No queues to clean up")
        return

    click.echo(f"Found {len(queues)} queues to remove:")
    for queue in queues:
        click.echo(f"  - {queue.name} ({queue.rate_limit})")

    if click.confirm(f"\nAre you sure you want to remove ALL {len(queues)} queues?"):
        removed_count = 0
        for queue in queues:
            if queue_manager.remove_queue(queue.name):
                removed_count += 1
                click.echo(f"  ✓ Removed {queue.name}")
            else:
                click.echo(f"  ✗ Failed to remove {queue.name}")

        click.echo(f"\nCleanup complete: {removed_count}/{len(queues)} queues removed")
    else:
        click.echo("Cleanup cancelled")


@queue.command("cleanup-empty")
@click.pass_context
def cleanup_empty_queues(ctx):
    """Remove queues that have no tasks."""
    config = ctx.obj["config"]
    queue_manager = get_queue_manager(config)

    queues = queue_manager.list_queues()
    if not queues:
        click.echo("No queues found")
        return

    empty_queues = []
    for queue in queues:
        stats = queue_manager.get_queue_stats(queue.name)
        if stats:
            total_tasks = (
                stats.tasks_waiting
                + stats.tasks_processing
                + stats.tasks_completed
                + stats.tasks_failed
            )
            if total_tasks == 0:
                empty_queues.append(queue)

    if not empty_queues:
        click.echo("No empty queues found")
        return

    click.echo(f"Found {len(empty_queues)} empty queues:")
    for queue in empty_queues:
        click.echo(
            f"  - {queue.name} ({queue.rate_limit}) - {'ACTIVE' if queue.active else 'INACTIVE'}"
        )

    if click.confirm(f"\nRemove these {len(empty_queues)} empty queues?"):
        removed_count = 0
        for queue in empty_queues:
            if queue_manager.remove_queue(queue.name):
                removed_count += 1
                click.echo(f"  ✓ Removed {queue.name}")
            else:
                click.echo(f"  ✗ Failed to remove {queue.name}")

        click.echo(
            f"\nCleanup complete: {removed_count}/{len(empty_queues)} empty queues removed"
        )
    else:
        click.echo("Cleanup cancelled")


if __name__ == "__main__":
    cli()
