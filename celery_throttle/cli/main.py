import sys
import os
import json
from datetime import datetime
from typing import Optional

import click
import redis
from loguru import logger

from ..main import CeleryThrottle
from ..config import CeleryThrottleConfig


@click.group()
@click.option('--config', type=click.Path(exists=True), help='Configuration file path')
@click.option('--redis-host', default='localhost', help='Redis host')
@click.option('--redis-port', default=6379, help='Redis port')
@click.option('--redis-db', default=0, help='Redis database')
@click.option('--app-name', default='celery-throttle', help='Celery app name')
@click.option('--target-queue', default='rate_limited_tasks', help='Target Celery queue for rate-limited tasks')
@click.option('--queue-prefix', default='throttle', help='Redis key prefix for isolation')
@click.pass_context
def cli(ctx, config, redis_host, redis_port, redis_db, app_name, target_queue, queue_prefix):
    """Celery Throttle - Advanced rate limiting and queue management for Celery workers."""
    ctx.ensure_object(dict)

    # Load configuration
    if config:
        with open(config, 'r') as f:
            config_dict = json.load(f)
        throttle_config = CeleryThrottleConfig.from_dict(config_dict)
    else:
        # Use command line options
        throttle_config = CeleryThrottleConfig(
            redis={
                "host": redis_host,
                "port": redis_port,
                "db": redis_db
            },
            app_name=app_name,
            target_queue=target_queue,
            queue_prefix=queue_prefix
        )

    ctx.obj['config'] = throttle_config


@cli.command()
@click.option('--concurrency', default=1, help='Number of worker processes')
@click.option('--loglevel', default='info', help='Logging level')
@click.option('--prefetch-multiplier', default=1, help='Task prefetch multiplier, keep at 1 for strict rate limiting')
@click.option('--queues', help='Comma-separated list of queues to listen to')
@click.pass_context
def worker(ctx, concurrency, loglevel, prefetch_multiplier, queues):
    """Start a Celery worker with rate limiting."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    queue_list = None
    if queues:
        queue_list = [q.strip() for q in queues.split(',')]
        click.echo(f"Starting Celery worker (queues={queue_list}, concurrency={concurrency}, loglevel={loglevel})")
    else:
        click.echo(f"Starting Celery worker (all queues, concurrency={concurrency}, loglevel={loglevel})")

    throttle.run_worker(
        queues=queue_list,
        concurrency=concurrency,
        loglevel=loglevel,
        prefetch_multiplier=prefetch_multiplier
    )


@cli.command('dedicated-worker')
@click.option('--concurrency', default=1, help='Number of worker processes')
@click.option('--loglevel', default='info', help='Logging level')
@click.option('--prefetch-multiplier', default=1, help='Task prefetch multiplier, keep at 1 for strict rate limiting')
@click.pass_context
def dedicated_worker(ctx, concurrency, loglevel, prefetch_multiplier):
    """Start a dedicated worker that only processes rate-limited tasks."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    click.echo(f"Starting dedicated worker for queue '{config.target_queue}' (concurrency={concurrency}, loglevel={loglevel})")
    throttle.run_dedicated_worker(
        concurrency=concurrency,
        loglevel=loglevel,
        prefetch_multiplier=prefetch_multiplier
    )


@cli.command()
@click.option('--interval', default=0.5, type=float, help='Dispatch interval in seconds')
@click.pass_context
def dispatcher(ctx, interval):
    """Start the task dispatcher."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    click.echo(f"Starting task dispatcher (interval={interval}s)")
    click.echo("Press Ctrl+C to stop")
    throttle.run_dispatcher(interval)


@cli.group()
@click.pass_context
def queue(ctx):
    """Queue management commands."""
    pass


@queue.command('create')
@click.argument('rate_limit')
@click.pass_context
def create_queue(ctx, rate_limit):
    """Create a new queue with specified rate limit (e.g., '10/60s')."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    try:
        queue_name = throttle.create_queue(rate_limit)
        click.echo(f"Created queue: {queue_name} with rate limit {rate_limit}")
    except Exception as e:
        click.echo(f"Error creating queue: {e}", err=True)
        sys.exit(1)


@queue.command('list')
@click.pass_context
def list_queues(ctx):
    """List all queues."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    queues = throttle.list_queues()
    if not queues:
        click.echo("No queues found")
        return

    click.echo(f"{'Queue Name':<25} {'Rate Limit':<12} {'Status':<10} {'Created':<20}")
    click.echo("-" * 70)

    for queue in queues:
        status = "ACTIVE" if queue.active else "INACTIVE"
        created = queue.created_at.strftime('%Y-%m-%d %H:%M:%S')
        click.echo(f"{queue.name:<25} {str(queue.rate_limit):<12} {status:<10} {created:<20}")


@queue.command('remove')
@click.argument('queue_name')
@click.pass_context
def remove_queue(ctx, queue_name):
    """Remove a queue."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    if throttle.remove_queue(queue_name):
        click.echo(f"Removed queue: {queue_name}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command('show')
@click.argument('queue_name')
@click.pass_context
def show_queue(ctx, queue_name):
    """Show detailed information about a queue."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    queue_config = throttle.queue_manager.get_queue_config(queue_name)
    if not queue_config:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)

    stats = throttle.get_queue_stats(queue_name)
    rate_status = throttle.get_rate_limit_status(queue_name)

    click.echo(f"\nQueue Details: {queue_name}")
    click.echo("=" * 50)
    click.echo(f"Rate Limit: {queue_config.rate_limit}")
    click.echo(f"Status: {'ACTIVE' if queue_config.active else 'INACTIVE'}")
    click.echo(f"Created: {queue_config.created_at.strftime('%Y-%m-%d %H:%M:%S')}")

    if stats:
        click.echo(f"\nTask Statistics:")
        click.echo(f"  Waiting: {stats.tasks_waiting}")
        click.echo(f"  Processing: {stats.tasks_processing}")
        click.echo(f"  Completed: {stats.tasks_completed}")
        click.echo(f"  Failed: {stats.tasks_failed}")

    if rate_status:
        click.echo(f"\nRate Limit Status:")
        click.echo(f"  Available Tokens: {rate_status['available_tokens']:.2f}/{rate_status['capacity']:.0f}")
        click.echo(f"  Refill Rate: {rate_status['refill_rate']:.4f} tokens/sec")
        if rate_status['next_token_in'] > 0:
            click.echo(f"  Next Token In: {rate_status['next_token_in']:.2f} seconds")
        else:
            click.echo(f"  Next Token: Available now")


@queue.command('test')
@click.argument('queue_name')
@click.argument('count', type=int)
@click.pass_context
def test_queue(ctx, queue_name, count):
    """Submit test tasks to a queue."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    if not throttle.queue_manager.queue_exists(queue_name):
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)

    click.echo(f"Submitting {count} test tasks to {queue_name}...")

    submitted = 0
    queued = 0

    for i in range(count):
        task_data = {
            "task_id": i + 1,
            "message": f"Test task {i + 1}",
            "timestamp": datetime.now().isoformat()
        }

        if throttle.submit_task(queue_name, task_data):
            submitted += 1
        else:
            queued += 1

    click.echo(f"Results: {submitted} submitted immediately, {queued} queued for later")


@queue.command('update')
@click.argument('queue_name')
@click.argument('rate_limit')
@click.pass_context
def update_queue(ctx, queue_name, rate_limit):
    """Update rate limit for a queue."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    if throttle.update_rate_limit(queue_name, rate_limit):
        click.echo(f"Updated {queue_name} rate limit to {rate_limit}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command('activate')
@click.argument('queue_name')
@click.pass_context
def activate_queue(ctx, queue_name):
    """Activate a queue."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    if throttle.activate_queue(queue_name):
        click.echo(f"Activated queue: {queue_name}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command('deactivate')
@click.argument('queue_name')
@click.pass_context
def deactivate_queue(ctx, queue_name):
    """Deactivate a queue."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    if throttle.deactivate_queue(queue_name):
        click.echo(f"Deactivated queue: {queue_name}")
    else:
        click.echo(f"Queue {queue_name} not found", err=True)
        sys.exit(1)


@queue.command('cleanup-all')
@click.pass_context
def cleanup_all_queues(ctx):
    """Remove all queues and their data."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    queues = throttle.list_queues()
    if not queues:
        click.echo("No queues to clean up")
        return

    click.echo(f"Found {len(queues)} queues to remove:")
    for queue in queues:
        click.echo(f"  - {queue.name} ({queue.rate_limit})")

    if click.confirm(f"\nAre you sure you want to remove ALL {len(queues)} queues?"):
        removed_count = 0
        for queue in queues:
            if throttle.remove_queue(queue.name):
                removed_count += 1
                click.echo(f"  ✓ Removed {queue.name}")
            else:
                click.echo(f"  ✗ Failed to remove {queue.name}")

        click.echo(f"\nCleanup complete: {removed_count}/{len(queues)} queues removed")
    else:
        click.echo("Cleanup cancelled")


@queue.command('cleanup-empty')
@click.pass_context
def cleanup_empty_queues(ctx):
    """Remove queues that have no tasks."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    queues = throttle.list_queues()
    if not queues:
        click.echo("No queues found")
        return

    empty_queues = []
    for queue in queues:
        stats = throttle.get_queue_stats(queue.name)
        if stats:
            total_tasks = (stats.tasks_waiting + stats.tasks_processing +
                         stats.tasks_completed + stats.tasks_failed)
            if total_tasks == 0:
                empty_queues.append(queue)

    if not empty_queues:
        click.echo("No empty queues found")
        return

    click.echo(f"Found {len(empty_queues)} empty queues:")
    for queue in empty_queues:
        click.echo(f"  - {queue.name} ({queue.rate_limit}) - {'ACTIVE' if queue.active else 'INACTIVE'}")

    if click.confirm(f"\nRemove these {len(empty_queues)} empty queues?"):
        removed_count = 0
        for queue in empty_queues:
            if throttle.remove_queue(queue.name):
                removed_count += 1
                click.echo(f"  ✓ Removed {queue.name}")
            else:
                click.echo(f"  ✗ Failed to remove {queue.name}")

        click.echo(f"\nCleanup complete: {removed_count}/{len(empty_queues)} empty queues removed")
    else:
        click.echo("Cleanup cancelled")


@cli.command('worker-status')
@click.option('--detailed', '-d', is_flag=True, help='Show detailed worker queue information')
@click.pass_context
def worker_status(ctx, detailed):
    """Show worker information and queue sizes."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    worker_info = throttle.get_worker_info()
    worker_count = throttle.get_worker_count()
    is_healthy = throttle.is_worker_infrastructure_healthy()

    click.echo(f"Worker Infrastructure Status: {'HEALTHY' if is_healthy else 'UNHEALTHY'}")
    click.echo(f"Active Workers: {worker_count}")
    click.echo()

    if not worker_info:
        click.echo("No workers found or workers not responding")
        return

    click.echo(f"{'Worker Name':<30} {'Status':<8} {'Active':<8} {'Reserved':<10} {'Total Tasks':<12}")
    click.echo("-" * 75)

    for worker_name, info in worker_info.items():
        click.echo(f"{worker_name:<30} {info.status:<8} {info.active_tasks:<8} {info.reserved_tasks:<10} {info.total_tasks:<12}")

        if detailed and info.queue_sizes:
            click.echo("  Queues:")
            for queue_name, size in info.queue_sizes.items():
                click.echo(f"    {queue_name}: {size} tasks")
        elif not detailed and info.queue_sizes:
            total_queue_tasks = sum(info.queue_sizes.values())
            click.echo(f"  Total queued tasks: {total_queue_tasks}")
        click.echo()

    # Show queue summary
    if detailed:
        worker_queue_summary = throttle.get_worker_queue_summary()
        if worker_queue_summary:
            click.echo("Worker Queue Summary:")
            click.echo(f"{'Queue Name':<25} {'Total Size':<12} {'Workers':<8} {'Worker Names'}")
            click.echo("-" * 75)

            for queue_name, queue_info in worker_queue_summary.items():
                workers_text = ", ".join(queue_info.workers_listening[:2])
                if len(queue_info.workers_listening) > 2:
                    workers_text += f" (+{len(queue_info.workers_listening) - 2})"

                click.echo(f"{queue_name[:24]:<25} {queue_info.total_size:<12} {len(queue_info.workers_listening):<8} {workers_text}")


@cli.command()
@click.option('--interval', default=1.0, type=float, help='Refresh interval in seconds')
@click.pass_context
def monitor(ctx, interval):
    """Real-time monitoring of queues and system status."""
    config = ctx.obj['config']
    throttle = CeleryThrottle(config=config)

    import time

    click.echo("Starting real-time monitor...")
    click.echo("Press Ctrl+C to stop")

    try:
        while True:
            # Clear screen
            click.clear()

            # Show header
            click.echo("=" * 100)
            click.echo(f"Celery Throttle Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            click.echo("=" * 100)

            # Show worker status
            worker_count = throttle.get_worker_count()
            is_healthy = throttle.is_worker_infrastructure_healthy()
            status_text = "HEALTHY" if is_healthy else "UNHEALTHY"
            click.echo(f"Workers: {worker_count} active | Status: {status_text}")
            click.echo()

            queues = throttle.list_queues()
            if not queues:
                click.echo("No throttle queues found")
            else:
                click.echo(f"{'Throttle Queue':<20} {'Rate Limit':<12} {'Status':<8} {'Waiting':<8} {'Processing':<10} {'Completed':<10} {'Failed':<8}")
                click.echo("-" * 100)

                for queue in queues:
                    stats = throttle.get_queue_stats(queue.name)
                    status = "ACTIVE" if queue.active else "INACTIVE"

                    if stats:
                        waiting = stats.tasks_waiting
                        processing = stats.tasks_processing
                        completed = stats.tasks_completed
                        failed = stats.tasks_failed
                    else:
                        waiting = processing = completed = failed = 0

                    queue_name = queue.name[:19] + "..." if len(queue.name) > 22 else queue.name
                    click.echo(f"{queue_name:<20} {str(queue.rate_limit):<12} {status:<8} "
                             f"{waiting:<8} {processing:<10} {completed:<10} {failed:<8}")

            # Show worker queue summary
            try:
                worker_queue_summary = throttle.get_worker_queue_summary()
                if worker_queue_summary:
                    click.echo(f"\n{'Worker Queue':<20} {'Total Size':<12} {'Workers Listening':<30}")
                    click.echo("-" * 100)

                    for queue_name, queue_info in worker_queue_summary.items():
                        workers_text = ", ".join(queue_info.workers_listening[:3])  # Show first 3 workers
                        if len(queue_info.workers_listening) > 3:
                            workers_text += f" (+{len(queue_info.workers_listening) - 3} more)"

                        queue_display = queue_name[:19] + "..." if len(queue_name) > 22 else queue_name
                        click.echo(f"{queue_display:<20} {queue_info.total_size:<12} {workers_text[:29]:<30}")
            except Exception as e:
                click.echo(f"\nWorker queue info unavailable: {str(e)[:50]}...")

            click.echo(f"\nRefresh every {interval}s | Press Ctrl+C to stop")
            time.sleep(interval)

    except KeyboardInterrupt:
        click.echo("\nMonitor stopped")


if __name__ == '__main__':
    cli()
