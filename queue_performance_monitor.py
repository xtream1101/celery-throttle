import time
from typing import Dict, Any, List, Tuple
from redis import Redis
from loguru import logger
from dataclasses import dataclass
from universal_queue_router import UniversalQueueRouter


@dataclass
class QueuePerformanceStats:
    """Performance statistics for a queue"""
    queue_name: str
    expected_rate_per_sec: float
    actual_rate_per_sec: float
    total_completed: int
    total_time_span: float
    avg_processing_time: float
    rate_efficiency: float  # actual_rate / expected_rate
    is_underperforming: bool
    backlog_estimate: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'queue_name': self.queue_name,
            'expected_rate_per_sec': self.expected_rate_per_sec,
            'actual_rate_per_sec': self.actual_rate_per_sec,
            'total_completed': self.total_completed,
            'total_time_span': self.total_time_span,
            'avg_processing_time': self.avg_processing_time,
            'rate_efficiency': self.rate_efficiency,
            'is_underperforming': self.is_underperforming,
            'backlog_estimate': self.backlog_estimate
        }


class QueuePerformanceMonitor:
    """Monitor actual queue performance vs expected rates"""

    def __init__(self, redis_connection: Redis, router: UniversalQueueRouter):
        self.redis = redis_connection
        self.router = router
        self.task_completion_key = "queue_performance:completions"
        self.start_time = time.time()

    def record_task_completion(self, queue_name: str, processing_duration: float):
        """Record that a task was completed for performance tracking"""
        completion_time = time.time()
        completion_data = f"{completion_time},{processing_duration}"

        # Store in Redis list for this queue
        queue_key = f"{self.task_completion_key}:{queue_name}"
        self.redis.lpush(queue_key, completion_data)

        # Keep only recent data (last 100 completions)
        self.redis.ltrim(queue_key, 0, 99)

        logger.debug(f"📊 Performance: Recorded completion for {queue_name} (duration: {processing_duration:.2f}s)")

        # Verify the data was stored
        total_completions = self.redis.llen(queue_key)
        logger.debug(f"📊 Performance: Queue {queue_name} now has {total_completions} recorded completions")

    def get_queue_performance(self, queue_name: str, time_window_seconds: int = 300) -> QueuePerformanceStats:
        """Get performance stats for a specific queue"""
        # Get queue configuration
        config = self.router.registry.get_config(queue_name)
        if not config:
            raise ValueError(f"Queue {queue_name} not found")

        expected_rate = 1.0 / config.refill_frequency

        # Get recent completions
        queue_key = f"{self.task_completion_key}:{queue_name}"
        completions_raw = self.redis.lrange(queue_key, 0, -1)

        if not completions_raw:
            return QueuePerformanceStats(
                queue_name=queue_name,
                expected_rate_per_sec=expected_rate,
                actual_rate_per_sec=0.0,
                total_completed=0,
                total_time_span=0.0,
                avg_processing_time=0.0,
                rate_efficiency=0.0,
                is_underperforming=True
            )

        # Parse completion data
        current_time = time.time()
        cutoff_time = current_time - time_window_seconds

        completions = []
        processing_times = []

        for completion_raw in completions_raw:
            try:
                completion_str = completion_raw.decode()
                completion_time_str, processing_duration_str = completion_str.split(',')
                completion_time = float(completion_time_str)
                processing_duration = float(processing_duration_str)

                if completion_time >= cutoff_time:
                    completions.append(completion_time)
                    processing_times.append(processing_duration)
            except:
                continue

        if not completions:
            return QueuePerformanceStats(
                queue_name=queue_name,
                expected_rate_per_sec=expected_rate,
                actual_rate_per_sec=0.0,
                total_completed=0,
                total_time_span=0.0,
                avg_processing_time=0.0,
                rate_efficiency=0.0,
                is_underperforming=True
            )

        # Calculate actual performance
        total_completed = len(completions)
        time_span = max(completions) - min(completions) if len(completions) > 1 else time_window_seconds
        time_span = max(time_span, 1.0)  # Avoid division by zero

        actual_rate = total_completed / time_span
        avg_processing_time = sum(processing_times) / len(processing_times)
        rate_efficiency = actual_rate / expected_rate if expected_rate > 0 else 0

        # Consider underperforming if actual rate is less than 80% of expected
        is_underperforming = rate_efficiency < 0.8

        # Estimate backlog based on rate difference
        rate_deficit = max(0, expected_rate - actual_rate)
        backlog_estimate = int(rate_deficit * 60)  # Estimate tasks backing up per minute

        return QueuePerformanceStats(
            queue_name=queue_name,
            expected_rate_per_sec=expected_rate,
            actual_rate_per_sec=actual_rate,
            total_completed=total_completed,
            total_time_span=time_span,
            avg_processing_time=avg_processing_time,
            rate_efficiency=rate_efficiency,
            is_underperforming=is_underperforming,
            backlog_estimate=backlog_estimate
        )

    def get_all_queue_performance(self, time_window_seconds: int = 300) -> List[QueuePerformanceStats]:
        """Get performance stats for all queues"""
        queue_names = self.router.list_logical_queues()
        stats = []

        for queue_name in queue_names:
            try:
                queue_stats = self.get_queue_performance(queue_name, time_window_seconds)
                stats.append(queue_stats)
            except Exception as e:
                logger.error(f"Error getting performance for {queue_name}: {e}")

        return stats

    def log_performance_report(self, time_window_seconds: int = 300):
        """Log a detailed performance report"""
        logger.info("=" * 70)
        logger.info("📊 QUEUE PERFORMANCE REPORT")
        logger.info("=" * 70)

        # Debug: Check what queues exist
        queue_names = self.router.list_logical_queues()
        logger.info(f"🔍 Debug: Found {len(queue_names)} registered queues: {queue_names}")

        # Debug: Check Redis keys
        pattern = f"{self.task_completion_key}:*"
        redis_keys = self.redis.keys(pattern)
        logger.info(f"🔍 Debug: Found {len(redis_keys)} performance keys in Redis: {[k.decode() for k in redis_keys]}")

        stats = self.get_all_queue_performance(time_window_seconds)

        if not stats:
            logger.info("❌ No queue performance data available yet.")
            logger.info("   This means either:")
            logger.info("   - No tasks have completed yet")
            logger.info("   - Task completion recording is not working")
            logger.info("   - Tasks completed outside the time window")
            return

        logger.info(f"Time window: {time_window_seconds}s")
        logger.info("")

        # Summary stats
        total_underperforming = sum(1 for s in stats if s.is_underperforming)
        total_queues = len(stats)
        avg_efficiency = sum(s.rate_efficiency for s in stats) / len(stats)

        logger.info(f"Overall Summary:")
        logger.info(f"  📈 Total queues: {total_queues}")
        logger.info(f"  ⚠️  Underperforming: {total_underperforming} ({total_underperforming/total_queues*100:.1f}%)")
        logger.info(f"  🎯 Average efficiency: {avg_efficiency:.1%}")
        logger.info("")

        # Individual queue details
        logger.info("Queue Details:")
        logger.info("-" * 70)

        for stat in sorted(stats, key=lambda s: s.rate_efficiency):
            # Status indicators
            if stat.rate_efficiency >= 0.9:
                status = "🟢 Excellent"
            elif stat.rate_efficiency >= 0.8:
                status = "🟡 Good"
            elif stat.rate_efficiency >= 0.5:
                status = "🟠 Slow"
            else:
                status = "🔴 Critical"

            logger.info(f"{status} | {stat.queue_name}")
            logger.info(f"  Expected: {stat.expected_rate_per_sec:.3f}/sec")
            logger.info(f"  Actual:   {stat.actual_rate_per_sec:.3f}/sec")
            logger.info(f"  Efficiency: {stat.rate_efficiency:.1%}")
            logger.info(f"  Completed: {stat.total_completed} tasks")
            logger.info(f"  Avg processing: {stat.avg_processing_time:.1f}s")

            if stat.is_underperforming:
                logger.info(f"  ⚠️  UNDERPERFORMING - Estimated backlog: ~{stat.backlog_estimate} tasks/min")

            logger.info("")

    def get_real_time_status(self) -> Dict[str, Any]:
        """Get real-time status for monitoring dashboards"""
        stats = self.get_all_queue_performance(60)  # Last minute

        return {
            'timestamp': time.time(),
            'monitor_uptime': time.time() - self.start_time,
            'total_queues': len(stats),
            'underperforming_queues': [s.queue_name for s in stats if s.is_underperforming],
            'queue_stats': [s.to_dict() for s in stats],
            'system_health': {
                'avg_efficiency': sum(s.rate_efficiency for s in stats) / len(stats) if stats else 0,
                'worst_performer': min(stats, key=lambda s: s.rate_efficiency).queue_name if stats else None,
                'best_performer': max(stats, key=lambda s: s.rate_efficiency).queue_name if stats else None
            }
        }

    def alert_on_underperformance(self, efficiency_threshold: float = 0.7):
        """Check for underperforming queues and log alerts"""
        stats = self.get_all_queue_performance(120)  # Last 2 minutes

        critical_queues = [s for s in stats if s.rate_efficiency < efficiency_threshold]

        if critical_queues:
            logger.warning("🚨 PERFORMANCE ALERT 🚨")
            for stat in critical_queues:
                logger.warning(f"Queue '{stat.queue_name}' running at {stat.rate_efficiency:.1%} efficiency!")
                logger.warning(f"  Expected: {stat.expected_rate_per_sec:.3f}/sec, Actual: {stat.actual_rate_per_sec:.3f}/sec")

        return critical_queues