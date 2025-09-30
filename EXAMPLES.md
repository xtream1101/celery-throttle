# Examples

Comprehensive examples for using Celery Throttle in various scenarios.

## Table of Contents

- [Basic Examples](#basic-examples)
- [API Rate Limiting](#api-rate-limiting)
- [Email & Notification Services](#email--notification-services)
- [Batch Processing](#batch-processing)
- [Multi-Service Architecture](#multi-service-architecture)
- [Advanced Patterns](#advanced-patterns)
- [Production Deployment](#production-deployment)

## Basic Examples

### Simple Rate Limiting

The most basic use case - limit task execution rate:

```python
from celery import Celery
from celery_throttle import CeleryThrottle

# Your existing Celery app
app = Celery('myapp', broker='redis://localhost:6379/0')

# Your existing task
@app.task
def process_item(item_id, data):
    print(f"Processing item {item_id}")
    return {"status": "processed", "item_id": item_id}

# Add rate limiting
throttle = CeleryThrottle(celery_app=app)

# Create a queue: 10 tasks per minute
queue_name = throttle.create_queue("10/1m", "processing_queue")

# Submit tasks
for i in range(100):
    throttle.submit_task("processing_queue", "myapp.process_item", i, {"value": i * 10})

print("Tasks submitted - will process at 10 per minute")
```

### Named Queues

Use meaningful queue names instead of auto-generated ones:

```python
from celery_throttle import CeleryThrottle

throttle = CeleryThrottle(celery_app=app)

# Create named queues for different purposes
throttle.create_queue("50/1m", "email_queue")
throttle.create_queue("100/1h", "api_queue")
throttle.create_queue("10/30s", "webhook_queue")

# Easy to understand which queue is which
throttle.submit_task("email_queue", "myapp.send_email", "user@example.com", "Subject", "Body")
throttle.submit_task("api_queue", "myapp.call_api", "/endpoint", {"data": "value"})
throttle.submit_task("webhook_queue", "myapp.trigger_webhook", "https://example.com/hook")
```

### Burst Allowance

Handle traffic spikes with burst tokens:

```python
# Without burst - smooth distribution
throttle.create_queue("60/1m", "smooth_queue")  # 1 task per second

# With burst - allow spikes
throttle.create_queue("60/1m:10", "burst_queue")  # 1/sec avg, but 10 can execute immediately

# Submit 20 tasks
for i in range(20):
    throttle.submit_task("burst_queue", "myapp.process", i)

# First 10 execute immediately (burst tokens)
# Remaining 10 execute at 1 per second
```

## API Rate Limiting

### Social Media APIs

Different social media platforms have different rate limits:

```python
from celery import Celery
from celery_throttle import CeleryThrottle

app = Celery('social_media', broker='redis://localhost:6379/0')

@app.task
def post_to_twitter(message):
    # Twitter API call
    return {"platform": "twitter", "status": "posted"}

@app.task
def post_to_facebook(message):
    # Facebook API call
    return {"platform": "facebook", "status": "posted"}

@app.task
def post_to_instagram(message):
    # Instagram API call
    return {"platform": "instagram", "status": "posted"}

# Set up rate limiting
throttle = CeleryThrottle(celery_app=app)

# Twitter: 300 requests per 15 minutes (per endpoint)
throttle.create_queue("300/15m", "twitter_api")

# Facebook: 200 requests per hour (per user)
throttle.create_queue("200/1h", "facebook_api")

# Instagram: 200 requests per hour
throttle.create_queue("200/1h", "instagram_api")

# Post to all platforms
message = "Check out our new product!"
throttle.submit_task("twitter_api", "social_media.post_to_twitter", message)
throttle.submit_task("facebook_api", "social_media.post_to_facebook", message)
throttle.submit_task("instagram_api", "social_media.post_to_instagram", message)
```

### Third-Party API Integration

Respect external API rate limits:

```python
from celery import Celery
from celery_throttle import CeleryThrottle

app = Celery('api_client', broker='redis://localhost:6379/0')

@app.task
def call_stripe_api(endpoint, data):
    # Stripe API call
    return {"service": "stripe", "endpoint": endpoint}

@app.task
def call_github_api(endpoint, data):
    # GitHub API call
    return {"service": "github", "endpoint": endpoint}

@app.task
def call_slack_api(endpoint, data):
    # Slack API call
    return {"service": "slack", "endpoint": endpoint}

throttle = CeleryThrottle(celery_app=app)

# Stripe: 100 requests per second (burst), avg 25/sec
throttle.create_queue("25/1s:100", "stripe_api")

# GitHub: 5000 requests per hour
throttle.create_queue("5000/1h", "github_api")

# Slack: 1 request per second (strict)
throttle.create_queue("1/1s", "slack_api")

# Use the APIs
throttle.submit_task("stripe_api", "api_client.call_stripe_api", "/charges", {"amount": 1000})
throttle.submit_task("github_api", "api_client.call_github_api", "/repos/user/repo/issues", {})
throttle.submit_task("slack_api", "api_client.call_slack_api", "/chat.postMessage", {"text": "Hello"})
```

## Email & Notification Services

### Email Service with Multiple Providers

Rate limit different email providers separately:

```python
from celery import Celery
from celery_throttle import CeleryThrottle

app = Celery('email_service', broker='redis://localhost:6379/0')

@app.task
def send_via_sendgrid(to_email, subject, body):
    # SendGrid API call
    return {"provider": "sendgrid", "to": to_email}

@app.task
def send_via_mailgun(to_email, subject, body):
    # Mailgun API call
    return {"provider": "mailgun", "to": to_email}

@app.task
def send_via_ses(to_email, subject, body):
    # AWS SES API call
    return {"provider": "ses", "to": to_email}

throttle = CeleryThrottle(celery_app=app, queue_prefix="email")

# Different limits for different providers
throttle.create_queue("100/1s", "sendgrid")      # SendGrid: 100/sec
throttle.create_queue("300/1m", "mailgun")       # Mailgun: 300/min
throttle.create_queue("14/1s", "ses")            # AWS SES: 14/sec

# Send emails
emails = [
    ("user1@example.com", "Welcome", "Welcome to our service!"),
    ("user2@example.com", "Update", "Your account has been updated."),
    # ... more emails
]

for to_email, subject, body in emails:
    # Use round-robin or logic to select provider
    throttle.submit_task("sendgrid", "email_service.send_via_sendgrid", to_email, subject, body)
```

### Notification Service with Priority Levels

Different rate limits for different priority notifications:

```python
from celery import Celery
from celery_throttle import CeleryThrottle

app = Celery('notifications', broker='redis://localhost:6379/0')

@app.task
def send_notification(user_id, title, message, priority):
    # Send notification via push, email, SMS, etc.
    return {"user_id": user_id, "priority": priority}

throttle = CeleryThrottle(celery_app=app)

# High priority: more frequent
throttle.create_queue("100/1m", "notifications_high")

# Medium priority: moderate
throttle.create_queue("50/1m", "notifications_medium")

# Low priority: less frequent
throttle.create_queue("20/1m", "notifications_low")

# Send notifications based on priority
def send_by_priority(user_id, title, message, priority):
    queue_map = {
        "high": "notifications_high",
        "medium": "notifications_medium",
        "low": "notifications_low"
    }
    queue = queue_map.get(priority, "notifications_low")
    throttle.submit_task(queue, "notifications.send_notification", user_id, title, message, priority)

# Usage
send_by_priority(123, "Alert", "Critical system issue", "high")
send_by_priority(456, "Info", "Weekly summary ready", "low")
```

## Batch Processing

### Data Processing Pipeline

Process large datasets with rate limiting:

```python
from celery import Celery
from celery_throttle import CeleryThrottle
import pandas as pd

app = Celery('data_pipeline', broker='redis://localhost:6379/0')

@app.task
def process_record(record_id, data):
    # Process individual record
    # This might call external APIs, databases, etc.
    return {"record_id": record_id, "processed": True}

@app.task
def enrich_data(record_id, external_data):
    # Enrich with external data sources
    return {"record_id": record_id, "enriched": True}

throttle = CeleryThrottle(celery_app=app)

# Processing: 1000 records per 5 minutes
throttle.create_queue("1000/5m", "processing")

# Enrichment: 100 per minute (external API limit)
throttle.create_queue("100/1m", "enrichment")

# Load large dataset
df = pd.read_csv("large_dataset.csv")

# Submit all records for processing
tasks_submitted = []
for idx, row in df.iterrows():
    result = throttle.submit_task(
        "processing",
        "data_pipeline.process_record",
        row['id'],
        row.to_dict()
    )
    tasks_submitted.append(result)

print(f"Submitted {len(tasks_submitted)} tasks for processing")

# Monitor progress
import time
while True:
    stats = throttle.get_queue_stats("processing")
    print(f"Waiting: {stats.tasks_waiting}, Completed: {stats.tasks_completed}")
    if stats.tasks_waiting == 0:
        print("All tasks processed!")
        break
    time.sleep(10)
```

### ETL Pipeline

Extract, Transform, Load with rate limiting:

```python
from celery import Celery
from celery_throttle import CeleryThrottle

app = Celery('etl', broker='redis://localhost:6379/0')

@app.task
def extract_data(source_id):
    # Extract from source
    return {"source_id": source_id, "data": [...]}

@app.task
def transform_data(data):
    # Transform data
    return {"transformed": True, "data": [...]}

@app.task
def load_data(data, destination):
    # Load to destination
    return {"loaded": True, "destination": destination}

throttle = CeleryThrottle(celery_app=app)

# Rate limits for each stage
throttle.create_queue("50/1m", "extract")     # Source API limit
throttle.create_queue("200/1m", "transform")  # CPU-bound, can be faster
throttle.create_queue("30/1m", "load")        # Destination DB limit

# Submit tasks for multiple batches
for batch_id in range(100):
    # Chain tasks: extract -> transform -> load
    throttle.submit_task("extract", "etl.extract_data", batch_id)
```

### File Processing

Process files at a controlled rate:

```python
from celery import Celery
from celery_throttle import CeleryThrottle
import os

app = Celery('file_processor', broker='redis://localhost:6379/0')

@app.task
def process_image(file_path):
    # Resize, optimize, upload, etc.
    return {"file": file_path, "processed": True}

@app.task
def process_video(file_path):
    # Transcode, thumbnail, upload, etc.
    return {"file": file_path, "processed": True}

throttle = CeleryThrottle(celery_app=app)

# Images: 20 per minute (CPU intensive)
throttle.create_queue("20/1m", "image_processing")

# Videos: 5 per minute (very CPU intensive)
throttle.create_queue("5/1m", "video_processing")

# Process all files in upload directory
upload_dir = "/uploads"
for filename in os.listdir(upload_dir):
    file_path = os.path.join(upload_dir, filename)

    if filename.endswith(('.jpg', '.png', '.gif')):
        throttle.submit_task("image_processing", "file_processor.process_image", file_path)
    elif filename.endswith(('.mp4', '.mov', '.avi')):
        throttle.submit_task("video_processing", "file_processor.process_video", file_path)
```

## Multi-Service Architecture

### Microservices with Shared Celery

Multiple services using the same Celery broker but isolated rate limiting:

```python
from celery import Celery
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig

# Shared Celery app
app = Celery('microservices', broker='redis://localhost:6379/0')

# Service A: User Service
@app.task
def create_user(email, name):
    return {"service": "user", "action": "create", "email": email}

@app.task
def send_welcome_email(email):
    return {"service": "user", "action": "email", "email": email}

# Service A throttle (isolated with prefix)
user_config = CeleryThrottleConfig(
    app_name="user-service",
    target_queue="user-rate-limited",
    queue_prefix="user"
)
user_throttle = CeleryThrottle(celery_app=app, config=user_config)
user_throttle.create_queue("50/1m", "user_operations")
user_throttle.create_queue("30/1m", "welcome_emails")

# Service B: Payment Service
@app.task
def process_payment(user_id, amount):
    return {"service": "payment", "action": "process", "user_id": user_id}

@app.task
def send_receipt(user_id, payment_id):
    return {"service": "payment", "action": "receipt", "user_id": user_id}

# Service B throttle (isolated with different prefix)
payment_config = CeleryThrottleConfig(
    app_name="payment-service",
    target_queue="payment-rate-limited",
    queue_prefix="payment"
)
payment_throttle = CeleryThrottle(celery_app=app, config=payment_config)
payment_throttle.create_queue("20/1m", "payment_processing")
payment_throttle.create_queue("40/1m", "receipt_emails")

# Use services independently
user_throttle.submit_task("user_operations", "microservices.create_user", "user@example.com", "John")
user_throttle.submit_task("welcome_emails", "microservices.send_welcome_email", "user@example.com")

payment_throttle.submit_task("payment_processing", "microservices.process_payment", 123, 50.00)
payment_throttle.submit_task("receipt_emails", "microservices.send_receipt", 123, "pay_123")
```

### Multi-Tenant Application

Isolate rate limits per tenant:

```python
from celery import Celery
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig

app = Celery('saas_app', broker='redis://localhost:6379/0')

@app.task
def process_tenant_data(tenant_id, data):
    return {"tenant_id": tenant_id, "processed": True}

class TenantThrottleManager:
    def __init__(self, celery_app):
        self.celery_app = celery_app
        self.throttles = {}

    def get_throttle(self, tenant_id: str):
        if tenant_id not in self.throttles:
            config = CeleryThrottleConfig(
                app_name=f"tenant-{tenant_id}",
                target_queue=f"tenant-{tenant_id}-rate-limited",
                queue_prefix=f"tenant_{tenant_id}"
            )
            throttle = CeleryThrottle(celery_app=self.celery_app, config=config)

            # Create queues for this tenant
            throttle.create_queue("100/1m", "processing")
            throttle.create_queue("50/1m", "notifications")

            self.throttles[tenant_id] = throttle

        return self.throttles[tenant_id]

# Usage
manager = TenantThrottleManager(app)

# Each tenant gets isolated rate limiting
tenant_a_throttle = manager.get_throttle("tenant_a")
tenant_b_throttle = manager.get_throttle("tenant_b")

# Submit tasks per tenant
tenant_a_throttle.submit_task("processing", "saas_app.process_tenant_data", "tenant_a", {"data": "..."})
tenant_b_throttle.submit_task("processing", "saas_app.process_tenant_data", "tenant_b", {"data": "..."})

# Tenants don't affect each other's rate limits
```

## Advanced Patterns

### Dynamic Rate Limit Adjustment

Adjust rate limits based on conditions:

```python
from celery_throttle import CeleryThrottle
import datetime

throttle = CeleryThrottle(celery_app=app)

# Create initial queue
throttle.create_queue("100/1m", "api_calls")

# Monitor and adjust
def adjust_rate_limit_based_on_time():
    current_hour = datetime.datetime.now().hour

    if 9 <= current_hour < 17:  # Business hours
        # Higher rate during business hours
        throttle.update_rate_limit("api_calls", "200/1m")
    else:  # Off hours
        # Lower rate during off hours
        throttle.update_rate_limit("api_calls", "50/1m")

# Run this periodically
```

### Queue Activation/Deactivation

Pause and resume queues dynamically:

```python
from celery_throttle import CeleryThrottle

throttle = CeleryThrottle(celery_app=app)
throttle.create_queue("50/1m", "maintenance_tasks")

# During maintenance window
print("Starting maintenance...")
throttle.deactivate_queue("maintenance_tasks")

# ... perform maintenance ...

# After maintenance
print("Maintenance complete, resuming tasks")
throttle.activate_queue("maintenance_tasks")
```

### Task Retry with Different Queues

Use different rate limits for retries:

```python
from celery import Celery
from celery_throttle import CeleryThrottle

app = Celery('retry_example', broker='redis://localhost:6379/0')

@app.task(bind=True, max_retries=3)
def flaky_api_call(self, data):
    try:
        # API call that might fail
        return {"status": "success"}
    except Exception as exc:
        # Retry with exponential backoff using slower queue
        raise self.retry(exc=exc, countdown=60)

throttle = CeleryThrottle(celery_app=app)

# Normal attempts: faster rate
throttle.create_queue("100/1m", "api_normal")

# Retries: slower rate to avoid overwhelming failing service
throttle.create_queue("10/1m", "api_retry")

# Submit initial attempt
throttle.submit_task("api_normal", "retry_example.flaky_api_call", {"data": "..."})
```

## Production Deployment

### Complete Production Setup

Full production-ready configuration:

```python
# production_app.py
import os
from celery import Celery
from celery_throttle import CeleryThrottle
from celery_throttle.config import CeleryThrottleConfig, RedisConfig
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Celery app
app = Celery('production_app')
app.config_from_object({
    'broker_url': os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    'result_backend': os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/1'),
    'task_serializer': 'json',
    'accept_content': ['json'],
    'result_serializer': 'json',
    'timezone': 'UTC',
    'enable_utc': True,
})

# Define tasks
@app.task
def send_email(to_email, subject, body):
    # Email sending logic
    return {"status": "sent", "to": to_email}

@app.task
def call_external_api(endpoint, data):
    # API calling logic
    return {"status": "success", "endpoint": endpoint}

# Production throttle configuration
prod_config = CeleryThrottleConfig(
    app_name=os.getenv('APP_NAME', 'production-app'),
    target_queue=os.getenv('RATE_LIMITED_QUEUE', 'rate-limited-tasks'),
    queue_prefix=os.getenv('QUEUE_PREFIX', 'prod'),
    redis=RedisConfig(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        db=int(os.getenv('REDIS_DB', 0)),
        password=os.getenv('REDIS_PASSWORD')
    )
)

# Initialize throttle
throttle = CeleryThrottle(celery_app=app, config=prod_config)

# Create queues
throttle.create_queue("100/1m", "email_queue")
throttle.create_queue("200/1h", "api_queue")

if __name__ == '__main__':
    # For development only
    app.start()
```

**Docker Compose:**

```yaml
# docker-compose.yml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  celery-worker:
    build: .
    command: celery -A production_app worker --loglevel=info --concurrency=1 --prefetch-multiplier=1
    environment:
      - CELERY_BROKER_URL=redis://redis:6379/0
      - CELERY_RESULT_BACKEND=redis://redis:6379/1
      - REDIS_HOST=redis
      - REDIS_PORT=6379
      - APP_NAME=production-app
      - QUEUE_PREFIX=prod
    depends_on:
      - redis

  celery-throttle-dispatcher:
    build: .
    command: celery-throttle dispatcher --celery-app=production_app:app
    environment:
      - CELERY_THROTTLE_REDIS_HOST=redis
      - CELERY_THROTTLE_REDIS_PORT=6379
      - CELERY_THROTTLE_QUEUE_PREFIX=prod
    depends_on:
      - redis
```

### Monitoring and Alerting

Monitor queue statistics in production:

```python
# monitoring.py
from celery_throttle import CeleryThrottle
import time
import logging

logger = logging.getLogger(__name__)

def monitor_queues(throttle: CeleryThrottle, alert_threshold: int = 1000):
    """Monitor queue statistics and alert if thresholds exceeded."""

    while True:
        for queue in throttle.list_queues():
            stats = throttle.get_queue_stats(queue.name)

            # Log statistics
            logger.info(
                f"Queue {queue.name}: "
                f"Waiting={stats.tasks_waiting}, "
                f"Processing={stats.tasks_processing}, "
                f"Completed={stats.tasks_completed}, "
                f"Failed={stats.tasks_failed}"
            )

            # Alert if queue is backing up
            if stats.tasks_waiting > alert_threshold:
                logger.warning(
                    f"ALERT: Queue {queue.name} has {stats.tasks_waiting} waiting tasks "
                    f"(threshold: {alert_threshold})"
                )
                # Send alert to monitoring system (PagerDuty, Slack, etc.)

            # Check rate limit status
            rate_status = throttle.get_rate_limit_status(queue.name)
            if rate_status:
                logger.debug(
                    f"Queue {queue.name} rate limit: "
                    f"{rate_status['available_tokens']:.2f}/{rate_status['capacity']:.0f} tokens"
                )

        time.sleep(60)  # Check every minute

# Usage
if __name__ == '__main__':
    throttle = CeleryThrottle(celery_app=app)
    monitor_queues(throttle, alert_threshold=1000)
```

## See Also

- [Main README](README.md) - Getting started guide
- [Configuration Guide](CONFIGURATION.md) - Detailed configuration options