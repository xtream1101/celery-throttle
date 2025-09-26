import time
import requests
from redis import Redis
import random
from celery import Celery
from loguru import logger
from limiters import SyncTokenBucket

# Create Celery app
celery_app = Celery('main', broker='redis://localhost:6379', backend='redis://localhost:6379')

# set the CELERY_WORKER_PREFETCH_MULTIPLIER using celery_app config
celery_app.conf.worker_prefetch_multiplier = 1

def get_limiter(name, frequency=1, amount=1):
    return SyncTokenBucket(
        name=name,
        capacity=1,
        refill_frequency=1 if name != "foo" else 0.5,
        refill_amount=1,
        # max_sleep=5,
        connection=Redis.from_url("redis://localhost:6379"),
    )

@celery_app.task
def do_action(item):
    # Extract queue name from the item data to create appropriate limiter
    queue_name = item.get("queue", "default")
    limiter = get_limiter(queue_name)

    # print(f"\nStart time for {item}: {time.time()}")
    st = time.time()
    with limiter:
        pt = time.time()
        logger.info(f"{item['item']}: Waited for: {pt - st:.2f} s ")
        # time.sleep(random.randint(1, 4))
        # print(f"End time for {item}: {time.time()}")
        # print("-" * 20)


def main(items):
    for item in items:
        # Send task to the specific queue using apply_async
        do_action.apply_async(args=[item], queue=item['queue'], priority=3)



if __name__ == "__main__":
    data_foo = [{"item": f"foo-{i}", "queue": "foo"} for i in range(10)]
    data_bar = [{"item": f"bar-{i}", "queue": "bar"} for i in range(20)]

    # add data to their own queues
    main(data_foo)
    main(data_bar)
