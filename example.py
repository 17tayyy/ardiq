"""Runnable example.

  ardiq run example:app          # start a long-running worker
  python example.py              # enqueue a few tasks and print their results

The `python example.py` path runs the worker in burst mode (drains the queue and
exits), so it works as a single-file demo without a separate worker process.
"""

import asyncio
import logging

from ardiq import Ardiq
from ardiq._core import init_logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
init_logging(True)  # surface the core's logs too

logger = logging.getLogger(__name__)

app = Ardiq(redis_url="redis://localhost:6379", queue_name="example")


@app.task()
async def add(a: int, b: int) -> int:
    logger.info(f"adding {a} + {b}")
    return a + b


@app.task(max_retries=3)
def slow_double(x: int) -> int:
    # A sync task: ArdiQ runs it in a thread so it never blocks the event loop.
    return x * 2


async def main() -> None:
    jobs = [await add.enqueue(i, i) for i in range(3)]
    jobs.append(await slow_double.enqueue(21))

    app.burst = True
    await app.run()  # process everything queued, then exit

    for job in jobs:
        print(await job.result())


if __name__ == "__main__":
    asyncio.run(main())
