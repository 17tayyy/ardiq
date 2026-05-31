"""Runnable example.

  ardiq run example:app          # start a long-running worker
  python example.py              # enqueue a few tasks and print their results

The `python example.py` path runs the worker in burst mode (drains the queue and
exits), so it works as a single-file demo without a separate worker process.
"""

import asyncio

from ardiq import Ardiq

app = Ardiq(redis_url="redis://localhost:6379", queue_name="example")


@app.task()
async def add(a: int, b: int) -> int:
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
