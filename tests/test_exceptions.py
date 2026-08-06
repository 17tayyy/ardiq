"""Typed errors from the core: a broker that's down vs everything else."""

import pytest

from ardiq import Ardiq, ArdiqError, BrokerError

DEAD_URL = "redis://localhost:6399"  # nothing listens here


def test_the_hierarchy_keeps_except_runtimeerror_working():
    """Existing code catching RuntimeError must not break on the new classes."""
    assert issubclass(BrokerError, ArdiqError)
    assert issubclass(ArdiqError, RuntimeError)


async def test_an_unreachable_broker_raises_brokererror():
    """Enqueuing from a request handler is the path that has to be catchable.

    Slow on purpose (~9s): that's the core's connection-retry budget being spent
    before it gives up. One case is enough to pin the classification.
    """
    app = Ardiq(redis_url=DEAD_URL, queue_name="dead")

    @app.task()
    async def work(): ...

    with pytest.raises(BrokerError):
        await work.enqueue()


def test_a_malformed_url_is_not_a_broker_failure():
    """A bad URL is the caller's config, not an outage — so not BrokerError."""
    with pytest.raises(ArdiqError) as excinfo:
        Ardiq(redis_url="not-a-url")

    assert not isinstance(excinfo.value, BrokerError)
