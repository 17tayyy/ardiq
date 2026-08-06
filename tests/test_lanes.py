"""A priority must name a lane some worker actually reads.

Writing to an unconfigured lane used to succeed and then lie about it: the task
sat in a stream nobody consumed, `status()` said `queued` forever and
`queue_size()` said 0.
"""

import pytest

from ardiq import Ardiq

LANES = ["low", "default", "high"]


def test_task_rejects_an_unconfigured_lane(make_app):
    app = make_app("lanes", priorities=LANES)

    with pytest.raises(ValueError, match="urgent"):

        @app.task(priority="urgent")
        async def charge():
            pass


def test_cron_rejects_an_unconfigured_lane(make_app):
    app = make_app("lanes-cron", priorities=LANES)

    with pytest.raises(ValueError, match="urgent"):

        @app.cron(every=60, priority="urgent")
        async def sweep():
            pass


async def test_ref_enqueue_rejects_an_unconfigured_lane(make_app):
    app = make_app("lanes-ref", priorities=LANES)

    with pytest.raises(ValueError, match="urgent"):
        await app.ref("charge", priority="urgent").enqueue()


async def test_options_rejects_an_unconfigured_lane(make_app):
    app = make_app("lanes-options", priorities=LANES)

    @app.task()
    async def charge():
        pass

    with pytest.raises(ValueError, match="urgent"):
        await charge.options(priority="urgent").enqueue()


async def test_error_names_the_configured_lanes(make_app):
    app = make_app("lanes-msg", priorities=LANES)

    with pytest.raises(ValueError) as exc:
        await app.ref("charge", priority="urgent").enqueue()

    message = str(exc.value)
    assert "'urgent'" in message
    assert all(lane in message for lane in LANES)


async def test_configured_lanes_still_enqueue(redis, make_app):
    app = make_app("lanes-ok", priorities=LANES)

    @app.task(priority="high")
    async def charge():
        pass

    job = await charge.enqueue()
    assert await job.status() == "queued"
    assert app.default_priority == "default"


def test_default_priority_is_still_validated():
    with pytest.raises(ValueError, match="default_priority"):
        Ardiq(
            queue_name="lanes-default",
            priorities=["low", "high"],
            default_priority="mid",
        )
