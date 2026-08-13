"""What the app accepts at construction, and the config it exposes."""

import pytest

from ardiq import Ardiq


@pytest.mark.parametrize("url", ["", "   "])
def test_an_empty_redis_url_is_refused(url):
    # It used to sail through to the Redis client and fail there, naming nothing.
    with pytest.raises(ValueError, match="redis_url"):
        Ardiq(redis_url=url)


def test_no_redis_url_means_the_default():
    assert Ardiq().redis_url == "redis://localhost:6379"


def test_core_settings_are_readable(make_app):
    app = make_app("cfg", idle_timeout_ms=1234, poll_block_ms=42, result_ttl_ms=99)

    assert app.idle_timeout_ms == 1234
    assert app.poll_block_ms == 42
    assert app.result_ttl_ms == 99
