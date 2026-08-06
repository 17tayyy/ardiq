"""`Task` carries the decorated function's signature, so `.enqueue` is checked.

The positive cases below are covered by the `ty check ardiq tests` CI step: if
the ParamSpec plumbing breaks, correct calls start failing there. The negative
case needs its own type-checker run, since a test suite cannot assert that
something *doesn't* type-check just by running it.
"""

import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent


async def test_enqueue_accepts_the_declared_signature(redis, make_app):
    app = make_app("typed")

    @app.task()
    async def charge(user_id: int, amount: float, *, currency: str = "EUR") -> str:
        return f"{amount}{currency}/{user_id}"

    job = await charge.enqueue(1, 9.99, currency="USD")
    assert job.id

    bound = await charge.options(priority=None).enqueue(2, 5.0)
    assert bound.id


async def test_calling_inline_returns_the_declared_type(make_app):
    app = make_app("typed-inline")

    @app.task()
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, 3) == 5


async def test_ref_stays_untyped(make_app):
    app = make_app("typed-ref")

    # A reference has no local function, so nothing is known about its
    # parameters and nothing is checked.
    with pytest.raises(TypeError, match="reference"):
        app.ref("elsewhere")()


@pytest.mark.skipif(shutil.which("ty") is None, reason="ty not on PATH")
def test_wrong_arguments_fail_the_type_checker(tmp_path):
    snippet = tmp_path / "wrong_enqueue.py"
    snippet.write_text(
        textwrap.dedent("""
        from ardiq import Ardiq

        app = Ardiq()

        @app.task()
        async def charge(user_id: int, amount: float) -> str:
            return "ok"

        async def main() -> None:
            await charge.enqueue("not-an-int", 1.0)
        """)
    )
    result = subprocess.run(
        ["ty", "check", str(snippet)],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, (
        f"ty accepted a str where an int was declared:\n{result.stdout}"
    )
