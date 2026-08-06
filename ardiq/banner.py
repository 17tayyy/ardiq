"""Startup banner for the `ardiq run` worker CLI.

Rendered by hand rather than with a formatting library: this runs in every
worker process, and importing one costs several MB of RSS for a box that is
drawn once.
"""

from __future__ import annotations

import os
import sys
from importlib.metadata import version
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import TextIO

    from ardiq import Ardiq

LOGO = r"""
                          ___
                         (   )  .-.
  .---.   ___ .-.      .-.| |  ( __)   .--.
 / .-, \ (   )   \    /   \ |  (''")  /    \
(__) ; |  | ' .-. ;  |  .-. |   | |  |  .-. '
  .'`  |  |  / (___) | |  | |   | |  | |  | |
 / .'| |  | |        | |  | |   | |  | |  | |
| /  | |  | |        | |  | |   | |  | |  | |
; |  ; |  | |        | '  | |   | |  | '  | |
' `-'  |  | |        ' `-'  /   | |  ' `-'  |
`.__.'_. (___)        `.__,'   (___)  `._ / |
                                          | |
                                         (___)
"""

_MAX_LIST_ITEMS = 8

_TITLE = "ArdiQ worker"
_SUBTITLE = "bytay.dev"

_PAD_X = 4  # columns of breathing room on each side
_PAD_Y = 1  # blank rows above and below the content

_BOX_UNICODE = ("╭", "╰", "╮", "╯", "─", "│")
_BOX_ASCII = ("+", "+", "+", "+", "-", "|")

_RESET = "\033[0m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_MAGENTA = "\033[1;35m"
_CYAN = "\033[1;36m"
_GREEN = "\033[32m"


def _safe_redis_url(url: str) -> str:
    if "@" not in url or "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    _, host = rest.rsplit("@", 1)
    return f"{scheme}://:***@{host}"


def _format_list(items: list[str]) -> str:
    if not items:
        return "(none)"
    if len(items) <= _MAX_LIST_ITEMS:
        return ", ".join(items)
    shown = ", ".join(items[:_MAX_LIST_ITEMS])
    return f"{shown}, … (+{len(items) - _MAX_LIST_ITEMS} more)"


def _use_colour(file: TextIO) -> bool:
    # Honour https://no-color.org; otherwise colour only a real terminal, so
    # redirected logs don't collect escape codes.
    if os.environ.get("NO_COLOR"):
        return False
    try:
        return bool(file.isatty())
    except (AttributeError, ValueError):
        return False


def _box_chars(file: TextIO) -> tuple[str, ...]:
    encoding = getattr(file, "encoding", None) or "ascii"
    try:
        "".join(_BOX_UNICODE).encode(encoding)
    except (UnicodeEncodeError, LookupError):
        return _BOX_ASCII
    return _BOX_UNICODE


def _rows(app: Ardiq, *, app_path: str, burst: bool) -> list[tuple[str, str]]:
    rows = [
        ("app", app_path),
        ("version", version("ardiq")),
        ("worker", app.worker_id),
        ("redis", _safe_redis_url(app.redis_url)),
        ("queue", app.queue_name),
        ("priorities", ", ".join(app.priorities)),
        ("concurrency", str(app.concurrency)),
        ("prefetch", str(app.prefetch)),
        ("mode", "burst" if burst else "continuous"),
        ("tasks", _format_list(app.tasks)),
    ]
    if app.crons:
        rows.append(("crons", _format_list(app.crons)))
    return rows


def print_startup_banner(
    app: Ardiq,
    *,
    app_path: str,
    burst: bool,
    file: TextIO | None = None,
) -> None:
    """Print the startup panel to stderr (Celery-style)."""
    out = file if file is not None else sys.stderr
    colour = _use_colour(out)
    top_l, bot_l, top_r, bot_r, horiz, vert = _box_chars(out)

    def paint(text: str, style: str) -> str:
        return f"{style}{text}{_RESET}" if colour else text

    logo_lines = LOGO.strip("\n").split("\n")
    rows = _rows(app, app_path=app_path, burst=burst)
    key_width = max(len(k) for k, _ in rows)

    # Lay the body out in plain text first, then paint: widths are measured on
    # the plain strings so escape sequences never count toward them.
    margin = [""] * _PAD_Y
    plain = [
        *margin,
        *logo_lines,
        "",
        *(f"{k.ljust(key_width)}    {v}" for k, v in rows),
        *margin,
    ]
    painted = [
        *margin,
        *(paint(line, _MAGENTA) for line in logo_lines),
        "",
        *(paint(k.ljust(key_width), _CYAN) + "    " + v for k, v in rows),
        *margin,
    ]

    # `inner` counts the characters between the two border columns.
    inner = max(
        max(len(line) for line in plain) + _PAD_X * 2,
        len(_TITLE) + 3,
        len(_SUBTITLE) + 3,
    )

    def edge(left: str, right: str, label: str, style: str) -> str:
        """A border row with the label centred in it."""
        fill = inner - len(label) - 2
        lead = fill // 2
        return (
            paint(left + horiz * lead + " ", _GREEN)
            + paint(label, style)
            + paint(" " + horiz * (fill - lead) + right, _GREEN)
        )

    write = out.write
    bar = paint(vert, _GREEN)

    write(edge(top_l, top_r, _TITLE, _BOLD) + "\n")
    for plain_line, shown in zip(plain, painted, strict=True):
        lead = " " * _PAD_X
        trail = " " * (inner - len(plain_line) - _PAD_X)
        write(f"{bar}{lead}{shown}{trail}{bar}\n")
    write(edge(bot_l, bot_r, _SUBTITLE, _DIM) + "\n")
