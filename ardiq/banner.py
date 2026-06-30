"""Startup banner for the `ardiq run` worker CLI."""

from __future__ import annotations

from importlib.metadata import version
from typing import TYPE_CHECKING

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
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


def print_startup_banner(
    app: Ardiq,
    *,
    app_path: str,
    burst: bool,
    console: Console | None = None,
) -> None:
    """Print a Rich startup panel to stderr (Celery-style)."""
    out = console or Console(stderr=True, highlight=False)

    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column(style="bold cyan", min_width=12)
    table.add_column()

    table.add_row("app", app_path)
    table.add_row("version", version("ardiq"))
    table.add_row("worker", app.worker_id)
    table.add_row("redis", _safe_redis_url(app.redis_url))
    table.add_row("queue", app.queue_name)
    table.add_row("priorities", ", ".join(app.priorities))
    table.add_row("concurrency", str(app.concurrency))
    table.add_row("prefetch", str(app.prefetch))
    table.add_row("mode", "burst" if burst else "continuous")
    table.add_row("tasks", _format_list(app.tasks))
    if app.crons:
        table.add_row("crons", _format_list(app.crons))

    body = Group(
        Text(LOGO.strip("\n"), style="bold magenta"),
        "",
        table,
    )

    out.print(
        Panel(
            body,
            title="[bold]ArdiQ worker[/]",
            subtitle="[dim]bytay.dev[/]",
            border_style="green",
        )
    )
