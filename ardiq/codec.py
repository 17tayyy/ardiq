"""Default wire codec: msgpack pack/unpack for task payloads and results."""

from __future__ import annotations

from typing import Any

import msgpack


def _default_dumps(obj: Any) -> bytes:
    return msgpack.packb(obj)


def _default_loads(data: bytes) -> Any:
    return msgpack.unpackb(data, raw=False)
