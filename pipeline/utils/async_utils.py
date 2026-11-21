"""Small reusable helpers for asynchronous work and batching."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Iterator
from typing import TypeVar

T = TypeVar("T")


async def async_retry(
    fn: Callable[[], Awaitable[T]],
    attempts: int = 3,
    base_delay: float = 1.0,
) -> T:
    """Run an async operation with exponential backoff between failures."""

    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if base_delay < 0:
        raise ValueError("base_delay cannot be negative")

    for attempt in range(attempts):
        try:
            return await fn()
        except Exception:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(base_delay * (2**attempt))
    raise RuntimeError("unreachable")


def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield fixed-size lists from any iterable."""

    if size < 1:
        raise ValueError("size must be at least 1")
    chunk: list[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
