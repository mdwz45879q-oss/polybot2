"""polybot2 CLI entrypoint."""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from polybot2._cli.router import dispatch
from polybot2._cli.parser import build_parser

LOG_FMT = "%(asctime)s [%(levelname)s] %(name)s | %(message)s"
log = logging.getLogger("polybot2")


def _cancel_pending(loop: asyncio.AbstractEventLoop) -> None:
    pending = asyncio.all_tasks(loop)
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format=LOG_FMT,
        handlers=[logging.StreamHandler(sys.stderr)],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    parser = build_parser()
    args = parser.parse_args()

    loop = asyncio.new_event_loop()

    try:
        code = loop.run_until_complete(dispatch(args, logger=log))
    except KeyboardInterrupt:
        code = 130
    finally:
        try:
            _cancel_pending(loop)
        finally:
            loop.close()
    raise SystemExit(int(code))


__all__ = ["main"]
