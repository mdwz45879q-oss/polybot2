"""Command router for polybot2 CLI actions."""

from __future__ import annotations

from typing import Any
import logging

from polybot2._cli.commands_data_provider import (
    run_market_sync,
    run_provider_sync,
)
from polybot2._cli.commands_hotpath_runtime import run_hotpath_live, run_hotpath_observe
from polybot2._cli.commands_link import run_link_build, run_link_review


async def dispatch(args: Any, *, logger: logging.Logger) -> int:
    cmd = str(getattr(args, "command", "")).strip().lower()
    if cmd == "market" and str(getattr(args, "market_command", "")).strip().lower() == "sync":
        return await run_market_sync(args, logger=logger)
    if cmd == "provider" and str(getattr(args, "provider_command", "")).strip().lower() == "sync":
        return run_provider_sync(args, logger=logger)
    if cmd == "link" and str(getattr(args, "link_command", "")).strip().lower() == "build":
        return run_link_build(args, logger=logger)
    if cmd == "link" and str(getattr(args, "link_command", "")).strip().lower() == "review":
        return run_link_review(args, logger=logger)
    if cmd == "hotpath" and str(getattr(args, "hotpath_command", "")).strip().lower() == "live":
        return run_hotpath_live(args, logger=logger)
    if cmd == "hotpath" and str(getattr(args, "hotpath_command", "")).strip().lower() == "observe":
        return run_hotpath_observe(args, logger=logger)
    logger.error("Unsupported command")
    return 1


__all__ = ["dispatch"]
