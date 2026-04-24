"""Command router for polybot2 CLI actions."""

from __future__ import annotations

from typing import Any
import logging

from polybot2._cli.commands_data_provider import (
    run_data_benchmark_markets,
    run_data_sync,
    run_provider_capture,
    run_provider_sync,
)
from polybot2._cli.commands_hotpath_runtime import run_hotpath, run_hotpath_observe, run_hotpath_replay
from polybot2._cli.commands_link import run_link_build, run_link_report, run_link_review, run_mapping_validate


async def dispatch(args: Any, *, logger: logging.Logger) -> int:
    cmd = str(getattr(args, "command", "")).strip().lower()
    if cmd == "data" and str(getattr(args, "data_command", "")).strip().lower() == "sync":
        return await run_data_sync(args, logger=logger)
    if cmd == "data" and str(getattr(args, "data_command", "")).strip().lower() == "benchmark-markets":
        return await run_data_benchmark_markets(args, logger=logger)
    if cmd == "provider" and str(getattr(args, "provider_command", "")).strip().lower() == "sync":
        return run_provider_sync(args, logger=logger)
    if cmd == "provider" and str(getattr(args, "provider_command", "")).strip().lower() == "capture":
        return run_provider_capture(args, logger=logger)
    if cmd == "mapping" and str(getattr(args, "mapping_command", "")).strip().lower() == "validate":
        return run_mapping_validate(args, logger=logger)
    if cmd == "link" and str(getattr(args, "link_command", "")).strip().lower() == "build":
        return run_link_build(args, logger=logger)
    if cmd == "link" and str(getattr(args, "link_command", "")).strip().lower() == "report":
        return run_link_report(args, logger=logger)
    if cmd == "link" and str(getattr(args, "link_command", "")).strip().lower() == "review":
        return run_link_review(args, logger=logger)
    if cmd == "hotpath" and str(getattr(args, "hotpath_command", "")).strip().lower() == "run":
        return run_hotpath(args, logger=logger)
    if cmd == "hotpath" and str(getattr(args, "hotpath_command", "")).strip().lower() == "replay":
        return run_hotpath_replay(args, logger=logger)
    if cmd == "hotpath" and str(getattr(args, "hotpath_command", "")).strip().lower() == "observe":
        return run_hotpath_observe(args, logger=logger)
    logger.error("Unsupported command")
    return 1


__all__ = ["dispatch"]
