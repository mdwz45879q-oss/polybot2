"""Public CLI actions facade."""

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
from polybot2._cli.router import dispatch as _dispatch


async def dispatch(args: Any, *, logger: logging.Logger) -> int:
    return await _dispatch(args, logger=logger)


__all__ = [
    "dispatch",
    "run_data_sync",
    "run_data_benchmark_markets",
    "run_provider_sync",
    "run_provider_capture",
    "run_mapping_validate",
    "run_link_build",
    "run_link_report",
    "run_link_review",
    "run_hotpath",
    "run_hotpath_observe",
    "run_hotpath_replay",
]
