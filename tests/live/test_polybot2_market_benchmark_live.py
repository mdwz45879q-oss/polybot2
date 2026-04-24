from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

import pytest

from polybot2._cli.actions import dispatch
from polybot2._cli.parser import build_parser

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        str(os.getenv("POLYBOT2_ENABLE_LIVE_BENCHMARK", "")).strip() != "1",
        reason="Set POLYBOT2_ENABLE_LIVE_BENCHMARK=1 to run live benchmark smoke.",
    ),
]


def test_live_market_benchmark_smoke(tmp_path: Path) -> None:
    out_dir = tmp_path / "bench_out"
    parser = build_parser()
    args = parser.parse_args(
        [
            "data",
            "benchmark-markets",
            "--concurrency-values",
            "10",
            "--max-rps-values",
            "24",
            "--repeats",
            "1",
            "--batch-size",
            "500",
            "--resolved-max-pages",
            "2",
            "--open-max-pages",
            "2",
            "--output-dir",
            str(out_dir),
        ]
    )
    code = asyncio.run(dispatch(args, logger=logging.getLogger("polybot2.test.live.market_benchmark")))
    assert code == 0

    summary_files = sorted(out_dir.glob("benchmark_*_summary.json"))
    assert summary_files
    payload = json.loads(summary_files[-1].read_text(encoding="utf-8"))
    assert payload["recommended"] is not None
