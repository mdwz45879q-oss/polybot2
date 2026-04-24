"""Execution service config wrapper for native hotpath."""

from __future__ import annotations

from polybot2.execution.contracts import FastExecutionConfig


class FastExecutionService:
    """Config container for native hotpath execution.

    All order dispatch is handled by the Rust native module.
    This class stores the execution config that gets passed to the Rust dispatch runtime.
    """

    def __init__(self, *, config: FastExecutionConfig | None = None):
        self._config = config or FastExecutionConfig.from_env()


__all__ = ["FastExecutionService"]
