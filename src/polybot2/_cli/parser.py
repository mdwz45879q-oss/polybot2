"""Top-level CLI parser for polybot2."""

from __future__ import annotations

import argparse

from polybot2._cli.args import add_subcommands


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="polybot2", description="polybot2 CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    add_subcommands(sub)
    return parser
