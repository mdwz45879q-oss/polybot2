"""Link review rendering/session helpers."""

from __future__ import annotations

import base64
from dataclasses import replace
from datetime import datetime, timezone
import io
import json
import os
import select
import sys
import termios
import time
from typing import Any
import tty

from polybot2._cli.common import _int_or_none

try:  # optional dependency in local env
    from rich import box
    from rich.console import Group
    from rich.console import Console
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    _RICH_AVAILABLE = True
except Exception:  # pragma: no cover - fallback if rich is absent
    _RICH_AVAILABLE = False

def _resolution_style(state: str) -> str:
    s = str(state or "").upper()
    if s == "MATCHED_CLEAN":
        return "bold black on green"
    if s in {"MATCHED_WITH_WARNINGS", "NO_TRADEABLE_TARGETS"}:
        return "bold black on yellow"
    if s in {"AMBIGUOUS_EVENT_MATCH", "TEAM_SET_NOT_FOUND", "NO_EVENT_CANDIDATES"}:
        return "bold white on red"
    return "bold white on blue"


def _decision_style(decision: str) -> str:
    d = str(decision or "").lower()
    if d == "approve":
        return "bold black on green"
    if d == "reject":
        return "bold white on red"
    if d == "skip":
        return "bold black on yellow"
    return "bold white on blue"


def _decision_label(decision: str) -> str:
    d = str(decision or "").lower()
    if d == "approve":
        return "APPROVED"
    if d == "reject":
        return "REJECTED"
    if d == "skip":
        return "SKIPPED"
    return "PENDING"


def _tradeable_style(is_tradeable: bool) -> str:
    return "bold black on green" if bool(is_tradeable) else "bold white on red"


def _tradeable_label(is_tradeable: bool) -> str:
    return "TRADEABLE" if bool(is_tradeable) else "NON-TRADEABLE"


def _game_state_style(state: str) -> str:
    s = str(state or "").strip().upper()
    if s == "NOT STARTED":
        return "bold black on grey70"
    if s == "LIVE":
        return "bold white on red"
    if s == "FINAL":
        return "bold white on bright_black"
    return "bold white on blue"


def _normalize_status_token(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _derive_game_state(
    *,
    provider_game: dict[str, Any],
    event_resolution: dict[str, Any],
    now_ts_utc: int | None = None,
) -> str:
    final_statuses = {
        "closed",
        "resolved",
        "ended",
        "finished",
        "final",
        "complete",
        "completed",
        "cancelled",
        "canceled",
    }
    live_statuses = {
        "live",
        "inplay",
        "in_play",
        "ongoing",
        "in_progress",
        "started",
        "halftime",
        "overtime",
    }
    not_started_statuses = {
        "scheduled",
        "upcoming",
        "not_started",
        "pending",
        "pre",
        "pregame",
    }

    status_tokens: list[str] = []
    selected_event_status = _normalize_status_token(event_resolution.get("selected_event_status"))
    if selected_event_status:
        status_tokens.append(selected_event_status)
    selected_events = event_resolution.get("selected_events")
    if isinstance(selected_events, list):
        for ev in selected_events:
            if isinstance(ev, dict):
                token = _normalize_status_token(ev.get("status"))
                if token:
                    status_tokens.append(token)

    if any(token in final_statuses for token in status_tokens):
        return "FINAL"
    if any(token in live_statuses for token in status_tokens):
        return "LIVE"
    if any(token in not_started_statuses for token in status_tokens):
        return "NOT STARTED"

    kickoff_ts = _int_or_none(event_resolution.get("selected_event_kickoff_ts_utc"))
    if kickoff_ts is None:
        kickoff_ts = _int_or_none(provider_game.get("kickoff_ts_utc"))
    if kickoff_ts is None:
        return "UNKNOWN"
    now_ts = int(time.time()) if now_ts_utc is None else int(now_ts_utc)
    return "NOT STARTED" if now_ts < int(kickoff_ts) else "LIVE"


_SESSION_STYLE_MAP: dict[str, str] = {
    "status_success": "bold green",
    "status_warn": "bold yellow",
    "status_error": "bold red",
    "section_title": "bold bright_cyan",
    "selected_primary": "bold bright_magenta",
    "label_dim": "grey70",
    "value_primary": "bold bright_white",
    "value_secondary": "bright_white",
    "meta_dim": "grey58",
    "meta_accent": "bright_blue",
}


def _session_style(name: str) -> str:
    return str(_SESSION_STYLE_MAP.get(name) or "white")


def _resolution_inline_style(state: str) -> str:
    s = str(state or "").upper()
    if s == "MATCHED_CLEAN":
        return _session_style("status_success")
    if s in {"MATCHED_WITH_WARNINGS", "NO_TRADEABLE_TARGETS"}:
        return _session_style("status_warn")
    if s in {"AMBIGUOUS_EVENT_MATCH", "TEAM_SET_NOT_FOUND", "NO_EVENT_CANDIDATES"}:
        return _session_style("status_error")
    return _session_style("value_secondary")


def _kv_value_style(key: str, value: str) -> str:
    k = str(key or "").strip().lower()
    v = str(value or "").strip()
    vu = v.upper()
    vl = v.lower()
    if k in {"state", "resolution_state"}:
        return _resolution_inline_style(v)
    if k in {"reason", "reject"}:
        return _session_style("status_warn") if v else _session_style("meta_dim")
    if k in {"parse"}:
        return _session_style("status_success") if vl == "ok" else _session_style("status_warn")
    if k in {"game_state"}:
        if vu == "LIVE":
            return _session_style("status_error")
        if vu == "NOT STARTED":
            return _session_style("meta_dim")
        if vu == "FINAL":
            return _session_style("status_warn")
        return _session_style("value_secondary")
    if k in {"primary_event", "title"}:
        return _session_style("selected_primary")
    if k in {"league", "home", "away", "raw"}:
        return _session_style("value_primary")
    if k in {"tradeable"}:
        if vl in {"yes", "true", "1"}:
            return _session_style("status_success")
        if vl in {"no", "false", "0"}:
            return _session_style("status_error")
    if k in {"binding"}:
        if vl in {"exact", "linked", "ok"}:
            return _session_style("status_success")
        if vl:
            return _session_style("status_warn")
        return _session_style("meta_dim")
    if k == "tradeable_targets":
        try:
            lhs, rhs = v.split("/", 1)
            return _session_style("status_success") if int(lhs or 0) > 0 and int(rhs or 0) > 0 else _session_style("status_error")
        except Exception:
            return _session_style("value_secondary")
    if k in {"condition", "token", "event_id"}:
        return _session_style("meta_accent")
    if k in {"slug", "slug_hint", "score", "delta", "kickoff_utc", "date_et", "id"}:
        return _session_style("meta_dim")
    if vu in {"PRIMARY"}:
        return _session_style("selected_primary")
    return _session_style("value_secondary")


def _styled_card_line(line: str) -> Text:
    src = str(line or "")
    if not src:
        return Text("")
    stripped = src.strip()
    if stripped in {"Provider Game", "Canonicalization", "Event Resolution", "Matched Events", "Notes", "Candidate Comparison", "Trace"}:
        return Text(src, style=_session_style("section_title"))

    leading = len(src) - len(src.lstrip(" "))
    body = src.lstrip(" ")
    out = Text()
    if leading > 0:
        out.append(" " * leading)

    if "| " in body and body.startswith("["):
        # Compact market semantic rows.
        icon_end = body.find("]")
        if icon_end >= 0:
            icon = body[: icon_end + 1]
            rest = body[icon_end + 1 :]
            icon_style = _session_style("status_warn")
            if "✓" in icon:
                icon_style = _session_style("status_success")
            elif "X" in icon:
                icon_style = _session_style("status_error")
            out.append(icon, style=icon_style)
            out.append(rest, style=_session_style("value_secondary"))
            return out

    if "=" not in body:
        style = _session_style("value_secondary")
        if "primary" in body.lower():
            style = _session_style("selected_primary")
        if "outcome " in body and "token=" in body:
            style = _session_style("value_secondary")
        return Text(src, style=style)

    segments = body.split("  ")
    for idx, segment in enumerate(segments):
        if idx > 0:
            out.append("  ")
        if "=" not in segment:
            seg_style = _session_style("value_secondary")
            if segment.strip().lower() == "primary":
                seg_style = _session_style("selected_primary")
            out.append(segment, style=seg_style)
            continue
        key, value = segment.split("=", 1)
        out.append(f"{key}=", style=_session_style("label_dim"))
        out.append(value, style=_kv_value_style(key, value))
    return out


def _fmt_ts_utc(value: Any) -> str:
    iv = _int_or_none(value)
    if iv is None:
        return ""
    return datetime.fromtimestamp(iv, tz=timezone.utc).isoformat()


def _fmt_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (list, tuple)):
        return json.dumps(value, separators=(",", ":"), default=str)
    return str(value)


def _kv_table(rows: list[tuple[str, Any]]) -> Any:
    table = Table.grid(padding=(0, 2))
    table.add_column(style="bold cyan", no_wrap=True)
    table.add_column(style="white")
    for key, value in rows:
        table.add_row(str(key), _fmt_value(value))
    return table


def _target_status_icon(target: dict[str, Any]) -> str:
    tradeable = int(target.get("is_tradeable") or 0) == 1
    reason = str(target.get("reason_code") or "").strip().lower()
    if tradeable:
        return "[green]✓[/green]"
    if reason and reason not in {"", "ok"}:
        return "[yellow]![/yellow]"
    return "[red]✗[/red]"


def _build_game_card_renderable(
    payload: dict[str, Any],
    *,
    queue_position: str = "",
    view_mode: str = "card",
    session_note: str = "",
    scope: str = "",
    filters_text: str = "",
    candidates: list[dict[str, Any]] | None = None,
) -> Any:
    if not bool(payload.get("found")):
        return Panel(
            f"Game card not found\nprovider={payload.get('provider')} run_id={payload.get('run_id')} provider_game_id={payload.get('provider_game_id')}",
            border_style="red",
            title="Link Review",
        )
    card = payload.get("card") if isinstance(payload.get("card"), dict) else {}
    provider_game = card.get("provider_game") if isinstance(card.get("provider_game"), dict) else {}
    canonical = card.get("canonicalization") if isinstance(card.get("canonicalization"), dict) else {}
    event_resolution = card.get("event_resolution") if isinstance(card.get("event_resolution"), dict) else {}
    markets = card.get("market_bindings") if isinstance(card.get("market_bindings"), dict) else {}
    notes = card.get("notes") if isinstance(card.get("notes"), dict) else {}
    latest_decision = card.get("latest_decision") if isinstance(card.get("latest_decision"), dict) else {}
    resolution_state = str(event_resolution.get("resolution_state") or "")
    decision = str(latest_decision.get("decision") or "")
    is_tradeable = bool(markets.get("is_tradeable"))
    game_state = _derive_game_state(provider_game=provider_game, event_resolution=event_resolution)
    targets = markets.get("targets") if isinstance(markets.get("targets"), list) else []

    header = Text()
    header.append(f" {_decision_label(decision)} ", style=_decision_style(decision))
    header.append(" ")
    header.append(f" {game_state} ", style=_game_state_style(game_state))
    header.append(" ")
    header.append(f" {_tradeable_label(is_tradeable)} ", style=_tradeable_style(is_tradeable))
    header.append(" ")
    header.append(f" {resolution_state or 'UNKNOWN'} ", style=_resolution_style(resolution_state))
    if queue_position:
        header.append(f"   {queue_position}", style="bold cyan")
    header.append(f"   run_id={payload.get('run_id')}", style="dim")
    if scope:
        header.append(f"   scope={scope}", style="dim")
    if filters_text:
        header.append(f"   {filters_text}", style="dim")

    provider_section = Panel(
        _kv_table(
            [
                ("ID", provider_game.get("provider_game_id")),
                ("Provider league", provider_game.get("league_raw")),
                ("Provider sport", provider_game.get("sport_raw")),
                ("Teams raw", f"{provider_game.get('away_raw') or ''} @ {provider_game.get('home_raw') or ''}".strip()),
                ("Game date ET", provider_game.get("game_date_et")),
                ("Kickoff UTC", _fmt_ts_utc(provider_game.get("kickoff_ts_utc"))),
                ("Game state", game_state),
                ("Parse status", provider_game.get("parse_status")),
                ("Label", provider_game.get("game_label")),
            ]
        ),
        title="Provider Game",
        border_style="cyan",
    )
    canonical_section = Panel(
        _kv_table(
            [
                ("League", canonical.get("canonical_league")),
                ("Home", canonical.get("canonical_home_team")),
                ("Away", canonical.get("canonical_away_team")),
                ("Team set", "{" + ", ".join(sorted([str(canonical.get("canonical_home_team") or ""), str(canonical.get("canonical_away_team") or "")])) + "}"),
                ("Slug hint", canonical.get("event_slug_prefix")),
            ]
        ),
        title="Canonicalization",
        border_style="cyan",
    )
    event_section = Panel(
        _kv_table(
            [
                ("Reason code", event_resolution.get("reason_code")),
                ("Selected event", event_resolution.get("selected_event_id")),
                (
                    "Selected events",
                    ",".join(
                        [
                            str(ev.get("event_id") or "")
                            for ev in (
                                event_resolution.get("selected_events")
                                if isinstance(event_resolution.get("selected_events"), list)
                                else []
                            )
                            if str(ev.get("event_id") or "")
                        ]
                    ),
                ),
                ("Selected slug", event_resolution.get("selected_event_slug")),
                ("Title", event_resolution.get("selected_event_title")),
                ("Kickoff UTC", _fmt_ts_utc(event_resolution.get("selected_event_kickoff_ts_utc"))),
                ("Kickoff delta (sec)", event_resolution.get("kickoff_delta_sec")),
                ("Kickoff tolerance (min)", event_resolution.get("kickoff_tolerance_minutes")),
                ("Score tuple", event_resolution.get("score_tuple") or []),
                ("Slug fallback used", event_resolution.get("used_slug_fallback")),
            ]
        ),
        title="Event Resolution",
        border_style="cyan",
    )
    market_summary_section = Panel(
        _kv_table(
            [
                ("Targets", len(targets)),
                ("Tradeable targets", markets.get("n_tradeable_targets")),
                ("Tradeable", is_tradeable),
            ]
        ),
        title="Market Binding Summary",
        border_style="cyan",
    )

    targets_table = Table(box=box.SIMPLE_HEAVY, header_style="bold magenta")
    targets_table.add_column("status", no_wrap=True)
    targets_table.add_column("type")
    targets_table.add_column("condition_id")
    targets_table.add_column("outcome")
    targets_table.add_column("token_id")
    targets_table.add_column("binding")
    targets_table.add_column("reason")
    for t in targets:
        targets_table.add_row(
            _target_status_icon(t),
            _fmt_value(t.get("sports_market_type")),
            _fmt_value(t.get("condition_id")),
            _fmt_value(t.get("outcome_index")),
            _fmt_value(t.get("token_id")),
            _fmt_value(t.get("binding_status")),
            _fmt_value(t.get("reason_code")),
        )
    notes_section = Panel(
        _kv_table(
            [
                ("Reason notes", ", ".join(notes.get("reason_notes") or [])),
                ("Decision note", latest_decision.get("note")),
                ("Decided by", latest_decision.get("actor")),
                ("Decided at", _fmt_ts_utc(latest_decision.get("decided_at"))),
            ]
        ),
        title="Notes",
        border_style="cyan",
    )

    content_blocks: list[Any] = [provider_section, canonical_section, event_section]
    mode = str(view_mode or "card").strip().lower()
    if mode == "candidates":
        cand_rows = candidates or []
        cand_table = Table(box=box.SIMPLE_HEAVY, header_style="bold cyan")
        cand_table.add_column("rank", no_wrap=True)
        cand_table.add_column("event_id")
        cand_table.add_column("event_slug")
        cand_table.add_column("team_set")
        cand_table.add_column("kickoff_ok")
        cand_table.add_column("slug_hint")
        cand_table.add_column("order_bonus")
        cand_table.add_column("delta_sec")
        cand_table.add_column("selected")
        cand_table.add_column("reject_reason")
        for row in cand_rows:
            cand_table.add_row(
                _fmt_value(row.get("candidate_rank")),
                _fmt_value(row.get("event_id")),
                _fmt_value(row.get("event_slug")),
                _fmt_value(row.get("team_set_match")),
                _fmt_value(row.get("kickoff_within_tolerance")),
                _fmt_value(row.get("slug_hint_match")),
                _fmt_value(row.get("ordering_bonus")),
                _fmt_value(row.get("kickoff_delta_sec")),
                "yes" if int(row.get("is_selected") or 0) == 1 else "",
                _fmt_value(row.get("reject_reason")),
            )
        content_blocks.append(Panel(cand_table, title="Candidate Comparison", border_style="magenta"))
    elif mode == "markets":
        content_blocks.extend([market_summary_section, Panel(targets_table, title="Market Targets", border_style="magenta"), notes_section])
    elif mode == "trace":
        trace_payload = notes.get("trace") if isinstance(notes.get("trace"), dict) else {}
        trace_json = json.dumps(trace_payload, indent=2, sort_keys=True, default=str)
        content_blocks.append(Panel(trace_json, title="Deterministic Trace", border_style="magenta"))
        content_blocks.append(notes_section)
    else:
        content_blocks.extend([market_summary_section, Panel(targets_table, title="Market Targets", border_style="magenta"), notes_section])

    footer = Text()
    if session_note:
        footer.append(session_note, style="bold yellow")
        footer.append("   ")
    footer.append("hotkeys: ", style="bold cyan")
    footer.append("←/h prev  →/l next  a approve  r reject  s skip  c candidates  m markets  t trace  q quit", style="white")
    content_blocks.append(Panel(footer, border_style="blue"))
    body = Group(*content_blocks)
    border = "green" if decision == "approve" else "red" if decision == "reject" else "yellow" if decision == "skip" else "cyan"
    return Panel(body, title=header, border_style=border, padding=(0, 1))


def _render_game_card_text(payload: dict[str, Any]) -> str:
    if _RICH_AVAILABLE:
        buf = io.StringIO()
        Console(file=buf, force_terminal=True, color_system="standard", width=160).print(_build_game_card_renderable(payload))
        return buf.getvalue().rstrip()
    if not bool(payload.get("found")):
        return f"Game card not found: provider={payload.get('provider')} run_id={payload.get('run_id')} provider_game_id={payload.get('provider_game_id')}"
    card = payload.get("card") if isinstance(payload.get("card"), dict) else {}
    provider_game = card.get("provider_game") if isinstance(card.get("provider_game"), dict) else {}
    event_resolution = card.get("event_resolution") if isinstance(card.get("event_resolution"), dict) else {}
    markets = card.get("market_bindings") if isinstance(card.get("market_bindings"), dict) else {}
    latest_decision = card.get("latest_decision") if isinstance(card.get("latest_decision"), dict) else {}
    game_state = _derive_game_state(provider_game=provider_game, event_resolution=event_resolution)
    return (
        f"Review Card provider={payload.get('provider')} run_id={payload.get('run_id')} provider_game_id={payload.get('provider_game_id')}\n"
        f"game_state={game_state} resolution_state={event_resolution.get('resolution_state')} tradeable={markets.get('is_tradeable')} latest_decision={latest_decision.get('decision')}"
    )


def _decode_session_key(data: bytes) -> str:
    if not data:
        return ""
    if data in {b"\x1b[D", b"\x1bOD"}:
        return "left"
    if data in {b"\x1b[C", b"\x1bOC"}:
        return "right"
    if data in {b"\x1b[A", b"\x1bOA"}:
        return "up"
    if data in {b"\x1b[B", b"\x1bOB"}:
        return "down"
    if data in {b"\x1b[5~"}:
        return "pageup"
    if data in {b"\x1b[6~"}:
        return "pagedown"
    if data in {b"\x1b[H", b"\x1bOH", b"\x1b[1~"}:
        return "home"
    if data in {b"\x1b[F", b"\x1bOF", b"\x1b[4~"}:
        return "end"
    if data in {b"\x03", b"\x04"}:
        return "q"
    try:
        text = data.decode("utf-8", errors="ignore").strip().lower()
    except Exception:
        return ""
    if text in {"h", "l", "a", "r", "s", "c", "m", "t", "q", "x", "o", "u"}:
        return text
    if text in {"1", "2", "3", "4", "5", "6", "7", "8", "9"}:
        return text
    if text in {"left"}:
        return "left"
    if text in {"right"}:
        return "right"
    if text in {"up"}:
        return "up"
    if text in {"down"}:
        return "down"
    if text in {"pageup", "pgup"}:
        return "pageup"
    if text in {"pagedown", "pgdn"}:
        return "pagedown"
    if text in {"home"}:
        return "home"
    if text in {"end"}:
        return "end"
    return ""


def _interactive_session_available() -> bool:
    return bool(_RICH_AVAILABLE and sys.stdin.isatty() and sys.stdout.isatty())


def _read_single_tty_key(*, timeout_s: float = 0.2) -> str:
    fd = sys.stdin.fileno()
    ready, _, _ = select.select([fd], [], [], timeout_s)
    if not ready:
        return ""
    first = os.read(fd, 1)
    if first != b"\x1b":
        return _decode_session_key(first)
    data = first
    while True:
        nxt, _, _ = select.select([fd], [], [], 0.005)
        if not nxt:
            break
        data += os.read(fd, 1)
        if len(data) >= 8:
            break
    return _decode_session_key(data)


def _abbrev_id(value: str, *, left: int = 6, right: int = 4) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= left + right + 1:
        return text
    return f"{text[:left]}…{text[-right:]}"


def _build_card_document_lines(
    payload: dict[str, Any],
    *,
    view_mode: str,
    expanded_event_ids: set[str],
    show_full_ids: bool,
    show_unselected_markets: bool = False,
    candidates: list[dict[str, Any]] | None,
) -> list[str]:
    if not bool(payload.get("found")):
        return [
            f"Game card not found: provider={payload.get('provider')} run_id={payload.get('run_id')} provider_game_id={payload.get('provider_game_id')}"
        ]
    card = payload.get("card") if isinstance(payload.get("card"), dict) else {}
    provider_game = card.get("provider_game") if isinstance(card.get("provider_game"), dict) else {}
    canonical = card.get("canonicalization") if isinstance(card.get("canonicalization"), dict) else {}
    event_resolution = card.get("event_resolution") if isinstance(card.get("event_resolution"), dict) else {}
    markets = card.get("market_bindings") if isinstance(card.get("market_bindings"), dict) else {}
    notes = card.get("notes") if isinstance(card.get("notes"), dict) else {}
    selected_events = (
        event_resolution.get("selected_events") if isinstance(event_resolution.get("selected_events"), list) else []
    )
    game_state = _derive_game_state(provider_game=provider_game, event_resolution=event_resolution)
    market_blocks = markets.get("markets") if isinstance(markets.get("markets"), list) else []
    unselected_market_blocks = (
        markets.get("unselected_markets")
        if isinstance(markets.get("unselected_markets"), list)
        else []
    )
    mode = str(view_mode or "card").strip().lower()

    lines: list[str] = []
    lines.append("Provider Game")
    lines.append(
        "  id={gid}  date_et={date}  kickoff_utc={kickoff}".format(
            gid=str(provider_game.get("provider_game_id") or ""),
            date=str(provider_game.get("game_date_et") or ""),
            kickoff=_fmt_ts_utc(provider_game.get("kickoff_ts_utc")),
        )
    )
    lines.append(
        "  raw={away} @ {home}  parse={parse}".format(
            away=str(provider_game.get("away_raw") or ""),
            home=str(provider_game.get("home_raw") or ""),
            parse=str(provider_game.get("parse_status") or ""),
        )
    )
    lines.append("")
    lines.append("Canonicalization")
    lines.append(
        "  league={league}  home={home}  away={away}".format(
            league=str(canonical.get("canonical_league") or ""),
            home=str(canonical.get("canonical_home_team") or ""),
            away=str(canonical.get("canonical_away_team") or ""),
        )
    )
    lines.append(f"  slug_hint={str(canonical.get('event_slug_prefix') or '')}")
    lines.append("")
    lines.append("Event Resolution")
    lines.append(
        "  game_state={game_state}  state={state}  reason={reason}  selected_events={n}  tradeable_targets={tt}/{t}  markets={sm}/{tm}".format(
            game_state=game_state,
            state=str(event_resolution.get("resolution_state") or ""),
            reason=str(event_resolution.get("reason_code") or ""),
            n=len(selected_events),
            tt=int(markets.get("n_tradeable_targets") or 0),
            t=int(markets.get("n_targets") or 0),
            sm=int(markets.get("n_selected_markets") or len(market_blocks)),
            tm=int(markets.get("n_total_markets") or len(market_blocks)),
        )
    )
    lines.append(
        "  primary_event={eid}  score={score}  kickoff_delta_sec={delta}".format(
            eid=str(event_resolution.get("selected_event_id") or ""),
            score=_fmt_value(event_resolution.get("score_tuple") or []),
            delta=_fmt_value(event_resolution.get("kickoff_delta_sec")),
        )
    )
    lines.append("")

    if mode == "candidates":
        cand_rows = candidates or []
        lines.append("Candidate Comparison")
        if not cand_rows:
            lines.append("  (none)")
        else:
            for row in cand_rows:
                selected_mark = "*" if int(row.get("is_selected") or 0) == 1 else " "
                lines.append(
                    "  {sel} rank={rank} event={event} slug={slug} team_set={ts} kickoff_ok={ko} delta={delta} score={score} reject={rej}".format(
                        sel=selected_mark,
                        rank=_fmt_value(row.get("candidate_rank")),
                        event=str(row.get("event_id") or ""),
                        slug=str(row.get("event_slug") or ""),
                        ts=_fmt_value(row.get("team_set_match")),
                        ko=_fmt_value(row.get("kickoff_within_tolerance")),
                        delta=_fmt_value(row.get("kickoff_delta_sec")),
                        score=_fmt_value(row.get("score_tuple") or []),
                        rej=str(row.get("reject_reason") or ""),
                    )
                )
        return lines

    if mode == "trace":
        lines.append("Trace")
        trace_payload = notes.get("trace") if isinstance(notes.get("trace"), dict) else {}
        trace_text = json.dumps(trace_payload, indent=2, sort_keys=True, default=str).splitlines() or ["{}"]
        lines.extend([f"  {x}" for x in trace_text])
        return lines

    # card/markets mode: semantic multi-event hierarchy
    lines.append("Matched Events")
    if not selected_events:
        lines.append("  (none)")
    for idx, event in enumerate(selected_events, start=1):
        event_id = str(event.get("event_id") or "")
        is_primary = bool(event.get("is_primary"))
        expanded = event_id in expanded_event_ids
        mark = "-" if expanded else "+"
        primary = " primary" if is_primary else ""
        lines.append(
            "  [{mark}] ({idx}) event_id={eid}{primary}  kickoff_utc={kickoff}".format(
                mark=mark,
                idx=idx,
                eid=event_id,
                primary=primary,
                kickoff=_fmt_ts_utc(event.get("kickoff_ts_utc")),
            )
        )
        lines.append(f"      title={str(event.get('event_title') or '')}")
        lines.append(f"      slug={str(event.get('event_slug') or '')}")
        if not expanded:
            continue
        ev_markets = [m for m in market_blocks if str(m.get("event_id") or "") == event_id]
        ev_unselected = [m for m in unselected_market_blocks if str(m.get("event_id") or "") == event_id]
        if not ev_markets and (not show_unselected_markets or not ev_unselected):
            lines.append("      markets: (none)")
            continue
        lines.append("      markets:")
        for market in ev_markets:
            cond = str(market.get("condition_id") or "")
            shown_cond = cond if show_full_ids else _abbrev_id(cond, left=10, right=6)
            market_type = str(market.get("display_market_type") or "OTHER")
            line_display = str(market.get("line_display") or "").strip()
            line_suffix = f" | {line_display}" if line_display else ""
            status = "[✓]" if bool(market.get("is_tradeable")) else "[!]"
            lines.append(
                "        {status} {family} | {question}{line_suffix}".format(
                    status=status,
                    family=market_type,
                    question=str(market.get("market_question") or ""),
                    line_suffix=line_suffix,
                )
            )
            lines.append(
                "            condition={cond} type={stype} binding={binding}".format(
                    cond=shown_cond,
                    stype=str(market.get("sports_market_type") or ""),
                    binding=str(market.get("binding_status") or ""),
                )
            )
            outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
            for outcome in outcomes:
                token_id = str(outcome.get("token_id") or "")
                shown_token = token_id if show_full_ids else _abbrev_id(token_id)
                lines.append(
                    "            outcome {idx}: {label}  token={tok}  tradeable={tr}".format(
                        idx=int(outcome.get("outcome_index") or 0),
                        label=str(outcome.get("outcome_label") or ""),
                        tok=shown_token,
                        tr="yes" if int(outcome.get("is_tradeable") or 0) == 1 else "no",
                    )
                )
        if show_unselected_markets and ev_unselected:
            lines.append("      unselected markets:")
            for market in ev_unselected:
                cond = str(market.get("condition_id") or "")
                shown_cond = cond if show_full_ids else _abbrev_id(cond, left=10, right=6)
                market_type = str(market.get("display_market_type") or "OTHER")
                line_display = str(market.get("line_display") or "").strip()
                line_suffix = f" | {line_display}" if line_display else ""
                lines.append(
                    "        [X] {family} | {question}{line_suffix}".format(
                        family=market_type,
                        question=str(market.get("market_question") or ""),
                        line_suffix=line_suffix,
                    )
                )
                lines.append(
                    "            condition={cond} type={stype} binding=not_selected".format(
                        cond=shown_cond,
                        stype=str(market.get("sports_market_type") or ""),
                    )
                )
                if mode != "markets":
                    continue
                outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
                for outcome in outcomes:
                    token_id = str(outcome.get("token_id") or "")
                    shown_token = token_id if show_full_ids else _abbrev_id(token_id)
                    lines.append(
                        "            outcome {idx}: {label}  token={tok}  tradeable=no".format(
                            idx=int(outcome.get("outcome_index") or 0),
                            label=str(outcome.get("outcome_label") or ""),
                            tok=shown_token,
                        )
                    )
    if mode == "markets":
        return lines
    lines.append("")
    lines.append("Notes")
    lines.append("  reason_notes={}".format(",".join(notes.get("reason_notes") or [])))
    return lines


def _build_session_renderable(
    payload: dict[str, Any],
    *,
    queue_position: str,
    scope: str,
    filters_text: str,
    decision_progress: dict[str, Any],
    view_mode: str,
    scroll_offset: int,
    expanded_event_ids: set[str],
    show_full_ids: bool,
    show_unselected_markets: bool,
    last_action_note: str,
    candidates: list[dict[str, Any]] | None,
    console: Any,
) -> tuple[Any, int, int]:
    card = payload.get("card") if isinstance(payload.get("card"), dict) else {}
    event_resolution = card.get("event_resolution") if isinstance(card.get("event_resolution"), dict) else {}
    latest_decision = card.get("latest_decision") if isinstance(card.get("latest_decision"), dict) else {}
    markets = card.get("market_bindings") if isinstance(card.get("market_bindings"), dict) else {}
    decision = str(latest_decision.get("decision") or "")
    resolution_state = str(event_resolution.get("resolution_state") or "")
    is_tradeable = bool(markets.get("is_tradeable"))
    provider_game = card.get("provider_game") if isinstance(card.get("provider_game"), dict) else {}
    game_state = _derive_game_state(provider_game=provider_game, event_resolution=event_resolution)

    header = Text()
    header.append(f" {_decision_label(decision)} ", style=_decision_style(decision))
    header.append(" ")
    header.append(f" {game_state} ", style=_game_state_style(game_state))
    header.append(" ")
    header.append(f" {_tradeable_label(is_tradeable)} ", style=_tradeable_style(is_tradeable))
    header.append(" ")
    header.append(f" {resolution_state or 'UNKNOWN'} ", style=_resolution_style(resolution_state))
    header.append(f"   {queue_position}", style="bold cyan")
    header.append(f"   scope={scope}", style="dim")
    if filters_text:
        header.append(f"   {filters_text}", style="dim")

    lines = _build_card_document_lines(
        payload,
        view_mode=view_mode,
        expanded_event_ids=expanded_event_ids,
        show_full_ids=show_full_ids,
        show_unselected_markets=show_unselected_markets,
        candidates=candidates,
    )
    term_h = int(getattr(console.size, "height", 40) or 40)
    viewport_h = max(8, term_h - 10)
    total_lines = len(lines)
    max_scroll = max(0, total_lines - viewport_h)
    clamped_scroll = max(0, min(int(scroll_offset or 0), max_scroll))
    visible = lines[clamped_scroll : clamped_scroll + viewport_h]

    top_line = clamped_scroll + 1 if total_lines > 0 else 0
    bottom_line = min(total_lines, clamped_scroll + viewport_h)
    if clamped_scroll <= 0:
        scroll_state = "top"
    elif clamped_scroll >= max_scroll:
        scroll_state = "bottom"
    else:
        scroll_state = "middle"
    scroll_line = f"scroll {top_line}-{bottom_line}/{total_lines} ({scroll_state})"

    content_text = Text()
    for i, line in enumerate(visible):
        content_text.append_text(_styled_card_line(str(line)))
        if i < len(visible) - 1:
            content_text.append("\n")

    note = str(last_action_note or "").strip()
    summary = "pending={p} approved={a} rejected={r} skipped={s}".format(
        p=decision_progress.get("n_pending"),
        a=decision_progress.get("n_approved"),
        r=decision_progress.get("n_rejected"),
        s=decision_progress.get("n_skipped"),
    )
    footer = Text()
    if note:
        footer.append(note, style="bold yellow")
        footer.append("   ")
    footer.append(summary, style="bold cyan")
    footer.append("\n", style="white")
    footer.append(
        "keys: ←/→ cards  ↑/↓ scroll  PgUp/PgDn  Home/End  a/r/s decide  1-9 toggle-event  o toggle-all  x ids  u unselected  c/m/t views  q quit",
        style="dim",
    )

    frame = Group(
        Panel(header, border_style="cyan"),
        Panel(content_text, border_style="white", title=scroll_line),
        Panel(footer, border_style="blue"),
    )
    return (frame, clamped_scroll, max_scroll)


def _run_session_line_input_fallback(
    *,
    svc: Any,
    provider: str,
    rid: int,
    decision_filter: str,
    resolution_filter: str,
    parse_status: str,
    scope: str,
    limit: int,
    actor: str,
    logger: logging.Logger,
    include_inactive: bool,
) -> int:
    rows = svc.get_queue(
        provider=provider,
        run_id=rid,
        scope=scope,
        decision_filter=decision_filter,
        resolution_filter=resolution_filter,
        parse_status=parse_status,
        limit=limit,
        include_inactive=bool(include_inactive),
    )
    if not rows:
        logger.info("Review session queue is empty: provider=%s run_id=%s scope=%s", provider, rid, scope)
        return 0
    idx = 0
    while True:
        current = rows[idx]
        provider_game_id = str(current.get("provider_game_id") or "")
        card = svc.get_game_card(provider=provider, run_id=rid, provider_game_id=provider_game_id)
        progress = svc.get_decision_progress(
            provider=provider,
            run_id=rid,
            include_inactive=bool(include_inactive),
        )
        logger.info(
            "[%d/%d] scope=%s pending=%s approved=%s rejected=%s skipped=%s\n%s",
            idx + 1,
            len(rows),
            scope,
            progress.get("n_pending"),
            progress.get("n_approved"),
            progress.get("n_rejected"),
            progress.get("n_skipped"),
            _render_game_card_text(card),
        )
        try:
            action = input("[a]pprove [r]eject [s]kip [c]andidates [m]arkets [t]race [n]ext [p]rev [q]uit > ").strip().lower()
        except EOFError:
            action = "q"
        if action in {"q", "quit", "exit"}:
            return 0
        if action in {"n", "next", "right", "l"}:
            idx = (idx + 1) % len(rows)
            continue
        if action in {"p", "prev", "left", "h"}:
            idx = (idx - 1) % len(rows)
            continue
        if action in {"c", "cand", "candidates"}:
            cand_rows = svc.get_candidate_comparison(provider=provider, run_id=rid, provider_game_id=provider_game_id)
            logger.info(
                "Candidates for %s\n%s",
                provider_game_id,
                _render_table(
                    rows=cand_rows,
                    columns=[
                        ("candidate_rank", "rank"),
                        ("event_id", "event_id"),
                        ("event_slug", "event_slug"),
                        ("team_set_match", "team_set"),
                        ("kickoff_within_tolerance", "kickoff_ok"),
                        ("score_tuple", "score_tuple"),
                        ("is_selected", "selected"),
                        ("reject_reason", "reject_reason"),
                    ],
                ),
            )
            continue
        if action in {"m", "markets", "t", "trace"}:
            logger.info("%s", _render_game_card_text(card))
            continue
        decision_map = {"a": "approve", "approve": "approve", "r": "reject", "reject": "reject", "s": "skip", "skip": "skip"}
        chosen = decision_map.get(action)
        if not chosen:
            logger.warning("unknown session action: %s", action)
            continue
        try:
            svc.record_decision(
                provider=provider,
                run_id=rid,
                provider_game_id=provider_game_id,
                decision=chosen,
                note="",
                actor=actor,
            )
        except ValueError as exc:
            logger.error("decision failed: %s", str(exc))
            continue
        rows = svc.get_queue(
            provider=provider,
            run_id=rid,
            scope=scope,
            decision_filter=decision_filter,
            resolution_filter=resolution_filter,
            parse_status=parse_status,
            limit=limit,
            include_inactive=bool(include_inactive),
        )
        if not rows:
            logger.info("Review session queue is now empty: provider=%s run_id=%s scope=%s", provider, rid, scope)
            return 0
        idx = min(idx, len(rows) - 1)


def _run_session_interactive(
    *,
    svc: Any,
    provider: str,
    rid: int,
    decision_filter: str,
    resolution_filter: str,
    parse_status: str,
    scope: str,
    limit: int,
    actor: str,
    include_inactive: bool,
) -> int:
    console = Console()
    rows = svc.get_queue(
        provider=provider,
        run_id=rid,
        scope=scope,
        decision_filter=decision_filter,
        resolution_filter=resolution_filter,
        parse_status=parse_status,
        limit=limit,
        include_inactive=bool(include_inactive),
    )
    if not rows:
        console.print(Panel(f"Review session queue is empty\nprovider={provider} run_id={rid} scope={scope}", border_style="yellow"))
        return 0
    idx = 0
    scroll_offset = 0
    view_mode = "card"
    show_full_ids = False
    show_unselected_markets = False
    expanded_event_ids: set[str] = set()
    session_note = ""
    card_identity = ""
    sticky_card: dict[str, Any] | None = None
    sticky_provider_game_id = ""
    pending_rows: list[dict[str, Any]] | None = None

    def _default_expanded_ids(card_payload: dict[str, Any]) -> set[str]:
        card = card_payload.get("card") if isinstance(card_payload.get("card"), dict) else {}
        event_resolution = card.get("event_resolution") if isinstance(card.get("event_resolution"), dict) else {}
        selected_events = (
            event_resolution.get("selected_events")
            if isinstance(event_resolution.get("selected_events"), list)
            else []
        )
        if not selected_events:
            return set()
        primary = str(event_resolution.get("selected_event_id") or "")
        if primary:
            return {primary}
        first = str(selected_events[0].get("event_id") or "")
        return {first} if first else set()

    def _apply_pending_rows_if_any() -> tuple[bool, list[str]]:
        nonlocal rows, pending_rows, idx, sticky_card, sticky_provider_game_id
        if pending_rows is None:
            return (False, [])
        new_rows = list(pending_rows)
        pending_rows = None
        sticky_card = None
        sticky_provider_game_id = ""
        ids = [str(r.get("provider_game_id") or "") for r in new_rows]
        rows = new_rows
        if not rows:
            idx = 0
            return (True, ids)
        idx = max(0, min(idx, len(rows) - 1))
        return (True, ids)

    fd = sys.stdin.fileno()
    old_term = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        with Live(console=console, auto_refresh=False, screen=True) as live:
            while True:
                current = rows[idx]
                provider_game_id = str(current.get("provider_game_id") or "")
                if sticky_card is not None and sticky_provider_game_id == provider_game_id:
                    card = sticky_card
                else:
                    card = svc.get_game_card(provider=provider, run_id=rid, provider_game_id=provider_game_id)
                progress = svc.get_decision_progress(
                    provider=provider,
                    run_id=rid,
                    include_inactive=bool(include_inactive),
                )
                candidates = None
                if view_mode == "candidates":
                    candidates = svc.get_candidate_comparison(provider=provider, run_id=rid, provider_game_id=provider_game_id)
                new_identity = f"{provider_game_id}|{view_mode}"
                if new_identity != card_identity:
                    card_identity = new_identity
                    scroll_offset = 0
                    expanded_event_ids = _default_expanded_ids(card)
                filters_text = f"parse_status={parse_status or '*'} decision={decision_filter or '*'} resolution={resolution_filter or '*'}"
                renderable, scroll_offset, max_scroll = _build_session_renderable(
                    card,
                    queue_position=f"Game {idx + 1}/{len(rows)}",
                    view_mode=view_mode,
                    scope=scope,
                    filters_text=filters_text,
                    decision_progress=progress,
                    scroll_offset=scroll_offset,
                    expanded_event_ids=expanded_event_ids,
                    show_full_ids=show_full_ids,
                    show_unselected_markets=show_unselected_markets,
                    last_action_note=session_note,
                    candidates=candidates,
                    console=console,
                )
                live.update(renderable, refresh=True)
                key = _read_single_tty_key(timeout_s=0.2)
                if not key:
                    continue
                if key in {"q"}:
                    return 0
                if key in {"up"}:
                    scroll_offset = max(0, scroll_offset - 1)
                    continue
                if key in {"down"}:
                    scroll_offset = min(max_scroll, scroll_offset + 1)
                    continue
                if key == "pageup":
                    step = max(1, int(round((int(getattr(console.size, "height", 40) or 40) - 10) * 0.7)))
                    scroll_offset = max(0, scroll_offset - step)
                    continue
                if key == "pagedown":
                    step = max(1, int(round((int(getattr(console.size, "height", 40) or 40) - 10) * 0.7)))
                    scroll_offset = min(max_scroll, scroll_offset + step)
                    continue
                if key == "home":
                    scroll_offset = 0
                    continue
                if key == "end":
                    scroll_offset = max_scroll
                    continue
                if key in {"left", "h"}:
                    changed, ids = _apply_pending_rows_if_any()
                    if changed and not rows:
                        live.update(
                            Panel(
                                f"Review session queue is now empty\nprovider={provider} run_id={rid} scope={scope}",
                                border_style="green",
                                title="Completed",
                            ),
                            refresh=True,
                        )
                        time.sleep(0.8)
                        return 0
                    if ids:
                        if provider_game_id in ids:
                            idx = ids.index(provider_game_id)
                    idx = (idx - 1) % len(rows)
                    session_note = ""
                    continue
                if key in {"right", "l"}:
                    changed, ids = _apply_pending_rows_if_any()
                    if changed and not rows:
                        live.update(
                            Panel(
                                f"Review session queue is now empty\nprovider={provider} run_id={rid} scope={scope}",
                                border_style="green",
                                title="Completed",
                            ),
                            refresh=True,
                        )
                        time.sleep(0.8)
                        return 0
                    if ids:
                        if provider_game_id in ids:
                            idx = ids.index(provider_game_id)
                    idx = (idx + 1) % len(rows)
                    session_note = ""
                    continue
                if key == "c":
                    view_mode = "candidates"
                    session_note = ""
                    continue
                if key == "m":
                    view_mode = "markets"
                    session_note = ""
                    continue
                if key == "t":
                    view_mode = "trace"
                    session_note = ""
                    continue
                if key == "x":
                    show_full_ids = not show_full_ids
                    session_note = "token ids: full" if show_full_ids else "token ids: abbreviated"
                    continue
                if key == "u":
                    show_unselected_markets = not show_unselected_markets
                    session_note = (
                        "unselected markets: visible"
                        if show_unselected_markets
                        else "unselected markets: hidden"
                    )
                    continue
                if key in {"1", "2", "3", "4", "5", "6", "7", "8", "9"}:
                    card_data = card.get("card") if isinstance(card.get("card"), dict) else {}
                    event_resolution = (
                        card_data.get("event_resolution")
                        if isinstance(card_data.get("event_resolution"), dict)
                        else {}
                    )
                    selected_events = (
                        event_resolution.get("selected_events")
                        if isinstance(event_resolution.get("selected_events"), list)
                        else []
                    )
                    idx_event = int(key) - 1
                    if 0 <= idx_event < len(selected_events):
                        eid = str(selected_events[idx_event].get("event_id") or "")
                        if eid:
                            if eid in expanded_event_ids:
                                expanded_event_ids.remove(eid)
                            else:
                                expanded_event_ids.add(eid)
                    continue
                if key == "o":
                    card_data = card.get("card") if isinstance(card.get("card"), dict) else {}
                    event_resolution = (
                        card_data.get("event_resolution")
                        if isinstance(card_data.get("event_resolution"), dict)
                        else {}
                    )
                    selected_events = (
                        event_resolution.get("selected_events")
                        if isinstance(event_resolution.get("selected_events"), list)
                        else []
                    )
                    all_event_ids = {str(ev.get("event_id") or "") for ev in selected_events if str(ev.get("event_id") or "")}
                    if all_event_ids and all_event_ids.issubset(expanded_event_ids):
                        expanded_event_ids = _default_expanded_ids(card)
                    else:
                        expanded_event_ids = set(all_event_ids)
                    continue
                if key in {"a", "r", "s"}:
                    chosen = {"a": "approve", "r": "reject", "s": "skip"}[key]
                    try:
                        svc.record_decision(
                            provider=provider,
                            run_id=rid,
                            provider_game_id=provider_game_id,
                            decision=chosen,
                            note="",
                            actor=actor,
                        )
                    except ValueError as exc:
                        session_note = f"decision failed: {str(exc)}"
                        continue
                    session_note = f"decision applied: {chosen.upper()} for {provider_game_id}"
                    sticky_card = svc.get_game_card(provider=provider, run_id=rid, provider_game_id=provider_game_id)
                    sticky_provider_game_id = provider_game_id
                    pending_rows = svc.get_queue(
                        provider=provider,
                        run_id=rid,
                        scope=scope,
                        decision_filter=decision_filter,
                        resolution_filter=resolution_filter,
                        parse_status=parse_status,
                        limit=limit,
                        include_inactive=bool(include_inactive),
                    )
                    continue
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_term)



__all__ = [
    "_RICH_AVAILABLE",
    "_SESSION_STYLE_MAP",
    "_build_card_document_lines",
    "_build_session_renderable",
    "_decode_session_key",
    "_derive_game_state",
    "_kv_value_style",
    "_styled_card_line",
    "_render_game_card_text",
    "_interactive_session_available",
    "_run_session_line_input_fallback",
    "_run_session_interactive",
]
