"""Link review rendering/session helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import io
import json
import logging
import os
import select
import sys
import termios
import time
from typing import Any
import tty

from polybot2._cli.common import _int_or_none, _render_table

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

_PROVIDER_TIMEZONE = {"boltodds": "ET", "kalstrop_v1": "UTC", "kalstrop_v2": "UTC", "kalstrop_opta": "UTC"}
_PROVIDER_ID_LABEL = {"boltodds": "universal_id", "kalstrop_v1": "fixture_id", "kalstrop_v2": "event_id", "kalstrop_opta": "event_id"}


def _render_provider_panels_text(all_pgs: list[dict[str, Any]], primary_pg: dict[str, Any], width: int) -> list[Any]:
    """Render provider games as side-by-side Rich panels, return as pre-styled Text lines."""
    if not _RICH_AVAILABLE:
        return []
    from rich.columns import Columns as RichColumns

    _lbl = "grey70"
    _val = "bold bright_white"

    panels = []
    for _pg in all_pgs:
        _prov = str(_pg.get("provider") or "")
        _gid = str(_pg.get("provider_game_id") or "")
        _home = str(_pg.get("home_raw") or "")
        _away = str(_pg.get("away_raw") or "")
        _when = str(_pg.get("when_raw") or "")
        _tz = _PROVIDER_TIMEZONE.get(_prov, "UTC")
        _league = str(_pg.get("league_raw") or "") or str(_pg.get("sport_raw") or "")
        _is_primary = (_pg is primary_pg)

        title = Text()
        title.append(f" {_prov} ", style="bold" if _is_primary else "")
        if _is_primary:
            title.append("* ", style="bold bright_magenta")
        if _prov == "kalstrop_v2":
            title.append("! ", style="bold red")

        body = Text()
        for i, (label, value) in enumerate([
            ("ID: ", _gid),
            ("League: ", _league),
            ("When: ", f"{_when} {_tz}"),
            ("Home: ", _home),
            ("Away: ", _away),
        ]):
            if i > 0:
                body.append("\n")
            body.append(label, style=_lbl)
            body.append(value, style=_val)

        content_width = max(len(f"{label}{value}") for label, value in [
            ("ID: ", _gid), ("League: ", _league), ("When: ", f"{_when} {_tz}"),
            ("Home: ", _home), ("Away: ", _away),
        ])
        panel_width = max(20, len(_prov) + 8, content_width + 4)
        panels.append(Panel(body, title=title, title_align="left", border_style="grey50", width=panel_width))

    if len(panels) == 1:
        content = panels[0]
    else:
        content = RichColumns(panels, padding=(0, 1), equal=False)

    outer = Panel(content, title="Provider Games", title_align="left", border_style="cyan",
                  style="bold bright_cyan", subtitle_align="left")
    buf = io.StringIO()
    temp_console = Console(file=buf, width=min(width, 160), highlight=False, force_terminal=True, color_system="truecolor")
    temp_console.print(outer)
    raw_lines = buf.getvalue().rstrip("\n").splitlines()
    return [Text.from_ansi(l) for l in raw_lines]


def _render_market_summary_table(targets: list[dict[str, Any]], width: int) -> list[Any]:
    """Render a grouped market summary table as pre-styled Text lines."""
    if not _RICH_AVAILABLE or not targets:
        return []

    groups: dict[str, dict[str, Any]] = {}
    for t in targets:
        mt = str(t.get("sports_market_type") or "unknown")
        if mt not in groups:
            groups[mt] = {"markets": set(), "targets": 0, "tradeable": 0, "lines": []}
        groups[mt]["markets"].add(str(t.get("condition_id") or ""))
        groups[mt]["targets"] += 1
        if int(t.get("is_tradeable") or 0) == 1:
            groups[mt]["tradeable"] += 1
        ln = t.get("line")
        if ln is not None and ln not in groups[mt]["lines"]:
            groups[mt]["lines"].append(ln)

    tbl = Table(box=box.SIMPLE_HEAVY, header_style="bold bright_cyan", padding=(0, 1), expand=True)
    tbl.add_column("type", style="bold bright_white", no_wrap=True)
    tbl.add_column("markets", justify="right", style="bright_white")
    tbl.add_column("targets", justify="right", style="bright_white")
    tbl.add_column("tradeable", justify="right")
    tbl.add_column("lines", style="grey70")

    total_markets = 0
    total_targets = 0
    total_tradeable = 0

    for mt in sorted(groups.keys()):
        g = groups[mt]
        n_markets = len(g["markets"])
        n_targets = g["targets"]
        n_tradeable = g["tradeable"]
        total_markets += n_markets
        total_targets += n_targets
        total_tradeable += n_tradeable

        if n_tradeable == n_targets:
            tr_style = "bold green"
        elif n_tradeable > 0:
            tr_style = "bold yellow"
        else:
            tr_style = "bold red"

        lns = sorted(g["lines"], key=lambda x: (float(x) if isinstance(x, (int, float)) else 0))
        if len(lns) <= 5:
            lines_str = ", ".join(str(l) for l in lns)
        else:
            lines_str = ", ".join(str(l) for l in lns[:5]) + f" +{len(lns) - 5}"

        tbl.add_row(
            mt,
            str(n_markets),
            str(n_targets),
            Text(str(n_tradeable), style=tr_style),
            lines_str,
        )

    tbl.add_section()
    if total_tradeable == total_targets:
        tot_tr_style = "bold green"
    elif total_tradeable > 0:
        tot_tr_style = "bold yellow"
    else:
        tot_tr_style = "bold red"
    tbl.add_row(
        Text("Total", style="bold"),
        Text(str(total_markets), style="bold"),
        Text(str(total_targets), style="bold"),
        Text(str(total_tradeable), style=tot_tr_style),
        "",
    )

    outer = Panel(tbl, title="Market Targets", title_align="left", border_style="cyan")
    buf = io.StringIO()
    temp_console = Console(file=buf, width=min(width, 160), highlight=False, force_terminal=True, color_system="truecolor")
    temp_console.print(outer)
    raw_lines = buf.getvalue().rstrip("\n").splitlines()
    return [Text.from_ansi(l) for l in raw_lines]


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

    # Provider Games panel (columnar: primary + siblings)
    rich_provider_name = str(provider_game.get("provider") or payload.get("provider") or "")
    siblings = card.get("provider_siblings", [])
    all_provider_games = [provider_game] + siblings

    provider_grid = Table.grid(padding=(0, 3))
    col_renderables = []
    for _pg in all_provider_games:
        _prov = str(_pg.get("provider") or "")
        _is_primary = (_pg is provider_game)
        _id_label = _PROVIDER_ID_LABEL.get(_prov, "ID")
        _gid = str(_pg.get("provider_game_id") or "")
        _id_display = _gid[:28] if len(_gid) > 28 else _gid
        _tz = _PROVIDER_TIMEZONE.get(_prov, "UTC")
        _when = str(_pg.get("when_raw") or "")
        _prov_display = f"[bold green]{_prov}[/bold green]" if _is_primary else _prov
        _col_rows = [
            ("Provider", _prov_display),
            (_id_label, _id_display),
            ("Home", _pg.get("home_raw", "")),
            ("Away", _pg.get("away_raw", "")),
            ("When", f"{_when} ({_tz})" if _when else ""),
        ]
        col_renderables.append(_kv_table(_col_rows))
        provider_grid.add_column()

    provider_grid.add_row(*col_renderables)
    provider_section = Panel(
        provider_grid,
        title=f"Provider Games ({len(all_provider_games)})",
        border_style="cyan",
    )

    # Canonicalization panel with team mapping trace
    rich_home_raw = str(provider_game.get("home_raw") or "")
    rich_away_raw = str(provider_game.get("away_raw") or "")
    rich_canonical_home = str(canonical.get("canonical_home_team") or "")
    rich_canonical_away = str(canonical.get("canonical_away_team") or "")

    home_trace = f'"{rich_home_raw}" → {rich_canonical_home}' if rich_canonical_home else f'"{rich_home_raw}" → (unmapped)'
    away_trace = f'"{rich_away_raw}" → {rich_canonical_away}' if rich_canonical_away else f'"{rich_away_raw}" → (unmapped)'

    # Compact canonicalization as a single line
    canon_parts: list[str] = []
    _canon_league = str(canonical.get("canonical_league") or "")
    if _canon_league:
        canon_parts.append(f"[bold]{_canon_league}[/bold]")
    canon_parts.append(f"Home: {home_trace}")
    canon_parts.append(f"Away: {away_trace}")
    _slug_hint = str(canonical.get("event_slug_prefix") or "")
    if _slug_hint:
        canon_parts.append(f"Slug: {_slug_hint}")
    rich_kickoff_ts = provider_game.get("kickoff_ts_utc")
    rich_kickoff_str = ""
    if rich_kickoff_ts:
        rich_kickoff_iv = _int_or_none(rich_kickoff_ts)
        if rich_kickoff_iv is not None:
            rich_kickoff_str = datetime.fromtimestamp(rich_kickoff_iv, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            canon_parts.append(f"Kickoff: {rich_kickoff_str}")
    rich_delta_sec = event_resolution.get("kickoff_delta_sec") if event_resolution else None
    if rich_delta_sec is not None:
        rich_delta_iv = _int_or_none(rich_delta_sec)
        if rich_delta_iv is not None:
            canon_parts.append(f"Δ: {rich_delta_iv // 60}min")

    canonical_section = Panel(
        Text.from_markup("  ".join(canon_parts)),
        title="Canonicalization",
        border_style="cyan",
    )
    # Event Resolution panel with unresolved warning
    rich_resolution_state = str(event_resolution.get("resolution_state") or "")
    rich_reason_code = str(event_resolution.get("reason_code") or "")
    _rich_resolved_states = {"MATCHED_CLEAN", "MATCHED_WITH_WARNINGS"}

    event_kv_rows: list[tuple[str, Any]] = []
    if rich_resolution_state and rich_resolution_state.upper() not in _rich_resolved_states:
        # Prominent unresolved state
        unresolved_detail = ""
        if "team_alias_unmapped" in rich_reason_code:
            if not rich_canonical_home:
                unresolved_detail = f'"{rich_home_raw}" has no alias for {rich_provider_name} in league {canonical.get("canonical_league") or "?"}'
            elif not rich_canonical_away:
                unresolved_detail = f'"{rich_away_raw}" has no alias for {rich_provider_name} in league {canonical.get("canonical_league") or "?"}'
        event_kv_rows.append(("State", f"UNRESOLVED: {rich_reason_code}"))
        if unresolved_detail:
            event_kv_rows.append(("Detail", unresolved_detail))
    else:
        event_kv_rows.append(("Reason code", rich_reason_code))

    event_kv_rows.extend([
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
        ("Score tuple", event_resolution.get("score_tuple") or []),
        ("Slug fallback used", event_resolution.get("used_slug_fallback")),
    ])

    event_border = "red" if (rich_resolution_state and rich_resolution_state.upper() not in _rich_resolved_states) else "cyan"
    event_section = Panel(
        _kv_table(event_kv_rows),
        title="Event Resolution",
        border_style=event_border,
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

    # Full per-token targets table (shown in "markets" mode)
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

    # Condensed grouped summary table (shown in default card mode)
    from collections import defaultdict as _defaultdict
    _type_groups: dict[str, dict[str, Any]] = {}
    for t in targets:
        mt = str(t.get("sports_market_type") or "unknown")
        if mt not in _type_groups:
            _type_groups[mt] = {"markets": set(), "targets": 0, "tradeable": 0, "lines": []}
        _type_groups[mt]["markets"].add(str(t.get("condition_id") or ""))
        _type_groups[mt]["targets"] += 1
        if str(t.get("binding_status") or "").lower() == "tradeable":
            _type_groups[mt]["tradeable"] += 1
        _line = t.get("line")
        if _line is not None and _line not in _type_groups[mt]["lines"]:
            _type_groups[mt]["lines"].append(_line)

    targets_summary_table = Table(box=box.SIMPLE_HEAVY, header_style="bold magenta")
    targets_summary_table.add_column("type", no_wrap=True)
    targets_summary_table.add_column("markets", justify="right")
    targets_summary_table.add_column("targets", justify="right")
    targets_summary_table.add_column("tradeable", justify="right")
    targets_summary_table.add_column("lines")
    _total_markets = 0
    _total_targets = 0
    _total_tradeable = 0
    for _mt in sorted(_type_groups.keys()):
        _info = _type_groups[_mt]
        _n_markets = len(_info["markets"])
        _total_markets += _n_markets
        _total_targets += _info["targets"]
        _total_tradeable += _info["tradeable"]
        _lines_list = _info["lines"]
        _lines_str = ", ".join(str(l) for l in _lines_list[:5])
        if len(_lines_list) > 5:
            _lines_str += f" +{len(_lines_list) - 5}"
        targets_summary_table.add_row(
            _mt, str(_n_markets), str(_info["targets"]),
            str(_info["tradeable"]), _lines_str,
        )
    targets_summary_table.add_row(
        "[bold]Total[/bold]", f"[bold]{_total_markets}[/bold]",
        f"[bold]{_total_targets}[/bold]", f"[bold]{_total_tradeable}[/bold]", "",
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
        content_blocks.extend([market_summary_section, Panel(targets_table, title="Market Targets (full)", border_style="magenta"), notes_section])
    elif mode == "trace":
        trace_payload = notes.get("trace") if isinstance(notes.get("trace"), dict) else {}
        trace_json = json.dumps(trace_payload, indent=2, sort_keys=True, default=str)
        content_blocks.append(Panel(trace_json, title="Deterministic Trace", border_style="magenta"))
        content_blocks.append(notes_section)
    else:
        content_blocks.extend([Panel(targets_summary_table, title="Market Targets", border_style="magenta"), notes_section])

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
    console_width: int = 100,
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

    # Provider Games section (Rich panels side-by-side)
    siblings = card.get("provider_siblings", [])
    all_pgs = [provider_game] + siblings
    provider_name = str(provider_game.get("provider") or payload.get("provider") or "")

    panel_lines = _render_provider_panels_text(all_pgs, provider_game, width=console_width)
    if panel_lines:
        lines.extend(panel_lines)
    else:
        lines.append(f"Provider Games ({len(all_pgs)})")
        for _pg in all_pgs:
            _prov = str(_pg.get("provider") or "")
            _id_label = _PROVIDER_ID_LABEL.get(_prov, "id")
            _gid = str(_pg.get("provider_game_id") or "")
            _home = str(_pg.get("home_raw") or "")
            _away = str(_pg.get("away_raw") or "")
            _when = str(_pg.get("when_raw") or "")
            _tz = _PROVIDER_TIMEZONE.get(_prov, "UTC")
            _is_primary = (_pg is provider_game)
            _marker = " *" if _is_primary else ""
            lines.append(f"  {_prov}{_marker}: {_id_label}={_gid[:30]}")
            lines.append(f"    \"{_home}\" vs \"{_away}\"  {_when} ({_tz})")
    lines.append("")

    # Canonicalization section (compact)
    home_raw = str(provider_game.get("home_raw") or "")
    away_raw = str(provider_game.get("away_raw") or "")
    canonical_home = str(canonical.get("canonical_home_team") or "")
    canonical_away = str(canonical.get("canonical_away_team") or "")
    _home_trace = f'"{home_raw}" → {canonical_home}' if canonical_home else f'"{home_raw}" → (unmapped)'
    _away_trace = f'"{away_raw}" → {canonical_away}' if canonical_away else f'"{away_raw}" → (unmapped)'

    lines.append("Canonicalization")
    lines.append(f"  league={canonical.get('canonical_league', '')}  slug-hint={canonical.get('event_slug_prefix', '')}")
    lines.append(f"  home: {_home_trace}")
    lines.append(f"  away: {_away_trace}")

    # Kickoff UTC (moved from provider game)
    kickoff_ts = provider_game.get("kickoff_ts_utc")
    if kickoff_ts:
        kickoff_iv = _int_or_none(kickoff_ts)
        if kickoff_iv is not None:
            kickoff_str = datetime.fromtimestamp(kickoff_iv, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            lines.append(f"  kickoff_utc={kickoff_str}")

    # Kickoff delta
    delta_sec = event_resolution.get("kickoff_delta_sec") if event_resolution else None
    if delta_sec is not None:
        delta_iv = _int_or_none(delta_sec)
        if delta_iv is not None:
            delta_min = delta_iv // 60
            lines.append(f"  kickoff_delta={delta_min} min")

    lines.append("")
    lines.append("Event Resolution")
    resolution_state = str(event_resolution.get("resolution_state") or "")
    reason_code = str(event_resolution.get("reason_code") or "")
    _resolved_states = {"MATCHED_CLEAN", "MATCHED_WITH_WARNINGS"}

    # Prominent unresolved warning
    if resolution_state and resolution_state.upper() not in _resolved_states:
        detail = ""
        if "team_alias_unmapped" in reason_code:
            if not canonical_home:
                detail = f'"{home_raw}" has no alias for {provider_name} in league {canonical.get("canonical_league") or "?"}'
            elif not canonical_away:
                detail = f'"{away_raw}" has no alias for {provider_name} in league {canonical.get("canonical_league") or "?"}'
        lines.append(f"  state=UNRESOLVED: {reason_code}")
        if detail:
            lines.append(f"    {detail}")
    else:
        lines.append(
            "  game_state={game_state}  state={state}  reason={reason}".format(
                game_state=game_state,
                state=resolution_state,
                reason=reason_code,
            )
        )

    lines.append(
        "  selected_events={n}  tradeable_targets={tt}/{t}  markets={sm}/{tm}".format(
            n=len(selected_events),
            tt=int(markets.get("n_tradeable_targets") or 0),
            t=int(markets.get("n_targets") or 0),
            sm=int(markets.get("n_selected_markets") or len(market_blocks)),
            tm=int(markets.get("n_total_markets") or len(market_blocks)),
        )
    )
    lines.append(
        "  primary_event={eid}  score={score}".format(
            eid=str(event_resolution.get("selected_event_id") or ""),
            score=_fmt_value(event_resolution.get("score_tuple") or []),
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

    if mode == "markets":
        _all_targets = markets.get("targets") if isinstance(markets.get("targets"), list) else []
        summary_lines = _render_market_summary_table(_all_targets, width=console_width)
        if summary_lines:
            lines.extend(summary_lines)
        else:
            lines.append("Market Targets")
            lines.append("  (no targets)")
        lines.append("")
        lines.append("Notes")
        lines.append("  reason_notes={}".format(",".join(notes.get("reason_notes") or [])))
        return lines

    # Matched Events (default card view)
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

    term_w = int(getattr(console.size, "width", 100) or 100)
    lines = _build_card_document_lines(
        payload,
        view_mode=view_mode,
        expanded_event_ids=expanded_event_ids,
        show_full_ids=show_full_ids,
        show_unselected_markets=show_unselected_markets,
        candidates=candidates,
        console_width=max(60, term_w - 4),
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
        if isinstance(line, Text):
            content_text.append_text(line)
        else:
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
                    view_mode = "card" if view_mode == "markets" else "markets"
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
    "_interactive_session_available",
    "_run_session_line_input_fallback",
    "_run_session_interactive",
]
