# Copyright (c) 2026 Continental. All rights reserved.
# Licensed under the Vanguard Proprietary Source-Available License (see /LICENSE).

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import discord
from discord.ext import commands

from data_paths import resolve_data_file

DATA_FILE = resolve_data_file("votes.json")
ACTIVE_VIEWS: dict[str, "VoteView"] = {}
ACTIVE_FINISH_TASKS: dict[str, asyncio.Task] = {}

MAX_DURATION_HOURS = 168
MAX_OPTIONS = 10


def _as_int(value: Any, default: int | None = None) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "on"}:
            return True
        if token in {"0", "false", "no", "off"}:
            return False
    return default


def _clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(maximum, value))


def _write_votes_atomic(payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    temp_path = f"{DATA_FILE}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
    os.replace(temp_path, DATA_FILE)


def _parse_datetime_utc(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _started_at_from_vote_id(vote_id: str) -> datetime | None:
    try:
        started_ts = int(str(vote_id).rsplit("-", 1)[-1])
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(started_ts, tz=timezone.utc)


def _get_vote_finish_time(vote_id: str, vote: dict[str, Any]) -> datetime | None:
    finish_time = _parse_datetime_utc(vote.get("finish_at"))
    if finish_time:
        return finish_time

    duration_hours = _as_int(vote.get("duration_hours"), 24)
    if duration_hours is None:
        duration_hours = 24
    if duration_hours < 1:
        duration_hours = 1

    started_at = _started_at_from_vote_id(vote_id)
    if started_at is None:
        return None
    return started_at + timedelta(hours=duration_hours)


def _normalize_option_id(value: Any, index: int) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    if not text:
        text = f"option-{index + 1}"
    return text[:32]


def _default_options_for_vote_type(vote_type: str) -> list[dict[str, Any]]:
    if vote_type == "confidence":
        return [
            {"id": "against", "label": "Against"},
            {"id": "support", "label": "Support"},
        ]
    if vote_type == "yesno":
        return [
            {"id": "yes", "label": "Yes"},
            {"id": "no", "label": "No"},
        ]
    return [
        {"id": "option-1", "label": "Option 1"},
        {"id": "option-2", "label": "Option 2"},
    ]


def _normalize_options(raw_options: Any, vote_type: str) -> list[dict[str, Any]]:
    if not isinstance(raw_options, list):
        return _default_options_for_vote_type(vote_type)

    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for index, entry in enumerate(raw_options):
        if len(normalized) >= MAX_OPTIONS:
            break

        if isinstance(entry, str):
            label = entry.strip()
            option_id = _normalize_option_id(label, index)
            description = ""
            member_id = None
        elif isinstance(entry, dict):
            label = str(entry.get("label") or entry.get("name") or "").strip()
            description = str(entry.get("description") or "").strip()[:100]
            option_id = _normalize_option_id(entry.get("id") or label, index)
            member_id = _as_int(entry.get("member_id"))
        else:
            continue

        if not label:
            continue

        if option_id in seen_ids:
            option_id = _normalize_option_id(f"{option_id}-{index + 1}", index)

        seen_ids.add(option_id)
        normalized.append(
            {
                "id": option_id,
                "label": label[:80],
                "description": description,
                "member_id": member_id,
            }
        )

    if len(normalized) < 2:
        return _default_options_for_vote_type(vote_type)
    return normalized


def _normalize_ballot_choice(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        token = value.strip()
        return [token] if token else []
    return []


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


def _sanitize_ballots(
    raw_ballots: Any,
    option_ids: set[str],
    ballot_mode: str,
    max_choices: int,
) -> dict[str, list[str]]:
    if not isinstance(raw_ballots, dict):
        return {}

    ballots: dict[str, list[str]] = {}
    for user_id, raw_choice in raw_ballots.items():
        user_key = _as_int(user_id)
        if user_key is None:
            continue

        choices = _normalize_ballot_choice(raw_choice)
        choices = [choice for choice in choices if choice in option_ids]
        choices = _dedupe_keep_order(choices)
        if not choices:
            continue

        if ballot_mode == "single":
            ballots[str(user_key)] = [choices[0]]
        else:
            ballots[str(user_key)] = choices[:max_choices]

    return ballots


def _build_legacy_votes_map(ballots: dict[str, list[str]]) -> dict[str, str]:
    legacy: dict[str, str] = {}
    for user_id, choices in ballots.items():
        if not isinstance(choices, list) or not choices:
            continue
        first = str(choices[0]).strip()
        if first:
            legacy[str(user_id)] = first
    return legacy


def _normalize_vote_type(raw: Any, options: list[dict[str, Any]], payload: dict[str, Any]) -> str:
    token = str(raw or "").strip().lower()
    if token in {"confidence", "yesno", "proposal", "approval", "election"}:
        return token

    option_ids = {opt.get("id") for opt in options}
    if {"against", "support"}.issubset(option_ids):
        return "confidence"
    if {"yes", "no"}.issubset(option_ids):
        return "yesno"
    if payload.get("target_id") is not None:
        return "confidence"
    return "proposal"


def _normalize_vote(vote_id: str, payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    channel_id = _as_int(payload.get("channel_id"))
    message_id = _as_int(payload.get("message_id"))
    starter_id = _as_int(payload.get("starter_id"))
    if channel_id is None or message_id is None or starter_id is None:
        return None

    raw_vote_type = str(payload.get("vote_type") or "").strip().lower()
    initial_options = _normalize_options(payload.get("options"), raw_vote_type or "proposal")
    vote_type = _normalize_vote_type(raw_vote_type, initial_options, payload)
    options = _normalize_options(payload.get("options"), vote_type)
    option_ids = {opt["id"] for opt in options}

    ballot_mode = str(payload.get("ballot_mode") or "").strip().lower()
    if ballot_mode not in {"single", "multiple"}:
        ballot_mode = "multiple" if vote_type == "approval" else "single"

    max_choices_default = 1 if ballot_mode == "single" else min(3, len(options))
    max_choices = _as_int(payload.get("max_choices"), max_choices_default)
    if max_choices is None:
        max_choices = max_choices_default
    max_choices = _clamp(max_choices, 1, len(options))
    if ballot_mode == "single":
        max_choices = 1

    legacy_votes = payload.get("votes", {})
    raw_ballots = payload.get("ballots", legacy_votes)
    ballots = _sanitize_ballots(raw_ballots, option_ids, ballot_mode, max_choices)
    votes_map = _build_legacy_votes_map(ballots)

    target_id = _as_int(payload.get("target_id"))
    if vote_type == "confidence" and target_id is None:
        return None

    target_name = str(payload.get("target_name") or "").strip()
    if target_id is not None and not target_name:
        target_name = f"User {target_id}"

    duration_hours = _as_int(payload.get("duration_hours"), 24)
    if duration_hours is None:
        duration_hours = 24
    duration_hours = _clamp(duration_hours, 1, MAX_DURATION_HOURS)

    finish_time = _get_vote_finish_time(vote_id, payload)
    if finish_time is None:
        return None

    created_at = _parse_datetime_utc(payload.get("created_at"))
    if created_at is None:
        created_at = _started_at_from_vote_id(vote_id) or datetime.now(timezone.utc)

    min_account_days = _as_int(payload.get("min_account_days"), 7 if vote_type == "confidence" else 0)
    min_join_days = _as_int(payload.get("min_join_days"), 1 if vote_type == "confidence" else 0)
    if min_account_days is None:
        min_account_days = 0
    if min_join_days is None:
        min_join_days = 0
    min_account_days = max(0, min_account_days)
    min_join_days = max(0, min_join_days)

    quorum = _as_int(payload.get("quorum"), 0)
    quorum = max(0, quorum or 0)

    pass_threshold_percent = _as_int(payload.get("pass_threshold_percent"), 50)
    if pass_threshold_percent is None:
        pass_threshold_percent = 50
    pass_threshold_percent = _clamp(pass_threshold_percent, 1, 100)

    seats = _as_int(payload.get("seats"), 1)
    if seats is None:
        seats = 1
    seats = _clamp(seats, 1, max(1, len(options)))

    primary_option_id = str(payload.get("primary_option_id") or options[0]["id"]).strip()
    if primary_option_id not in option_ids:
        primary_option_id = options[0]["id"]

    title = str(payload.get("title") or "").strip()
    if not title:
        if vote_type == "confidence":
            title = "Emergency Vote: No Confidence"
        elif vote_type == "election":
            title = "Election"
        elif vote_type == "yesno":
            title = "Yes/No Vote"
        elif vote_type == "approval":
            title = "Approval Vote"
        else:
            title = "Community Vote"

    description = str(payload.get("description") or "").strip()[:1000]
    if vote_type == "confidence" and target_name and not description:
        description = f"A no-confidence vote has been started against **{target_name}**."

    normalized = {
        "id": vote_id,
        "schema_version": 2,
        "vote_type": vote_type,
        "title": title[:120],
        "description": description,
        "options": options,
        "ballot_mode": ballot_mode,
        "max_choices": max_choices,
        "ballots": ballots,
        "votes": votes_map,
        "starter_id": starter_id,
        "target_id": target_id,
        "target_name": target_name[:100],
        "channel_id": channel_id,
        "message_id": message_id,
        "duration_hours": duration_hours,
        "created_at": created_at.isoformat(),
        "finish_at": finish_time.isoformat(),
        "min_account_days": min_account_days,
        "min_join_days": min_join_days,
        "quorum": quorum,
        "pass_threshold_percent": pass_threshold_percent,
        "primary_option_id": primary_option_id,
        "anonymous": _as_bool(payload.get("anonymous"), False),
        "show_live_results": _as_bool(payload.get("show_live_results"), True),
        "eligible_role_id": _as_int(payload.get("eligible_role_id")),
        "seats": seats,
        "runoff_enabled": _as_bool(payload.get("runoff_enabled"), False),
        "runoff_from": str(payload.get("runoff_from") or "").strip() or None,
        "delete_channel_after_close": _as_bool(payload.get("delete_channel_after_close"), False),
        "delete_delay_seconds": max(0, _as_int(payload.get("delete_delay_seconds"), 3600) or 3600),
    }

    return normalized


def _load_votes() -> dict[str, dict[str, Any]]:
    if not os.path.exists(DATA_FILE):
        try:
            _write_votes_atomic({})
        except OSError:
            pass
        return {}

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as handle:
            raw_votes = json.load(handle)
    except (json.JSONDecodeError, OSError):
        try:
            _write_votes_atomic({})
        except OSError:
            pass
        return {}

    if not isinstance(raw_votes, dict):
        return {}

    normalized: dict[str, dict[str, Any]] = {}
    for vote_id, payload in raw_votes.items():
        clean = _normalize_vote(str(vote_id), payload)
        if clean is not None:
            normalized[str(vote_id)] = clean
    return normalized


def save_votes() -> None:
    try:
        _write_votes_atomic(votes)
    except OSError as exc:
        print("Failed to save votes:", exc)


votes = _load_votes()


def _vote_option_map(vote: dict[str, Any]) -> dict[str, dict[str, Any]]:
    options = vote.get("options", [])
    if not isinstance(options, list):
        return {}
    output: dict[str, dict[str, Any]] = {}
    for option in options:
        if not isinstance(option, dict):
            continue
        option_id = str(option.get("id") or "").strip()
        if not option_id:
            continue
        output[option_id] = option
    return output


def _clean_ballot_map(vote: dict[str, Any]) -> dict[str, list[str]]:
    option_ids = set(_vote_option_map(vote).keys())
    ballot_mode = str(vote.get("ballot_mode") or "single")
    max_choices = _as_int(vote.get("max_choices"), 1) or 1
    if ballot_mode == "single":
        max_choices = 1
    return _sanitize_ballots(vote.get("ballots", {}), option_ids, ballot_mode, max_choices)


def _sync_legacy_vote_fields(vote: dict[str, Any]) -> dict[str, Any]:
    ballots = _clean_ballot_map(vote)
    vote["ballots"] = ballots
    vote["votes"] = _build_legacy_votes_map(ballots)
    return vote


def tally_vote(vote: dict[str, Any]) -> tuple[dict[str, int], int]:
    option_map = _vote_option_map(vote)
    tallies = {option_id: 0 for option_id in option_map}
    ballots = _clean_ballot_map(vote)
    ballot_mode = str(vote.get("ballot_mode") or "single")

    for choices in ballots.values():
        if not choices:
            continue
        if ballot_mode == "multiple":
            for choice in choices:
                if choice in tallies:
                    tallies[choice] += 1
        else:
            choice = choices[0]
            if choice in tallies:
                tallies[choice] += 1

    return tallies, len(ballots)


def option_label(vote: dict[str, Any], option_id: str) -> str:
    option = _vote_option_map(vote).get(option_id)
    if not option:
        return str(option_id)
    return str(option.get("label") or option_id)


def ballot_to_text(vote: dict[str, Any], ballot: Any) -> str:
    choices = _normalize_ballot_choice(ballot)
    if not choices:
        return "No ballot"
    labels = [option_label(vote, choice) for choice in choices]
    return ", ".join(labels)


def _sorted_option_rows(vote: dict[str, Any], tallies: dict[str, int]) -> list[dict[str, Any]]:
    options = vote.get("options", [])
    if not isinstance(options, list):
        return []
    return sorted(
        [opt for opt in options if isinstance(opt, dict) and str(opt.get("id") or "").strip()],
        key=lambda opt: (-tallies.get(str(opt.get("id")), 0), str(opt.get("label") or "").lower()),
    )


def _tally_lines(vote: dict[str, Any], tallies: dict[str, int], turnout: int) -> str:
    rows = _sorted_option_rows(vote, tallies)
    if not rows:
        return "No options"

    lines: list[str] = []
    for row in rows:
        option_id = str(row.get("id"))
        label = str(row.get("label") or option_id)
        count = tallies.get(option_id, 0)
        pct = (count / turnout * 100) if turnout > 0 else 0.0
        lines.append(f"• {label}: **{count}** ({pct:.1f}%)")
    return "\n".join(lines)


def _compute_vote_outcome(vote: dict[str, Any], tallies: dict[str, int], turnout: int) -> dict[str, Any]:
    vote_type = str(vote.get("vote_type") or "proposal")
    ballot_mode = str(vote.get("ballot_mode") or "single")
    threshold = _clamp(_as_int(vote.get("pass_threshold_percent"), 50) or 50, 1, 100)
    quorum = max(0, _as_int(vote.get("quorum"), 0) or 0)
    quorum_met = turnout >= quorum if quorum > 0 else True

    if turnout <= 0:
        return {
            "status": "no_votes",
            "summary": "No ballots were cast.",
            "winners": [],
            "runoff_candidates": [],
            "quorum_met": quorum_met,
        }

    if not quorum_met:
        return {
            "status": "quorum_failed",
            "summary": f"Quorum not met ({turnout}/{quorum}).",
            "winners": [],
            "runoff_candidates": [],
            "quorum_met": False,
        }

    ordered = _sorted_option_rows(vote, tallies)
    if not ordered:
        return {
            "status": "invalid",
            "summary": "Vote data is invalid.",
            "winners": [],
            "runoff_candidates": [],
            "quorum_met": True,
        }

    if vote_type in {"confidence", "yesno"}:
        primary_id = str(vote.get("primary_option_id") or ordered[0]["id"])
        primary_count = tallies.get(primary_id, 0)
        opposition_count = max(
            (count for option_id, count in tallies.items() if option_id != primary_id),
            default=0,
        )
        primary_pct = (primary_count / turnout * 100) if turnout > 0 else 0.0
        passed = primary_pct >= threshold and primary_count > opposition_count

        if vote_type == "confidence":
            summary = "No-confidence motion passed." if passed else "No-confidence motion did not pass."
        else:
            summary = "Motion passed." if passed else "Motion did not pass."

        return {
            "status": "passed" if passed else "failed",
            "summary": summary,
            "winners": [primary_id] if passed else [],
            "runoff_candidates": [],
            "quorum_met": True,
            "primary_percent": primary_pct,
        }

    if vote_type == "election":
        seats = _clamp(_as_int(vote.get("seats"), 1) or 1, 1, max(1, len(ordered)))
        cutoff_index = min(seats - 1, len(ordered) - 1)
        cutoff_count = tallies.get(str(ordered[cutoff_index].get("id")), 0)

        guaranteed = [opt for opt in ordered if tallies.get(str(opt.get("id")), 0) > cutoff_count]
        tied = [opt for opt in ordered if tallies.get(str(opt.get("id")), 0) == cutoff_count]
        remaining_slots = seats - len(guaranteed)

        tie_for_last = remaining_slots > 0 and len(tied) > remaining_slots
        winners: list[str] = [str(opt.get("id")) for opt in guaranteed]
        if remaining_slots > 0 and not tie_for_last:
            winners.extend(str(opt.get("id")) for opt in tied[:remaining_slots])

        if seats == 1 and _as_bool(vote.get("runoff_enabled"), False):
            first = ordered[0]
            first_count = tallies.get(str(first.get("id")), 0)
            first_pct = (first_count / turnout * 100) if turnout > 0 else 0.0
            if len(ordered) >= 2 and first_pct <= 50:
                second = ordered[1]
                return {
                    "status": "runoff",
                    "summary": (
                        f"No majority winner ({first_pct:.1f}%). Runoff required between "
                        f"{first.get('label')} and {second.get('label')}."
                    ),
                    "winners": [],
                    "runoff_candidates": [str(first.get("id")), str(second.get("id"))],
                    "quorum_met": True,
                }

        if tie_for_last:
            tied_labels = ", ".join(str(opt.get("label")) for opt in tied)
            return {
                "status": "tie",
                "summary": f"Tie for final seat between: {tied_labels}.",
                "winners": winners,
                "runoff_candidates": [],
                "quorum_met": True,
            }

        winner_labels = ", ".join(option_label(vote, winner) for winner in winners) if winners else "None"
        return {
            "status": "passed" if winners else "failed",
            "summary": f"Winner{'s' if seats > 1 else ''}: {winner_labels}",
            "winners": winners,
            "runoff_candidates": [],
            "quorum_met": True,
        }

    if ballot_mode == "multiple" or vote_type == "approval":
        approved: list[str] = []
        for option in ordered:
            option_id = str(option.get("id"))
            score = tallies.get(option_id, 0)
            pct = (score / turnout * 100) if turnout > 0 else 0.0
            if pct >= threshold:
                approved.append(option_id)

        if approved:
            labels = ", ".join(option_label(vote, option_id) for option_id in approved)
            return {
                "status": "passed",
                "summary": f"Approved options: {labels}",
                "winners": approved,
                "runoff_candidates": [],
                "quorum_met": True,
            }

        return {
            "status": "failed",
            "summary": "No option reached the approval threshold.",
            "winners": [],
            "runoff_candidates": [],
            "quorum_met": True,
        }

    top = ordered[0]
    top_id = str(top.get("id"))
    top_count = tallies.get(top_id, 0)
    top_pct = (top_count / turnout * 100) if turnout > 0 else 0.0
    tied_top = [opt for opt in ordered if tallies.get(str(opt.get("id")), 0) == top_count]

    if len(tied_top) > 1:
        labels = ", ".join(str(opt.get("label")) for opt in tied_top)
        return {
            "status": "tie",
            "summary": f"Tie between: {labels}",
            "winners": [],
            "runoff_candidates": [],
            "quorum_met": True,
        }

    passed = top_pct >= threshold
    return {
        "status": "passed" if passed else "failed",
        "summary": (
            f"Winning option: {option_label(vote, top_id)}"
            if passed
            else f"Top option {option_label(vote, top_id)} did not meet threshold ({threshold}%)."
        ),
        "winners": [top_id] if passed else [],
        "runoff_candidates": [],
        "quorum_met": True,
    }


def _vote_kind_label(vote: dict[str, Any]) -> str:
    vote_type = str(vote.get("vote_type") or "proposal")
    ballot_mode = str(vote.get("ballot_mode") or "single")
    names = {
        "confidence": "No Confidence",
        "yesno": "Yes / No",
        "proposal": "Proposal",
        "approval": "Approval",
        "election": "Election",
    }
    return f"{names.get(vote_type, 'Vote')} ({'Multi' if ballot_mode == 'multiple' else 'Single'} Choice)"


def _time_remaining_text(finish_time: datetime) -> str:
    remaining = int((finish_time - datetime.now(timezone.utc)).total_seconds())
    if remaining <= 0:
        return "0s"
    hours, rem = divmod(remaining, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"


def _build_vote_embed(
    vote_id: str,
    vote: dict[str, Any],
    finish_time: datetime,
    *,
    closed: bool = False,
    outcome: dict[str, Any] | None = None,
) -> discord.Embed:
    tallies, turnout = tally_vote(vote)
    color = discord.Color.orange() if closed else discord.Color.red()
    title_prefix = "📣 VOTE CLOSED" if closed else "🗳️ ACTIVE VOTE"

    embed = discord.Embed(
        title=f"{title_prefix}: {vote.get('title', 'Vote')}",
        description=str(vote.get("description") or "")[:1000],
        color=color,
    )

    embed.add_field(name="Type", value=_vote_kind_label(vote), inline=True)
    quorum = max(0, _as_int(vote.get("quorum"), 0) or 0)
    quorum_text = f"{turnout}/{quorum}" if quorum > 0 else str(turnout)
    embed.add_field(name="Turnout", value=quorum_text, inline=True)

    rules_line = [
        f"Threshold: {_clamp(_as_int(vote.get('pass_threshold_percent'), 50) or 50, 1, 100)}%",
        f"Max choices: {_as_int(vote.get('max_choices'), 1) or 1}",
    ]
    if _as_int(vote.get("eligible_role_id")):
        rules_line.append("Role-gated")
    if _as_bool(vote.get("anonymous"), False):
        rules_line.append("Anonymous")
    embed.add_field(name="Rules", value=" • ".join(rules_line), inline=False)

    show_live_results = _as_bool(vote.get("show_live_results"), True)
    if show_live_results or closed:
        embed.add_field(name="Results", value=_tally_lines(vote, tallies, turnout), inline=False)
    else:
        embed.add_field(name="Results", value="Hidden until close.", inline=False)

    if closed and outcome:
        embed.add_field(name="Outcome", value=str(outcome.get("summary") or "Finalized."), inline=False)

    starter_id = _as_int(vote.get("starter_id"))
    starter_text = f"<@{starter_id}>" if starter_id else "unknown"

    if closed:
        footer = f"Vote ID: {vote_id} • Started by {starter_text} • Closed"
    else:
        footer = (
            f"Vote ID: {vote_id} • Started by {starter_text} • "
            f"Time remaining: {_time_remaining_text(finish_time)}"
        )
    embed.set_footer(text=footer)
    return embed


def _component_token(vote_id: str) -> str:
    return hashlib.sha1(vote_id.encode("utf-8")).hexdigest()[:10]


class VoteOptionButton(discord.ui.Button):
    def __init__(self, vote_id: str, option: dict[str, Any], index: int):
        styles = [
            discord.ButtonStyle.primary,
            discord.ButtonStyle.success,
            discord.ButtonStyle.secondary,
            discord.ButtonStyle.danger,
        ]
        option_id = str(option.get("id"))
        label = str(option.get("label") or option_id)
        token = _component_token(vote_id)
        custom_id = f"vote:{token}:opt:{option_id}"[:100]
        super().__init__(
            label=label[:80],
            style=styles[index % len(styles)],
            custom_id=custom_id,
            row=min(3, index // 5),
        )
        self.option_id = option_id

    async def callback(self, interaction: discord.Interaction) -> None:
        if not isinstance(self.view, VoteView):
            return
        await self.view.record_vote(interaction, [self.option_id])


class VoteChoiceSelect(discord.ui.Select):
    def __init__(self, vote_id: str, vote: dict[str, Any]):
        options = vote.get("options", [])
        if not isinstance(options, list):
            options = []

        select_options: list[discord.SelectOption] = []
        for option in options[:25]:
            if not isinstance(option, dict):
                continue
            option_id = str(option.get("id") or "").strip()
            label = str(option.get("label") or option_id).strip()
            if not option_id or not label:
                continue
            description = str(option.get("description") or "").strip()[:100]
            select_options.append(
                discord.SelectOption(
                    label=label[:100],
                    value=option_id,
                    description=description if description else None,
                )
            )

        ballot_mode = str(vote.get("ballot_mode") or "single")
        max_choices = _as_int(vote.get("max_choices"), 1) or 1
        max_choices = _clamp(max_choices, 1, max(1, len(select_options)))
        if ballot_mode == "single":
            max_choices = 1

        token = _component_token(vote_id)
        custom_id = f"vote:{token}:select"[:100]
        placeholder = "Choose your option" if ballot_mode == "single" else "Choose one or more options"
        super().__init__(
            placeholder=placeholder,
            min_values=1,
            max_values=max_choices,
            options=select_options[:25],
            custom_id=custom_id,
            row=0,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not isinstance(self.view, VoteView):
            return
        await self.view.record_vote(interaction, list(self.values))


class VoteShowBallotButton(discord.ui.Button):
    def __init__(self, vote_id: str):
        token = _component_token(vote_id)
        super().__init__(
            label="My Ballot",
            style=discord.ButtonStyle.secondary,
            custom_id=f"vote:{token}:ballot"[:100],
            row=4,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not isinstance(self.view, VoteView):
            return
        await self.view.show_ballot(interaction)


class VoteClearBallotButton(discord.ui.Button):
    def __init__(self, vote_id: str):
        token = _component_token(vote_id)
        super().__init__(
            label="Clear Ballot",
            style=discord.ButtonStyle.danger,
            custom_id=f"vote:{token}:clear"[:100],
            row=4,
        )

    async def callback(self, interaction: discord.Interaction) -> None:
        if not isinstance(self.view, VoteView):
            return
        await self.view.clear_ballot(interaction)


class VoteView(discord.ui.View):
    def __init__(self, vote_id: str, finish_time: datetime, bot: commands.Bot):
        super().__init__(timeout=None)
        self.vote_id = vote_id
        self.finish_time = finish_time
        self.bot = bot
        self._stopped = False
        self._build_components()
        self.update_task = asyncio.create_task(self._countdown_updater())

    def _vote(self) -> dict[str, Any] | None:
        return votes.get(self.vote_id)

    def _build_components(self) -> None:
        vote = self._vote()
        if not vote:
            return

        options = vote.get("options", [])
        if not isinstance(options, list):
            options = []

        ballot_mode = str(vote.get("ballot_mode") or "single")
        max_choices = _as_int(vote.get("max_choices"), 1) or 1

        if ballot_mode == "single" and max_choices == 1 and 2 <= len(options) <= 5:
            for idx, option in enumerate(options):
                if not isinstance(option, dict):
                    continue
                self.add_item(VoteOptionButton(self.vote_id, option, idx))
        else:
            self.add_item(VoteChoiceSelect(self.vote_id, vote))

        self.add_item(VoteShowBallotButton(self.vote_id))
        self.add_item(VoteClearBallotButton(self.vote_id))

    async def _current_channel(self, fallback: discord.abc.GuildChannel | None = None) -> Any:
        if fallback is not None:
            return fallback
        vote = self._vote()
        if not vote:
            return None
        channel_id = _as_int(vote.get("channel_id"))
        if channel_id is None:
            return None
        channel = self.bot.get_channel(channel_id)
        if channel is not None:
            return channel
        try:
            return await self.bot.fetch_channel(channel_id)
        except Exception:
            return None

    async def _enforce_voter_rules(self, interaction: discord.Interaction, vote: dict[str, Any]) -> tuple[bool, str | None]:
        member = interaction.user
        now = datetime.now(timezone.utc)

        if not isinstance(member, discord.Member):
            return False, "Only server members can vote."

        account_age = (now - member.created_at).days
        join_age = (now - member.joined_at).days if getattr(member, "joined_at", None) else 0

        min_account_days = max(0, _as_int(vote.get("min_account_days"), 0) or 0)
        min_join_days = max(0, _as_int(vote.get("min_join_days"), 0) or 0)

        if account_age < min_account_days:
            return False, f"⚠️ Your account is too new ({account_age}d) for this vote."
        if join_age < min_join_days:
            return False, f"⚠️ You joined too recently ({join_age}d) for this vote."

        eligible_role_id = _as_int(vote.get("eligible_role_id"))
        if eligible_role_id is not None:
            role = member.guild.get_role(eligible_role_id)
            if role is None or role not in member.roles:
                return False, "⚠️ You do not have the required role to vote in this ballot."

        return True, None

    async def record_vote(self, interaction: discord.Interaction, selected_choices: list[str]) -> None:
        vote = self._vote()
        if not vote:
            await interaction.response.send_message("This vote has already ended.", ephemeral=True)
            return

        finish_time = _get_vote_finish_time(self.vote_id, vote)
        if finish_time is None or datetime.now(timezone.utc) >= finish_time:
            await interaction.response.send_message("This vote is closed.", ephemeral=True)
            return

        ok, error_message = await self._enforce_voter_rules(interaction, vote)
        if not ok:
            await interaction.response.send_message(error_message or "Voting is not allowed.", ephemeral=True)
            return

        option_ids = set(_vote_option_map(vote).keys())
        ballot_mode = str(vote.get("ballot_mode") or "single")
        max_choices = _as_int(vote.get("max_choices"), 1) or 1
        if ballot_mode == "single":
            max_choices = 1

        choices = _dedupe_keep_order([choice for choice in selected_choices if choice in option_ids])
        if not choices:
            await interaction.response.send_message("Please select at least one valid option.", ephemeral=True)
            return

        if ballot_mode == "single":
            choices = [choices[0]]
        else:
            choices = choices[:max_choices]

        ballots = _clean_ballot_map(vote)
        user_id = str(interaction.user.id)
        previous = ballots.get(user_id)
        if previous == choices:
            text = f"ℹ️ Your ballot is unchanged: {ballot_to_text(vote, choices)}"
            if interaction.response.is_done():
                await interaction.followup.send(text, ephemeral=True)
            else:
                await interaction.response.send_message(text, ephemeral=True)
            return

        ballots[user_id] = choices
        vote["ballots"] = ballots
        _sync_legacy_vote_fields(vote)
        votes[self.vote_id] = vote
        save_votes()

        if previous is None:
            text = f"✅ Ballot recorded: {ballot_to_text(vote, choices)}"
        else:
            text = (
                f"🔁 Ballot updated: {ballot_to_text(vote, previous)}"
                f" → {ballot_to_text(vote, choices)}"
            )

        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)

        channel = await self._current_channel(interaction.channel)
        if channel is not None:
            await self.update_message(channel, vote)

    async def clear_ballot(self, interaction: discord.Interaction) -> None:
        vote = self._vote()
        if not vote:
            await interaction.response.send_message("This vote has already ended.", ephemeral=True)
            return

        ballots = _clean_ballot_map(vote)
        user_id = str(interaction.user.id)
        if user_id not in ballots:
            await interaction.response.send_message("You do not currently have a ballot in this vote.", ephemeral=True)
            return

        previous = ballots.pop(user_id)
        vote["ballots"] = ballots
        _sync_legacy_vote_fields(vote)
        votes[self.vote_id] = vote
        save_votes()

        text = f"🧹 Removed your ballot: {ballot_to_text(vote, previous)}"
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)

        channel = await self._current_channel(interaction.channel)
        if channel is not None:
            await self.update_message(channel, vote)

    async def show_ballot(self, interaction: discord.Interaction) -> None:
        vote = self._vote()
        if not vote:
            await interaction.response.send_message("This vote has already ended.", ephemeral=True)
            return

        ballots = _clean_ballot_map(vote)
        current = ballots.get(str(interaction.user.id), [])
        text = ballot_to_text(vote, current)
        if interaction.response.is_done():
            await interaction.followup.send(f"🗳️ Your ballot: {text}", ephemeral=True)
        else:
            await interaction.response.send_message(f"🗳️ Your ballot: {text}", ephemeral=True)

    async def update_message(
        self,
        channel: Any,
        vote: dict[str, Any],
        *,
        closed: bool = False,
        outcome: dict[str, Any] | None = None,
    ) -> None:
        if channel is None:
            return

        message_id = _as_int(vote.get("message_id"))
        if message_id is None:
            return

        try:
            msg = await channel.fetch_message(message_id)
        except Exception:
            return

        finish_time = _get_vote_finish_time(self.vote_id, vote) or self.finish_time
        self.finish_time = finish_time
        embed = _build_vote_embed(self.vote_id, vote, finish_time, closed=closed, outcome=outcome)

        try:
            await msg.edit(embed=embed, view=None if closed else self)
        except Exception:
            pass

    async def _countdown_updater(self) -> None:
        try:
            while True:
                if self._stopped:
                    break

                vote = self._vote()
                if not vote:
                    break

                finish_time = _get_vote_finish_time(self.vote_id, vote) or self.finish_time
                self.finish_time = finish_time

                channel = await self._current_channel()
                if channel is not None:
                    await self.update_message(channel, vote)

                if datetime.now(timezone.utc) >= finish_time:
                    break

                await asyncio.sleep(30)
        except asyncio.CancelledError:
            pass

    async def stop_updater(self) -> None:
        self._stopped = True
        try:
            if self.update_task:
                self.update_task.cancel()
        except Exception:
            pass
        super().stop()


async def _delete_vote_channel_later(bot: commands.Bot, channel_id: int, delay_seconds: int = 3600) -> None:
    await asyncio.sleep(max(0, int(delay_seconds)))
    try:
        channel = bot.get_channel(channel_id) if channel_id is not None else None
        bot_member = None
        if channel and isinstance(channel, discord.TextChannel):
            bot_member = channel.guild.me
            if bot_member is None and bot.user:
                bot_member = channel.guild.get_member(bot.user.id)
        if (
            isinstance(channel, discord.TextChannel)
            and bot_member is not None
            and channel.permissions_for(bot_member).manage_channels
        ):
            await channel.delete(reason="Vote ended")
    except Exception:
        pass


def _cancel_finish_task(vote_id: str) -> None:
    task = ACTIVE_FINISH_TASKS.pop(vote_id, None)
    if task and not task.done():
        task.cancel()


async def _finish_watcher(bot: commands.Bot, vote_id: str) -> None:
    try:
        while True:
            vote = votes.get(vote_id)
            if not vote:
                return

            finish_time = _get_vote_finish_time(vote_id, vote)
            if finish_time is None:
                await finish_vote(bot, vote_id)
                return

            remaining = (finish_time - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                await finish_vote(bot, vote_id)
                return

            await asyncio.sleep(min(60, max(1, remaining)))
    except asyncio.CancelledError:
        return
    finally:
        task = ACTIVE_FINISH_TASKS.get(vote_id)
        if task is asyncio.current_task():
            ACTIVE_FINISH_TASKS.pop(vote_id, None)


def _schedule_finish_watcher(bot: commands.Bot, vote_id: str) -> None:
    current = ACTIVE_FINISH_TASKS.get(vote_id)
    if current and not current.done():
        return
    ACTIVE_FINISH_TASKS[vote_id] = asyncio.create_task(_finish_watcher(bot, vote_id))


async def _send_ctx_message(ctx: commands.Context, text: str, *, ephemeral: bool = False) -> None:
    interaction = getattr(ctx, "interaction", None)
    if not ephemeral or interaction is None:
        await ctx.send(text)
        return

    try:
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)
    except Exception:
        await ctx.send(text)


async def _create_vote(
    *,
    bot: commands.Bot,
    guild: discord.Guild,
    channel: discord.TextChannel,
    starter_id: int,
    title: str,
    description: str,
    vote_type: str,
    options: list[dict[str, Any]],
    ballot_mode: str,
    max_choices: int,
    duration_hours: int,
    min_account_days: int,
    min_join_days: int,
    quorum: int,
    pass_threshold_percent: int,
    anonymous: bool,
    show_live_results: bool,
    eligible_role_id: int | None,
    primary_option_id: str | None = None,
    target_id: int | None = None,
    target_name: str = "",
    seats: int = 1,
    runoff_enabled: bool = False,
    mention_everyone: bool = False,
    delete_channel_after_close: bool = False,
    delete_delay_seconds: int = 3600,
    runoff_from: str | None = None,
) -> tuple[str, dict[str, Any]]:
    duration_hours = _clamp(duration_hours, 1, MAX_DURATION_HOURS)
    finish_time = datetime.now(timezone.utc) + timedelta(hours=duration_hours)

    nonce = int(datetime.now(timezone.utc).microsecond)
    vote_id = f"{guild.id}-{channel.id}-{nonce}-{int(datetime.now(timezone.utc).timestamp())}"

    payload = {
        "id": vote_id,
        "schema_version": 2,
        "vote_type": vote_type,
        "title": title,
        "description": description,
        "options": options,
        "ballot_mode": ballot_mode,
        "max_choices": max_choices,
        "ballots": {},
        "votes": {},
        "starter_id": starter_id,
        "target_id": target_id,
        "target_name": target_name,
        "channel_id": channel.id,
        "message_id": 0,
        "duration_hours": duration_hours,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "finish_at": finish_time.isoformat(),
        "min_account_days": max(0, min_account_days),
        "min_join_days": max(0, min_join_days),
        "quorum": max(0, quorum),
        "pass_threshold_percent": _clamp(pass_threshold_percent, 1, 100),
        "primary_option_id": primary_option_id or str(options[0]["id"]),
        "anonymous": bool(anonymous),
        "show_live_results": bool(show_live_results),
        "eligible_role_id": eligible_role_id,
        "seats": _clamp(seats, 1, max(1, len(options))),
        "runoff_enabled": bool(runoff_enabled),
        "runoff_from": runoff_from,
        "delete_channel_after_close": bool(delete_channel_after_close),
        "delete_delay_seconds": max(0, int(delete_delay_seconds)),
    }

    payload = _sync_legacy_vote_fields(payload)
    votes[vote_id] = payload
    save_votes()

    view = VoteView(vote_id, finish_time, bot)
    ACTIVE_VIEWS[vote_id] = view

    embed = _build_vote_embed(vote_id, payload, finish_time)
    content = "@everyone" if mention_everyone else None

    try:
        message = await channel.send(content=content, embed=embed, view=view)
    except Exception:
        if mention_everyone:
            try:
                message = await channel.send(embed=embed, view=view)
            except Exception:
                await view.stop_updater()
                ACTIVE_VIEWS.pop(vote_id, None)
                votes.pop(vote_id, None)
                save_votes()
                raise
        else:
            await view.stop_updater()
            ACTIVE_VIEWS.pop(vote_id, None)
            votes.pop(vote_id, None)
            save_votes()
            raise

    payload["message_id"] = message.id
    votes[vote_id] = _sync_legacy_vote_fields(payload)
    save_votes()

    try:
        bot.add_view(view, message_id=message.id)
    except Exception:
        pass

    _schedule_finish_watcher(bot, vote_id)
    return vote_id, payload


async def _create_election_runoff(
    *,
    bot: commands.Bot,
    original_vote_id: str,
    vote: dict[str, Any],
    runoff_candidates: list[str],
) -> str | None:
    channel_id = _as_int(vote.get("channel_id"))
    if channel_id is None:
        return None

    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            fetched = await bot.fetch_channel(channel_id)
            channel = fetched if isinstance(fetched, discord.TextChannel) else None
        except Exception:
            channel = None

    if not isinstance(channel, discord.TextChannel):
        return None

    option_map = _vote_option_map(vote)
    options = [option_map.get(option_id) for option_id in runoff_candidates]
    options = [option for option in options if isinstance(option, dict)]
    if len(options) < 2:
        return None

    duration_hours = _as_int(vote.get("duration_hours"), 24) or 24
    runoff_duration = _clamp(max(6, duration_hours // 2), 6, 48)

    try:
        runoff_id, _ = await _create_vote(
            bot=bot,
            guild=channel.guild,
            channel=channel,
            starter_id=_as_int(vote.get("starter_id"), 0) or 0,
            title=f"Runoff Election: {str(vote.get('title') or 'Election')[:80]}",
            description=f"Automatic runoff from `{original_vote_id}`.",
            vote_type="election",
            options=options,
            ballot_mode="single",
            max_choices=1,
            duration_hours=runoff_duration,
            min_account_days=max(0, _as_int(vote.get("min_account_days"), 0) or 0),
            min_join_days=max(0, _as_int(vote.get("min_join_days"), 0) or 0),
            quorum=max(0, _as_int(vote.get("quorum"), 0) or 0),
            pass_threshold_percent=50,
            anonymous=_as_bool(vote.get("anonymous"), False),
            show_live_results=_as_bool(vote.get("show_live_results"), True),
            eligible_role_id=_as_int(vote.get("eligible_role_id")),
            seats=1,
            runoff_enabled=False,
            runoff_from=original_vote_id,
        )
    except Exception:
        return None

    try:
        await channel.send(f"🔁 Runoff election started automatically: `{runoff_id}`")
    except Exception:
        pass

    return runoff_id


async def finish_vote(bot: commands.Bot, vote_id: str) -> None:
    vote = votes.get(vote_id)
    if not vote:
        _cancel_finish_task(vote_id)
        return

    view = ACTIVE_VIEWS.pop(vote_id, None)
    if view is not None:
        await view.stop_updater()

    _cancel_finish_task(vote_id)

    tallies, turnout = tally_vote(vote)
    outcome = _compute_vote_outcome(vote, tallies, turnout)

    channel_id = _as_int(vote.get("channel_id"))
    message_id = _as_int(vote.get("message_id"))
    channel = bot.get_channel(channel_id) if channel_id is not None else None

    if channel is None and channel_id is not None:
        try:
            fetched = await bot.fetch_channel(channel_id)
            channel = fetched if isinstance(fetched, discord.TextChannel) else None
        except Exception:
            channel = None

    try:
        if isinstance(channel, discord.TextChannel) and message_id is not None:
            finish_time = _get_vote_finish_time(vote_id, vote) or datetime.now(timezone.utc)
            embed = _build_vote_embed(vote_id, vote, finish_time, closed=True, outcome=outcome)
            try:
                msg = await channel.fetch_message(message_id)
                await msg.edit(embed=embed, view=None)
            except Exception:
                pass

            starter_id = _as_int(vote.get("starter_id"))
            target_id = _as_int(vote.get("target_id"))
            target_text = f" • Target: <@{target_id}>" if target_id else ""
            starter_text = f"<@{starter_id}>" if starter_id else "unknown"
            await channel.send(
                f"🔔 **VOTE ENDED** `{vote_id}` — {outcome.get('summary', 'Finalized.')}"
                f"\nStarter: {starter_text}{target_text}"
            )

            if outcome.get("status") == "runoff" and str(vote.get("vote_type")) == "election":
                await _create_election_runoff(
                    bot=bot,
                    original_vote_id=vote_id,
                    vote=vote,
                    runoff_candidates=list(outcome.get("runoff_candidates", [])),
                )

            if _as_bool(vote.get("delete_channel_after_close"), False) and channel_id is not None:
                delay = max(0, _as_int(vote.get("delete_delay_seconds"), 3600) or 3600)
                asyncio.create_task(_delete_vote_channel_later(bot, channel_id, delay_seconds=delay))
    except Exception as exc:
        print("Error finishing vote:", exc)
    finally:
        votes.pop(vote_id, None)
        save_votes()


async def restore_vote_state(bot: commands.Bot) -> None:
    now = datetime.now(timezone.utc)
    changed = False

    for view in list(ACTIVE_VIEWS.values()):
        try:
            await view.stop_updater()
        except Exception:
            pass
    ACTIVE_VIEWS.clear()

    for vote_id in list(ACTIVE_FINISH_TASKS):
        _cancel_finish_task(vote_id)

    for vote_id, vote in list(votes.items()):
        clean = _normalize_vote(vote_id, vote)
        if clean is None:
            votes.pop(vote_id, None)
            changed = True
            continue

        if clean != vote:
            votes[vote_id] = clean
            vote = clean
            changed = True

        finish_time = _get_vote_finish_time(vote_id, vote)
        if finish_time is None:
            votes.pop(vote_id, None)
            changed = True
            continue

        if finish_time <= now:
            _schedule_finish_watcher(bot, vote_id)
            continue

        message_id = _as_int(vote.get("message_id"))
        channel_id = _as_int(vote.get("channel_id"))
        if message_id is None or channel_id is None:
            votes.pop(vote_id, None)
            changed = True
            continue

        view = VoteView(vote_id, finish_time, bot)
        ACTIVE_VIEWS[vote_id] = view
        try:
            bot.add_view(view, message_id=message_id)
        except Exception:
            pass

        _schedule_finish_watcher(bot, vote_id)

    if changed:
        save_votes()


def _parse_pipe_values(raw: str, *, limit: int = MAX_OPTIONS) -> list[str]:
    values = [token.strip() for token in str(raw or "").split("|")]
    values = [token for token in values if token]
    return values[:limit]


def _build_option_list(values: list[str]) -> list[dict[str, Any]]:
    options: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for idx, label in enumerate(values):
        option_id = _normalize_option_id(label, idx)
        if option_id in seen_ids:
            option_id = _normalize_option_id(f"{option_id}-{idx + 1}", idx)
        seen_ids.add(option_id)
        options.append(
            {
                "id": option_id,
                "label": label[:80],
                "description": "",
                "member_id": None,
            }
        )

    return options


def _parse_candidate_token(guild: discord.Guild, token: str, index: int) -> dict[str, Any] | None:
    token = token.strip()
    if not token:
        return None

    mention_match = re.fullmatch(r"<@!?(\d+)>", token)
    member_id = _as_int(mention_match.group(1) if mention_match else token)
    member = guild.get_member(member_id) if member_id is not None else None

    if member is not None:
        return {
            "id": f"candidate-{member.id}",
            "label": member.display_name[:80],
            "description": f"{member.name}#{member.discriminator}",
            "member_id": member.id,
        }

    option_id = _normalize_option_id(token, index)
    return {
        "id": option_id,
        "label": token[:80],
        "description": "",
        "member_id": None,
    }


def _vote_belongs_to_guild(vote_id: str, guild: discord.Guild) -> bool:
    token = str(vote_id or "")
    return token.startswith(f"{guild.id}-")


def setup_vote_module(bot: commands.Bot) -> None:
    @bot.hybrid_command(name="votecreate")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def votecreate(
        ctx: commands.Context,
        title: str,
        options: str = "",
        duration_hours: int = 24,
        vote_type: str = "proposal",
        max_choices: int = 1,
        quorum: int = 0,
        pass_threshold_percent: int = 50,
        anonymous: bool = False,
        hide_live_results: bool = False,
        min_account_days: int = 0,
        min_join_days: int = 0,
        eligible_role: discord.Role | None = None,
        channel: discord.TextChannel | None = None,
        target: discord.Member | None = None,
        mention_everyone: bool = False,
    ) -> None:
        """Create a configurable vote. For confidence votes, set vote_type=confidence and provide target."""
        guild = ctx.guild
        if not guild:
            await _send_ctx_message(ctx, "This command must be used in a server.")
            return

        duration_hours = _as_int(duration_hours, 24) or 24
        if duration_hours < 1 or duration_hours > MAX_DURATION_HOURS:
            await _send_ctx_message(ctx, f"⚠️ Duration must be between 1 and {MAX_DURATION_HOURS} hours.")
            return

        vote_type = str(vote_type or "proposal").strip().lower()
        if vote_type not in {"proposal", "yesno", "approval", "confidence"}:
            await _send_ctx_message(ctx, "⚠️ vote_type must be one of: proposal, yesno, approval, confidence.")
            return

        target_id: int | None = None
        target_name = ""
        description = ""
        option_list: list[dict[str, Any]]
        if vote_type == "confidence":
            if target is None:
                await _send_ctx_message(ctx, "⚠️ confidence votes require a `target` member.")
                return
            option_list = [
                {"id": "against", "label": "Against", "description": "", "member_id": None},
                {"id": "support", "label": "Support", "description": "", "member_id": None},
            ]
            ballot_mode = "single"
            max_choices = 1
            target_id = target.id
            target_name = str(target)
            if not title.strip():
                title = f"No-Confidence Vote: {target.display_name}"
            description = (
                f"A no-confidence vote has been initiated against **{target.display_name}**. "
                "Vote carefully and responsibly."
            )
            if min_account_days <= 0:
                min_account_days = 7
            if min_join_days <= 0:
                min_join_days = 1
        else:
            option_values = _parse_pipe_values(options, limit=MAX_OPTIONS)
            if vote_type == "yesno":
                if len(option_values) < 2:
                    option_values = ["Yes", "No"]
                option_values = option_values[:2]
            elif len(option_values) < 2:
                await _send_ctx_message(ctx, "⚠️ Provide at least 2 options separated by `|`.")
                return

            option_list = _build_option_list(option_values)
            ballot_mode = "multiple" if vote_type == "approval" or (max_choices and max_choices > 1) else "single"
            max_choices = _as_int(max_choices, 1) or 1
            max_choices = _clamp(max_choices, 1, max(1, len(option_list)))
            if ballot_mode == "single":
                max_choices = 1

        target_channel = channel or ctx.channel
        if not isinstance(target_channel, discord.TextChannel):
            await _send_ctx_message(ctx, "⚠️ Target channel must be a text channel.")
            return

        primary_option_id = None
        if vote_type == "yesno":
            primary_option_id = option_list[0]["id"]
        if vote_type == "confidence":
            primary_option_id = "against"

        try:
            vote_id, _ = await _create_vote(
                bot=bot,
                guild=guild,
                channel=target_channel,
                starter_id=ctx.author.id,
                title=title[:120],
                description=description,
                vote_type=vote_type,
                options=option_list,
                ballot_mode=ballot_mode,
                max_choices=max_choices,
                duration_hours=duration_hours,
                min_account_days=max(0, _as_int(min_account_days, 0) or 0),
                min_join_days=max(0, _as_int(min_join_days, 0) or 0),
                quorum=max(0, _as_int(quorum, 0) or 0),
                pass_threshold_percent=_clamp(_as_int(pass_threshold_percent, 50) or 50, 1, 100),
                anonymous=bool(anonymous),
                show_live_results=not bool(hide_live_results),
                eligible_role_id=eligible_role.id if eligible_role else None,
                primary_option_id=primary_option_id,
                target_id=target_id,
                target_name=target_name,
                seats=1,
                runoff_enabled=False,
                mention_everyone=bool(mention_everyone and vote_type == "confidence"),
            )
        except Exception:
            await _send_ctx_message(ctx, "⚠️ Failed to create vote message.")
            return

        if vote_type == "confidence":
            await _send_ctx_message(
                ctx,
                f"🚨 No-confidence vote created in {target_channel.mention}. Vote ID: `{vote_id}`",
            )
            return

        await _send_ctx_message(
            ctx,
            f"✅ Vote created in {target_channel.mention}. Vote ID: `{vote_id}`",
        )

    @bot.hybrid_command(name="startelection")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    @commands.bot_has_permissions(send_messages=True, embed_links=True)
    async def startelection(
        ctx: commands.Context,
        title: str,
        candidates: str,
        duration_hours: int = 24,
        seats: int = 1,
        runoff: bool = True,
        quorum: int = 0,
        min_account_days: int = 0,
        min_join_days: int = 0,
        anonymous: bool = False,
        hide_live_results: bool = False,
        eligible_role: discord.Role | None = None,
        channel: discord.TextChannel | None = None,
    ) -> None:
        """Start an election. Candidates format: @user1 | @user2 | Candidate 3."""
        guild = ctx.guild
        if not guild:
            await _send_ctx_message(ctx, "This command must be used in a server.")
            return

        duration_hours = _as_int(duration_hours, 24) or 24
        if duration_hours < 1 or duration_hours > MAX_DURATION_HOURS:
            await _send_ctx_message(ctx, f"⚠️ Duration must be between 1 and {MAX_DURATION_HOURS} hours.")
            return

        candidate_tokens = _parse_pipe_values(candidates, limit=MAX_OPTIONS)
        candidate_options: list[dict[str, Any]] = []
        for index, token in enumerate(candidate_tokens):
            option = _parse_candidate_token(guild, token, index)
            if option is not None:
                candidate_options.append(option)

        # Keep candidates unique by option id.
        deduped: dict[str, dict[str, Any]] = {}
        for option in candidate_options:
            deduped[option["id"]] = option
        candidate_options = list(deduped.values())

        if len(candidate_options) < 2:
            await _send_ctx_message(ctx, "⚠️ Provide at least 2 unique candidates.")
            return

        seats = _as_int(seats, 1) or 1
        seats = _clamp(seats, 1, len(candidate_options))

        target_channel = channel or ctx.channel
        if not isinstance(target_channel, discord.TextChannel):
            await _send_ctx_message(ctx, "⚠️ Target channel must be a text channel.")
            return

        description = (
            "Election is open. Pick one candidate. "
            "If enabled, runoff is created automatically when no majority winner exists."
        )

        try:
            vote_id, _ = await _create_vote(
                bot=bot,
                guild=guild,
                channel=target_channel,
                starter_id=ctx.author.id,
                title=title[:120],
                description=description,
                vote_type="election",
                options=candidate_options,
                ballot_mode="single",
                max_choices=1,
                duration_hours=duration_hours,
                min_account_days=max(0, _as_int(min_account_days, 0) or 0),
                min_join_days=max(0, _as_int(min_join_days, 0) or 0),
                quorum=max(0, _as_int(quorum, 0) or 0),
                pass_threshold_percent=50,
                anonymous=bool(anonymous),
                show_live_results=not bool(hide_live_results),
                eligible_role_id=eligible_role.id if eligible_role else None,
                seats=seats,
                runoff_enabled=bool(runoff and seats == 1),
            )
        except Exception:
            await _send_ctx_message(ctx, "⚠️ Failed to create election message.")
            return

        await _send_ctx_message(
            ctx,
            f"🏛️ Election started in {target_channel.mention}. Vote ID: `{vote_id}`",
        )

    @bot.hybrid_command(name="voteclose")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def voteclose(ctx: commands.Context, vote_id: str) -> None:
        """Close an active vote immediately by vote ID."""
        guild = ctx.guild
        if not guild:
            await _send_ctx_message(ctx, "This command must be used in a server.")
            return

        vote_id = str(vote_id or "").strip()
        if not _vote_belongs_to_guild(vote_id, guild):
            await _send_ctx_message(ctx, "❌ This vote ID does not belong to this server.")
            return

        if vote_id not in votes:
            await _send_ctx_message(ctx, "❌ Vote not found.")
            return

        await finish_vote(bot, vote_id)
        await _send_ctx_message(ctx, f"✅ Vote `{vote_id}` has been closed.")

    @bot.hybrid_command(name="voteextend")
    @commands.guild_only()
    @commands.has_permissions(manage_guild=True)
    async def voteextend(ctx: commands.Context, vote_id: str, extra_hours: int) -> None:
        """Extend an active vote by N hours."""
        guild = ctx.guild
        if not guild:
            await _send_ctx_message(ctx, "This command must be used in a server.")
            return

        vote_id = str(vote_id or "").strip()
        if not _vote_belongs_to_guild(vote_id, guild):
            await _send_ctx_message(ctx, "❌ This vote ID does not belong to this server.")
            return

        vote = votes.get(vote_id)
        if vote is None:
            await _send_ctx_message(ctx, "❌ Vote not found.")
            return

        extra_hours = _as_int(extra_hours)
        if extra_hours is None or extra_hours < 1 or extra_hours > MAX_DURATION_HOURS:
            await _send_ctx_message(ctx, f"⚠️ extra_hours must be between 1 and {MAX_DURATION_HOURS}.")
            return

        finish_time = _get_vote_finish_time(vote_id, vote)
        if finish_time is None:
            await _send_ctx_message(ctx, "⚠️ Vote has invalid timing metadata.")
            return

        if datetime.now(timezone.utc) >= finish_time:
            await _send_ctx_message(ctx, "⚠️ Vote already ended.")
            return

        new_finish = finish_time + timedelta(hours=extra_hours)
        vote["finish_at"] = new_finish.isoformat()
        vote["duration_hours"] = _clamp(
            _as_int(vote.get("duration_hours"), 24) or 24,
            1,
            MAX_DURATION_HOURS,
        )
        votes[vote_id] = vote
        save_votes()

        view = ACTIVE_VIEWS.get(vote_id)
        if view is not None:
            view.finish_time = new_finish

        _schedule_finish_watcher(bot, vote_id)
        await _send_ctx_message(
            ctx,
            f"⏳ Extended `{vote_id}` by {extra_hours}h. New end: <t:{int(new_finish.timestamp())}:F>",
        )

    @bot.hybrid_command(name="voteconfig")
    @commands.guild_only()
    async def voteconfig(ctx: commands.Context, vote_id: str) -> None:
        """Show settings and rules for a specific vote."""
        guild = ctx.guild
        if not guild:
            await _send_ctx_message(ctx, "This command must be used in a server.")
            return

        vote_id = str(vote_id or "").strip()
        if not _vote_belongs_to_guild(vote_id, guild):
            await _send_ctx_message(ctx, "❌ This vote ID does not belong to this server.")
            return

        vote = votes.get(vote_id)
        if vote is None:
            await _send_ctx_message(ctx, "❌ Vote not found.")
            return

        finish_time = _get_vote_finish_time(vote_id, vote)
        finish_text = f"<t:{int(finish_time.timestamp())}:F>" if finish_time else "unknown"
        tallies, turnout = tally_vote(vote)

        lines = [
            f"Title: {vote.get('title', 'Vote')}",
            f"Type: {_vote_kind_label(vote)}",
            f"Turnout: {turnout}",
            f"Quorum: {max(0, _as_int(vote.get('quorum'), 0) or 0)}",
            f"Threshold: {_clamp(_as_int(vote.get('pass_threshold_percent'), 50) or 50, 1, 100)}%",
            f"Min account age: {max(0, _as_int(vote.get('min_account_days'), 0) or 0)}d",
            f"Min join age: {max(0, _as_int(vote.get('min_join_days'), 0) or 0)}d",
            f"Anonymous: {'yes' if _as_bool(vote.get('anonymous'), False) else 'no'}",
            f"Live results: {'shown' if _as_bool(vote.get('show_live_results'), True) else 'hidden'}",
            f"Ends: {finish_text}",
            "",
            "Current tallies:",
            _tally_lines(vote, tallies, turnout),
        ]

        await _send_ctx_message(ctx, "\n".join(lines))
