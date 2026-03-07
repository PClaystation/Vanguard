# Copyright (c) 2026 Continental. All rights reserved.
# Licensed under the Vanguard Proprietary Source-Available License (see /LICENSE).

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
import hashlib
import re
from typing import Any, Awaitable, Callable

import discord
from discord.ext import commands

MAX_TIMEOUT_SECONDS = 60 * 60 * 24 * 28
ACTIVE_CHANNEL_WINDOW_SECONDS = 900
DUPLICATE_TRACKER_CLEAN_INTERVAL_SECONDS = 60
CHANNEL_ACTIVITY_CLEAN_INTERVAL_SECONDS = 30

URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)
INVITE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite)/[A-Za-z0-9-]+",
    re.IGNORECASE,
)
MENTION_TOKEN_RE = re.compile(r"<[@#][!&]?\d{17,20}>")
WHITESPACE_RE = re.compile(r"\s+")

GUARD_CONFIG_DEFAULTS: dict[str, Any] = {
    "guard_enabled": False,
    "guard_window_seconds": 30,
    "guard_threshold": 8,
    "guard_new_account_hours": 24,
    "guard_slowmode_seconds": 30,
    "guard_cooldown_seconds": 300,
    "guard_slowmode_scope": "trigger",
    "guard_max_slowmode_channels": 3,
    "guard_critical_slowmode_seconds": 120,
    "guard_timeout_seconds": 0,
    "guard_delete_trigger_message": False,
    "guard_join_threshold": 6,
    "guard_join_window_seconds": 45,
    "guard_mention_per_message": 6,
    "guard_mention_burst_threshold": 3,
    "guard_mention_window_seconds": 20,
    "guard_duplicate_threshold": 4,
    "guard_duplicate_window_seconds": 25,
    "guard_duplicate_min_chars": 12,
    "guard_link_threshold": 5,
    "guard_link_window_seconds": 45,
    "guard_detect_joins": True,
    "guard_detect_mentions": True,
    "guard_detect_duplicates": True,
    "guard_detect_links": True,
}

VALID_GUARD_SCOPES = {"trigger", "active"}

GUARD_PRESETS: dict[str, dict[str, Any]] = {
    "off": {
        "guard_enabled": False,
    },
    "relaxed": {
        "guard_enabled": True,
        "guard_threshold": 12,
        "guard_window_seconds": 30,
        "guard_new_account_hours": 12,
        "guard_slowmode_seconds": 20,
        "guard_cooldown_seconds": 240,
        "guard_slowmode_scope": "trigger",
        "guard_timeout_seconds": 0,
        "guard_join_threshold": 9,
        "guard_join_window_seconds": 45,
        "guard_mention_per_message": 10,
        "guard_mention_burst_threshold": 4,
        "guard_mention_window_seconds": 20,
        "guard_duplicate_threshold": 5,
        "guard_duplicate_window_seconds": 25,
        "guard_link_threshold": 7,
        "guard_link_window_seconds": 45,
    },
    "balanced": {
        "guard_enabled": True,
        "guard_threshold": 8,
        "guard_window_seconds": 30,
        "guard_new_account_hours": 24,
        "guard_slowmode_seconds": 30,
        "guard_cooldown_seconds": 300,
        "guard_slowmode_scope": "active",
        "guard_max_slowmode_channels": 3,
        "guard_critical_slowmode_seconds": 120,
        "guard_timeout_seconds": 300,
        "guard_delete_trigger_message": False,
        "guard_join_threshold": 6,
        "guard_join_window_seconds": 45,
        "guard_mention_per_message": 6,
        "guard_mention_burst_threshold": 3,
        "guard_mention_window_seconds": 20,
        "guard_duplicate_threshold": 4,
        "guard_duplicate_window_seconds": 25,
        "guard_duplicate_min_chars": 12,
        "guard_link_threshold": 5,
        "guard_link_window_seconds": 45,
        "guard_detect_joins": True,
        "guard_detect_mentions": True,
        "guard_detect_duplicates": True,
        "guard_detect_links": True,
    },
    "strict": {
        "guard_enabled": True,
        "guard_threshold": 6,
        "guard_window_seconds": 20,
        "guard_new_account_hours": 48,
        "guard_slowmode_seconds": 45,
        "guard_cooldown_seconds": 180,
        "guard_slowmode_scope": "active",
        "guard_max_slowmode_channels": 6,
        "guard_critical_slowmode_seconds": 180,
        "guard_timeout_seconds": 900,
        "guard_delete_trigger_message": True,
        "guard_join_threshold": 5,
        "guard_join_window_seconds": 35,
        "guard_mention_per_message": 4,
        "guard_mention_burst_threshold": 2,
        "guard_mention_window_seconds": 20,
        "guard_duplicate_threshold": 3,
        "guard_duplicate_window_seconds": 20,
        "guard_duplicate_min_chars": 10,
        "guard_link_threshold": 4,
        "guard_link_window_seconds": 35,
        "guard_detect_joins": True,
        "guard_detect_mentions": True,
        "guard_detect_duplicates": True,
        "guard_detect_links": True,
    },
    "siege": {
        "guard_enabled": True,
        "guard_threshold": 4,
        "guard_window_seconds": 15,
        "guard_new_account_hours": 72,
        "guard_slowmode_seconds": 60,
        "guard_cooldown_seconds": 120,
        "guard_slowmode_scope": "active",
        "guard_max_slowmode_channels": 12,
        "guard_critical_slowmode_seconds": 300,
        "guard_timeout_seconds": 1800,
        "guard_delete_trigger_message": True,
        "guard_join_threshold": 4,
        "guard_join_window_seconds": 25,
        "guard_mention_per_message": 3,
        "guard_mention_burst_threshold": 2,
        "guard_mention_window_seconds": 15,
        "guard_duplicate_threshold": 3,
        "guard_duplicate_window_seconds": 15,
        "guard_duplicate_min_chars": 8,
        "guard_link_threshold": 3,
        "guard_link_window_seconds": 25,
        "guard_detect_joins": True,
        "guard_detect_mentions": True,
        "guard_detect_duplicates": True,
        "guard_detect_links": True,
    },
}

GUARD_PRESET_ALIASES = {
    "low": "relaxed",
    "medium": "balanced",
    "high": "strict",
    "paranoid": "siege",
    "panic": "siege",
}

new_account_message_tracker: dict[int, deque[datetime]] = defaultdict(deque)
join_rate_tracker: dict[int, deque[datetime]] = defaultdict(deque)
mention_rate_tracker: dict[int, deque[datetime]] = defaultdict(deque)
link_rate_tracker: dict[int, deque[datetime]] = defaultdict(deque)
duplicate_message_tracker: dict[int, dict[str, deque[tuple[datetime, int]]]] = defaultdict(dict)
channel_activity_tracker: dict[int, dict[int, datetime]] = defaultdict(dict)
channel_activity_cleanup_at: dict[int, datetime] = {}
duplicate_cleanup_at: dict[int, datetime] = {}
guard_last_trigger: dict[int, datetime] = {}


def _new_runtime_stats() -> dict[str, Any]:
    return {
        "triggers_total": 0,
        "suppressed_total": 0,
        "last_trigger_at": None,
        "last_trigger_reasons": [],
        "last_trigger_severity": None,
        "last_trigger_actor_id": None,
    }


guard_runtime_stats: dict[int, dict[str, Any]] = defaultdict(_new_runtime_stats)

RequireModContextCallback = Callable[
    [commands.Context],
    Awaitable[tuple[discord.Guild, dict[str, Any]] | None],
]
SaveSettingsCallback = Callable[[], None]
SendOpsLogCallback = Callable[[discord.Guild, str], Awaitable[None]]
LogModerationActionCallback = Callable[..., int]


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in {"1", "true", "yes", "on"}:
            return True
        if token in {"0", "false", "no", "off"}:
            return False
    return default


def _bounded_int(
    raw: Any,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    value = _as_int(raw)
    if value is None or not minimum <= value <= maximum:
        return default
    return value


def _normalize_scope(raw: Any, default: str = "trigger") -> str:
    if isinstance(raw, str):
        candidate = raw.strip().lower()
        if candidate in VALID_GUARD_SCOPES:
            return candidate
    return default


def guard_default_settings() -> dict[str, Any]:
    return dict(GUARD_CONFIG_DEFAULTS)


def normalize_guard_settings(raw_cfg: Any) -> dict[str, Any]:
    cfg = guard_default_settings()
    if not isinstance(raw_cfg, dict):
        return cfg

    cfg["guard_enabled"] = _as_bool(raw_cfg.get("guard_enabled"), cfg["guard_enabled"])
    cfg["guard_window_seconds"] = _bounded_int(
        raw_cfg.get("guard_window_seconds"),
        default=cfg["guard_window_seconds"],
        minimum=5,
        maximum=300,
    )
    cfg["guard_threshold"] = _bounded_int(
        raw_cfg.get("guard_threshold"),
        default=cfg["guard_threshold"],
        minimum=3,
        maximum=100,
    )
    cfg["guard_new_account_hours"] = _bounded_int(
        raw_cfg.get("guard_new_account_hours"),
        default=cfg["guard_new_account_hours"],
        minimum=1,
        maximum=168,
    )
    cfg["guard_slowmode_seconds"] = _bounded_int(
        raw_cfg.get("guard_slowmode_seconds"),
        default=cfg["guard_slowmode_seconds"],
        minimum=0,
        maximum=21600,
    )
    cfg["guard_cooldown_seconds"] = _bounded_int(
        raw_cfg.get("guard_cooldown_seconds"),
        default=cfg["guard_cooldown_seconds"],
        minimum=30,
        maximum=3600,
    )
    cfg["guard_slowmode_scope"] = _normalize_scope(
        raw_cfg.get("guard_slowmode_scope"),
        default=cfg["guard_slowmode_scope"],
    )
    cfg["guard_max_slowmode_channels"] = _bounded_int(
        raw_cfg.get("guard_max_slowmode_channels"),
        default=cfg["guard_max_slowmode_channels"],
        minimum=1,
        maximum=25,
    )
    cfg["guard_critical_slowmode_seconds"] = _bounded_int(
        raw_cfg.get("guard_critical_slowmode_seconds"),
        default=cfg["guard_critical_slowmode_seconds"],
        minimum=0,
        maximum=21600,
    )
    cfg["guard_timeout_seconds"] = _bounded_int(
        raw_cfg.get("guard_timeout_seconds"),
        default=cfg["guard_timeout_seconds"],
        minimum=0,
        maximum=MAX_TIMEOUT_SECONDS,
    )
    cfg["guard_delete_trigger_message"] = _as_bool(
        raw_cfg.get("guard_delete_trigger_message"),
        cfg["guard_delete_trigger_message"],
    )
    cfg["guard_join_threshold"] = _bounded_int(
        raw_cfg.get("guard_join_threshold"),
        default=cfg["guard_join_threshold"],
        minimum=2,
        maximum=100,
    )
    cfg["guard_join_window_seconds"] = _bounded_int(
        raw_cfg.get("guard_join_window_seconds"),
        default=cfg["guard_join_window_seconds"],
        minimum=5,
        maximum=600,
    )
    cfg["guard_mention_per_message"] = _bounded_int(
        raw_cfg.get("guard_mention_per_message"),
        default=cfg["guard_mention_per_message"],
        minimum=1,
        maximum=50,
    )
    cfg["guard_mention_burst_threshold"] = _bounded_int(
        raw_cfg.get("guard_mention_burst_threshold"),
        default=cfg["guard_mention_burst_threshold"],
        minimum=1,
        maximum=50,
    )
    cfg["guard_mention_window_seconds"] = _bounded_int(
        raw_cfg.get("guard_mention_window_seconds"),
        default=cfg["guard_mention_window_seconds"],
        minimum=5,
        maximum=600,
    )
    cfg["guard_duplicate_threshold"] = _bounded_int(
        raw_cfg.get("guard_duplicate_threshold"),
        default=cfg["guard_duplicate_threshold"],
        minimum=2,
        maximum=30,
    )
    cfg["guard_duplicate_window_seconds"] = _bounded_int(
        raw_cfg.get("guard_duplicate_window_seconds"),
        default=cfg["guard_duplicate_window_seconds"],
        minimum=5,
        maximum=300,
    )
    cfg["guard_duplicate_min_chars"] = _bounded_int(
        raw_cfg.get("guard_duplicate_min_chars"),
        default=cfg["guard_duplicate_min_chars"],
        minimum=4,
        maximum=200,
    )
    cfg["guard_link_threshold"] = _bounded_int(
        raw_cfg.get("guard_link_threshold"),
        default=cfg["guard_link_threshold"],
        minimum=1,
        maximum=50,
    )
    cfg["guard_link_window_seconds"] = _bounded_int(
        raw_cfg.get("guard_link_window_seconds"),
        default=cfg["guard_link_window_seconds"],
        minimum=5,
        maximum=600,
    )
    cfg["guard_detect_joins"] = _as_bool(raw_cfg.get("guard_detect_joins"), cfg["guard_detect_joins"])
    cfg["guard_detect_mentions"] = _as_bool(
        raw_cfg.get("guard_detect_mentions"),
        cfg["guard_detect_mentions"],
    )
    cfg["guard_detect_duplicates"] = _as_bool(
        raw_cfg.get("guard_detect_duplicates"),
        cfg["guard_detect_duplicates"],
    )
    cfg["guard_detect_links"] = _as_bool(raw_cfg.get("guard_detect_links"), cfg["guard_detect_links"])

    return cfg


def sync_guard_settings(guild_cfg: dict[str, Any]) -> bool:
    normalized = normalize_guard_settings(guild_cfg)
    changed = False
    for key, value in normalized.items():
        if guild_cfg.get(key) != value:
            guild_cfg[key] = value
            changed = True
    return changed


def resolve_guard_preset_name(raw_preset: str | None) -> str | None:
    if raw_preset is None:
        return None
    token = str(raw_preset).strip().lower()
    if not token:
        return None
    token = GUARD_PRESET_ALIASES.get(token, token)
    if token not in GUARD_PRESETS:
        return None
    return token


def apply_guard_preset(guild_cfg: dict[str, Any], preset_name: str) -> bool:
    preset = GUARD_PRESETS.get(preset_name)
    if preset is None:
        return False
    guild_cfg.update(preset)
    sync_guard_settings(guild_cfg)
    return True


def count_guard_window(guild_id: int, window_seconds: int, now: datetime) -> int:
    tracker = new_account_message_tracker[guild_id]
    return _count_window(tracker, window_seconds, now)


def should_trigger_guard(guild_id: int, now: datetime) -> bool:
    cooldown_seconds = int(GUARD_CONFIG_DEFAULTS["guard_cooldown_seconds"])
    return _can_trigger(guild_id, now, cooldown_seconds, severity="elevated")


def _count_window(tracker: deque[datetime], window_seconds: int, now: datetime) -> int:
    cutoff = now - timedelta(seconds=max(1, int(window_seconds)))
    while tracker and tracker[0] < cutoff:
        tracker.popleft()
    return len(tracker)


def _record_channel_activity(guild_id: int, channel_id: int | None, now: datetime) -> None:
    if channel_id is None:
        return
    channel_activity = channel_activity_tracker[guild_id]
    channel_activity[channel_id] = now

    last_cleanup = channel_activity_cleanup_at.get(guild_id)
    if last_cleanup and (now - last_cleanup).total_seconds() < CHANNEL_ACTIVITY_CLEAN_INTERVAL_SECONDS:
        return
    channel_activity_cleanup_at[guild_id] = now

    cutoff = now - timedelta(seconds=ACTIVE_CHANNEL_WINDOW_SECONDS)
    stale_channel_ids = [cid for cid, seen_at in channel_activity.items() if seen_at < cutoff]
    for stale_id in stale_channel_ids:
        channel_activity.pop(stale_id, None)
    if not channel_activity:
        channel_activity_tracker.pop(guild_id, None)


def _contains_link(content: str) -> bool:
    return bool(URL_RE.search(content) or INVITE_RE.search(content))


def _message_fingerprint(content: str, min_chars: int) -> str | None:
    text = str(content or "").strip().lower()
    if not text:
        return None
    text = URL_RE.sub("<url>", text)
    text = MENTION_TOKEN_RE.sub("<mention>", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    if len(text) < min_chars:
        return None
    compact = text[:300]
    return hashlib.sha1(compact.encode("utf-8")).hexdigest()[:24]


def _cleanup_duplicate_tracker(guild_id: int, now: datetime, window_seconds: int) -> None:
    last_cleanup = duplicate_cleanup_at.get(guild_id)
    if last_cleanup and (now - last_cleanup).total_seconds() < DUPLICATE_TRACKER_CLEAN_INTERVAL_SECONDS:
        return
    duplicate_cleanup_at[guild_id] = now

    cutoff = now - timedelta(seconds=max(1, window_seconds))
    guild_tracker = duplicate_message_tracker.get(guild_id, {})
    for fingerprint, entries in list(guild_tracker.items()):
        while entries and entries[0][0] < cutoff:
            entries.popleft()
        if not entries:
            guild_tracker.pop(fingerprint, None)
    if not guild_tracker:
        duplicate_message_tracker.pop(guild_id, None)


def _record_duplicate_message(
    guild_id: int,
    *,
    fingerprint: str,
    author_id: int,
    now: datetime,
    window_seconds: int,
) -> tuple[int, int]:
    guild_tracker = duplicate_message_tracker[guild_id]
    entries = guild_tracker.setdefault(fingerprint, deque())
    entries.append((now, author_id))

    cutoff = now - timedelta(seconds=max(1, window_seconds))
    while entries and entries[0][0] < cutoff:
        entries.popleft()

    unique_authors = {author_id for _, author_id in entries}
    return len(entries), len(unique_authors)


def _determine_severity(reasons: list[dict[str, Any]]) -> str:
    if not reasons:
        return "none"
    max_ratio = max(float(reason.get("ratio", 0.0)) for reason in reasons)
    if len(reasons) >= 2 or max_ratio >= 2.0:
        return "critical"
    return "elevated"


def _can_trigger(guild_id: int, now: datetime, cooldown_seconds: int, severity: str) -> bool:
    last = guard_last_trigger.get(guild_id)
    if not last:
        return True
    elapsed = (now - last).total_seconds()
    if elapsed >= cooldown_seconds:
        return True
    if severity == "critical" and elapsed >= max(15, cooldown_seconds // 3):
        return True
    return False


def _resolve_alert_channel(
    guild: discord.Guild,
    guild_cfg: dict[str, Any],
    fallback_channel: discord.abc.GuildChannel | None = None,
) -> discord.abc.Messageable | None:
    ops_channel_id = _as_int(guild_cfg.get("ops_channel_id"))
    if ops_channel_id:
        configured = guild.get_channel(ops_channel_id)
        if configured and hasattr(configured, "send"):
            return configured

    if fallback_channel and hasattr(fallback_channel, "send"):
        return fallback_channel

    if guild.system_channel and hasattr(guild.system_channel, "send"):
        return guild.system_channel

    for channel in guild.text_channels:
        if hasattr(channel, "send"):
            return channel
    return None


def _select_slowmode_channels(
    guild: discord.Guild,
    guild_id: int,
    *,
    source_channel: discord.abc.GuildChannel | None,
    scope: str,
    max_channels: int,
) -> list[discord.TextChannel]:
    selected: list[discord.TextChannel] = []
    seen_ids: set[int] = set()

    if scope == "trigger" and isinstance(source_channel, discord.TextChannel):
        return [source_channel]

    if isinstance(source_channel, discord.TextChannel):
        selected.append(source_channel)
        seen_ids.add(source_channel.id)

    active_channels = channel_activity_tracker.get(guild_id, {})
    for channel_id, _ in sorted(active_channels.items(), key=lambda item: item[1], reverse=True):
        if len(selected) >= max_channels:
            break
        if channel_id in seen_ids:
            continue
        channel = guild.get_channel(channel_id)
        if isinstance(channel, discord.TextChannel):
            selected.append(channel)
            seen_ids.add(channel_id)

    if not selected and guild.text_channels:
        selected.append(guild.text_channels[0])

    return selected[:max_channels]


async def _apply_slowmode(
    guild: discord.Guild,
    guild_cfg: dict[str, Any],
    *,
    source_channel: discord.abc.GuildChannel | None,
    severity: str,
) -> list[str]:
    slowmode_seconds = int(guild_cfg.get("guard_slowmode_seconds", 0))
    critical_slowmode = int(guild_cfg.get("guard_critical_slowmode_seconds", 0))
    if severity == "critical" and critical_slowmode > 0:
        slowmode_seconds = max(slowmode_seconds, critical_slowmode)
    if slowmode_seconds <= 0:
        return []

    scope = str(guild_cfg.get("guard_slowmode_scope") or "trigger")
    max_channels = int(guild_cfg.get("guard_max_slowmode_channels", 1))
    channels = _select_slowmode_channels(
        guild,
        guild.id,
        source_channel=source_channel,
        scope=scope,
        max_channels=max(1, max_channels),
    )

    applied: list[str] = []
    for channel in channels:
        current_slowmode = int(getattr(channel, "slowmode_delay", 0) or 0)
        new_slowmode = max(current_slowmode, slowmode_seconds)
        try:
            if new_slowmode != current_slowmode:
                await channel.edit(
                    slowmode_delay=new_slowmode,
                    reason=f"Guard automatic defense ({severity})",
                )
            applied.append(f"{channel.mention} ({new_slowmode}s)")
        except Exception:
            continue
    return applied


async def _apply_timeout(member: discord.Member, timeout_seconds: int) -> bool:
    if timeout_seconds <= 0:
        return False
    if member.bot or member.guild.owner_id == member.id:
        return False

    now = datetime.now(timezone.utc)
    timeout_until = now + timedelta(seconds=timeout_seconds)

    try:
        await member.edit(
            timed_out_until=timeout_until,
            reason="Guard automatic defensive timeout",
        )
        return True
    except Exception:
        return False


async def _delete_message_if_enabled(message: discord.Message, guild_cfg: dict[str, Any]) -> bool:
    if not bool(guild_cfg.get("guard_delete_trigger_message", False)):
        return False
    if not str(message.content or "").strip():
        return False
    try:
        await message.delete()
        return True
    except Exception:
        return False


def _runtime_stats(guild_id: int) -> dict[str, Any]:
    stats = guard_runtime_stats[guild_id]
    if not isinstance(stats, dict):
        stats = _new_runtime_stats()
        guard_runtime_stats[guild_id] = stats
    return stats


def _record_guard_trigger(
    guild_id: int,
    *,
    now: datetime,
    reasons: list[dict[str, Any]],
    severity: str,
    actor_id: int | None,
) -> None:
    stats = _runtime_stats(guild_id)
    stats["triggers_total"] = int(stats.get("triggers_total", 0)) + 1
    stats["last_trigger_at"] = now
    stats["last_trigger_reasons"] = [str(reason.get("signal", "")) for reason in reasons]
    stats["last_trigger_severity"] = severity
    stats["last_trigger_actor_id"] = actor_id


def _record_guard_suppressed(guild_id: int) -> None:
    stats = _runtime_stats(guild_id)
    stats["suppressed_total"] = int(stats.get("suppressed_total", 0)) + 1


def _format_reason_lines(reasons: list[dict[str, Any]]) -> list[str]:
    return [f"- {reason.get('text', 'unknown signal')}" for reason in reasons]


def _short_channel_list(items: list[str], limit: int = 4) -> str:
    if not items:
        return "none"
    if len(items) <= limit:
        return ", ".join(items)
    head = ", ".join(items[:limit])
    return f"{head}, +{len(items) - limit} more"


async def handle_guard_message(
    *,
    bot: commands.Bot,
    message: discord.Message,
    guild_cfg: dict[str, Any],
    log_moderation_action: LogModerationActionCallback,
    send_ops_log: SendOpsLogCallback,
) -> None:
    try:
        if not isinstance(message.author, discord.Member):
            return
        guild = message.guild
        if guild is None:
            return

        if not _as_bool(guild_cfg.get("guard_enabled"), False):
            return
        cfg = normalize_guard_settings(guild_cfg)

        now = datetime.now(timezone.utc)
        channel_id = _as_int(getattr(message.channel, "id", None))
        _record_channel_activity(guild.id, channel_id, now)

        account_age = now - message.author.created_at.astimezone(timezone.utc)
        new_account_cutoff = timedelta(hours=int(cfg["guard_new_account_hours"]))
        is_new_account = account_age <= new_account_cutoff

        reasons: list[dict[str, Any]] = []

        if is_new_account:
            burst_tracker = new_account_message_tracker[guild.id]
            burst_tracker.append(now)
            burst_count = _count_window(burst_tracker, int(cfg["guard_window_seconds"]), now)
            threshold = int(cfg["guard_threshold"])
            if burst_count >= threshold:
                reasons.append(
                    {
                        "signal": "new_account_burst",
                        "text": f"{burst_count} messages from young accounts in {cfg['guard_window_seconds']}s",
                        "ratio": burst_count / max(1, threshold),
                    }
                )

        if bool(cfg.get("guard_detect_mentions")):
            mention_count = len(message.mentions) + len(message.role_mentions)
            if message.mention_everyone:
                mention_count = max(mention_count, int(cfg["guard_mention_per_message"]))
            mention_limit = int(cfg["guard_mention_per_message"])
            if mention_count >= mention_limit:
                mention_tracker = mention_rate_tracker[guild.id]
                mention_tracker.append(now)
                mention_burst = _count_window(
                    mention_tracker,
                    int(cfg["guard_mention_window_seconds"]),
                    now,
                )
                mention_threshold = int(cfg["guard_mention_burst_threshold"])
                if mention_burst >= mention_threshold:
                    reasons.append(
                        {
                            "signal": "mention_spam",
                            "text": (
                                f"mention spam burst ({mention_burst} flagged messages in "
                                f"{cfg['guard_mention_window_seconds']}s)"
                            ),
                            "ratio": mention_burst / max(1, mention_threshold),
                        }
                    )

        content = str(message.content or "")
        if bool(cfg.get("guard_detect_links")) and is_new_account and _contains_link(content):
            link_tracker = link_rate_tracker[guild.id]
            link_tracker.append(now)
            link_burst = _count_window(link_tracker, int(cfg["guard_link_window_seconds"]), now)
            link_threshold = int(cfg["guard_link_threshold"])
            if link_burst >= link_threshold:
                reasons.append(
                    {
                        "signal": "link_burst",
                        "text": f"link burst ({link_burst} links in {cfg['guard_link_window_seconds']}s)",
                        "ratio": link_burst / max(1, link_threshold),
                    }
                )

        if bool(cfg.get("guard_detect_duplicates")):
            _cleanup_duplicate_tracker(guild.id, now, int(cfg["guard_duplicate_window_seconds"]))
            fingerprint = _message_fingerprint(content, int(cfg["guard_duplicate_min_chars"]))
            if fingerprint:
                dup_count, dup_authors = _record_duplicate_message(
                    guild.id,
                    fingerprint=fingerprint,
                    author_id=message.author.id,
                    now=now,
                    window_seconds=int(cfg["guard_duplicate_window_seconds"]),
                )
                duplicate_threshold = int(cfg["guard_duplicate_threshold"])
                if dup_count >= duplicate_threshold and dup_authors >= 2:
                    reasons.append(
                        {
                            "signal": "duplicate_spam",
                            "text": (
                                f"duplicate content burst ({dup_count} messages / {dup_authors} accounts in "
                                f"{cfg['guard_duplicate_window_seconds']}s)"
                            ),
                            "ratio": dup_count / max(1, duplicate_threshold),
                        }
                    )

        if not reasons:
            return

        severity = _determine_severity(reasons)
        cooldown_seconds = int(cfg["guard_cooldown_seconds"])
        if not _can_trigger(guild.id, now, cooldown_seconds, severity):
            _record_guard_suppressed(guild.id)
            return

        guard_last_trigger[guild.id] = now
        slowmode_actions = await _apply_slowmode(
            guild,
            cfg,
            source_channel=message.channel,
            severity=severity,
        )

        timeout_seconds = int(cfg.get("guard_timeout_seconds", 0))
        timeout_applied = False
        if timeout_seconds > 0 and (is_new_account or severity == "critical"):
            timeout_applied = await _apply_timeout(message.author, timeout_seconds)

        message_deleted = await _delete_message_if_enabled(message, cfg)

        action_lines: list[str] = []
        if slowmode_actions:
            action_lines.append(f"Slowmode applied: {_short_channel_list(slowmode_actions)}")
        else:
            action_lines.append("Slowmode unchanged")
        if timeout_applied:
            action_lines.append(f"Timed out user for {timeout_seconds}s")
        if message_deleted:
            action_lines.append("Deleted triggering message")

        alert_text = (
            f"🚨 **Guard Triggered ({severity.upper()})**\n"
            f"User: {message.author.mention} (`{message.author.id}`)\n"
            f"Channel: {getattr(message.channel, 'mention', '#unknown')}\n"
            "Signals:\n"
            + "\n".join(_format_reason_lines(reasons))
            + "\nActions:\n"
            + "\n".join(f"- {line}" for line in action_lines)
        )

        alert_channel = _resolve_alert_channel(guild, guild_cfg, message.channel)
        if alert_channel and hasattr(alert_channel, "send"):
            try:
                await alert_channel.send(alert_text)
            except Exception:
                pass

        details = (
            f"severity={severity}; signals={','.join(reason['signal'] for reason in reasons)}; "
            f"slowmode_targets={len(slowmode_actions)}; timeout={timeout_applied}; "
            f"deleted_message={message_deleted}; actor_id={message.author.id}"
        )
        case_id = log_moderation_action(
            guild_id=guild.id,
            action="guard_trigger",
            actor_id=bot.user.id if bot.user else 0,
            target_id=message.author.id,
            reason=f"Automated guard defense ({severity})",
            details=details,
            undoable=False,
        )
        await send_ops_log(
            guild,
            (
                f"🛡️ Case `{case_id}` guard trigger in {getattr(message.channel, 'mention', '#unknown')}: "
                f"{', '.join(reason['signal'] for reason in reasons)} ({severity})"
            ),
        )

        _record_guard_trigger(
            guild.id,
            now=now,
            reasons=reasons,
            severity=severity,
            actor_id=message.author.id,
        )
    except Exception as exc:
        print(f"[GUARD] Message handling error in guild {getattr(message.guild, 'id', 'unknown')}: {exc}")


async def handle_guard_member_join(
    *,
    bot: commands.Bot,
    member: discord.Member,
    guild_cfg: dict[str, Any],
    log_moderation_action: LogModerationActionCallback,
    send_ops_log: SendOpsLogCallback,
) -> None:
    try:
        guild = member.guild
        if not _as_bool(guild_cfg.get("guard_enabled"), False):
            return
        cfg = normalize_guard_settings(guild_cfg)
        if not bool(cfg.get("guard_detect_joins", True)):
            return

        now = datetime.now(timezone.utc)
        join_tracker = join_rate_tracker[guild.id]
        join_tracker.append(now)

        join_count = _count_window(join_tracker, int(cfg["guard_join_window_seconds"]), now)
        join_threshold = int(cfg["guard_join_threshold"])
        if join_count < join_threshold:
            return

        reasons = [
            {
                "signal": "join_burst",
                "text": f"join burst ({join_count} joins in {cfg['guard_join_window_seconds']}s)",
                "ratio": join_count / max(1, join_threshold),
            }
        ]
        severity = _determine_severity(reasons)
        cooldown_seconds = int(cfg["guard_cooldown_seconds"])
        if not _can_trigger(guild.id, now, cooldown_seconds, severity):
            _record_guard_suppressed(guild.id)
            return

        guard_last_trigger[guild.id] = now
        slowmode_actions = await _apply_slowmode(
            guild,
            cfg,
            source_channel=None,
            severity=severity,
        )

        action_lines: list[str] = []
        if slowmode_actions:
            action_lines.append(f"Slowmode applied: {_short_channel_list(slowmode_actions)}")
        else:
            action_lines.append("Slowmode unchanged")

        alert_channel = _resolve_alert_channel(guild, guild_cfg, guild.system_channel)
        alert_text = (
            f"🚨 **Guard Triggered ({severity.upper()})**\n"
            f"Member joined: {member.mention} (`{member.id}`)\n"
            "Signals:\n"
            + "\n".join(_format_reason_lines(reasons))
            + "\nActions:\n"
            + "\n".join(f"- {line}" for line in action_lines)
        )
        if alert_channel and hasattr(alert_channel, "send"):
            try:
                await alert_channel.send(alert_text)
            except Exception:
                pass

        details = (
            f"severity={severity}; signals=join_burst; join_count={join_count}; "
            f"slowmode_targets={len(slowmode_actions)}"
        )
        case_id = log_moderation_action(
            guild_id=guild.id,
            action="guard_trigger",
            actor_id=bot.user.id if bot.user else 0,
            target_id=member.id,
            reason=f"Automated join-burst defense ({severity})",
            details=details,
            undoable=False,
        )
        await send_ops_log(
            guild,
            f"🛡️ Case `{case_id}` guard join-burst trigger: {join_count} joins/{cfg['guard_join_window_seconds']}s",
        )

        _record_guard_trigger(
            guild.id,
            now=now,
            reasons=reasons,
            severity=severity,
            actor_id=member.id,
        )
    except Exception as exc:
        print(f"[GUARD] Join handling error in guild {member.guild.id}: {exc}")


def clear_guard_runtime(guild_id: int) -> None:
    new_account_message_tracker.pop(guild_id, None)
    join_rate_tracker.pop(guild_id, None)
    mention_rate_tracker.pop(guild_id, None)
    link_rate_tracker.pop(guild_id, None)
    duplicate_message_tracker.pop(guild_id, None)
    channel_activity_tracker.pop(guild_id, None)
    channel_activity_cleanup_at.pop(guild_id, None)
    duplicate_cleanup_at.pop(guild_id, None)
    guard_last_trigger.pop(guild_id, None)
    guard_runtime_stats.pop(guild_id, None)


def get_guard_runtime_snapshot(guild_id: int, guild_cfg: dict[str, Any]) -> dict[str, Any]:
    cfg = normalize_guard_settings(guild_cfg)
    now = datetime.now(timezone.utc)

    snapshot = {
        "new_account_window_count": _count_window(
            new_account_message_tracker[guild_id],
            int(cfg["guard_window_seconds"]),
            now,
        ),
        "join_window_count": _count_window(
            join_rate_tracker[guild_id],
            int(cfg["guard_join_window_seconds"]),
            now,
        ),
        "mention_window_count": _count_window(
            mention_rate_tracker[guild_id],
            int(cfg["guard_mention_window_seconds"]),
            now,
        ),
        "link_window_count": _count_window(
            link_rate_tracker[guild_id],
            int(cfg["guard_link_window_seconds"]),
            now,
        ),
        "duplicate_clusters": len(duplicate_message_tracker.get(guild_id, {})),
        "active_channels": len(channel_activity_tracker.get(guild_id, {})),
    }

    stats = _runtime_stats(guild_id)
    snapshot["triggers_total"] = int(stats.get("triggers_total", 0))
    snapshot["suppressed_total"] = int(stats.get("suppressed_total", 0))
    snapshot["last_trigger_at"] = stats.get("last_trigger_at")
    snapshot["last_trigger_reasons"] = list(stats.get("last_trigger_reasons") or [])
    snapshot["last_trigger_severity"] = stats.get("last_trigger_severity")
    snapshot["last_trigger_actor_id"] = stats.get("last_trigger_actor_id")

    return snapshot


def _guard_summary(cfg: dict[str, Any]) -> str:
    return (
        f"🛡️ Guard\n"
        f"- Enabled: `{cfg['guard_enabled']}`\n"
        f"- New account burst: `{cfg['guard_threshold']}` in `{cfg['guard_window_seconds']}s` (account age `{cfg['guard_new_account_hours']}h`)\n"
        f"- Slowmode: `{cfg['guard_slowmode_seconds']}s` (critical `{cfg['guard_critical_slowmode_seconds']}s`)\n"
        f"- Scope: `{cfg['guard_slowmode_scope']}` across up to `{cfg['guard_max_slowmode_channels']}` channel(s)\n"
        f"- Cooldown: `{cfg['guard_cooldown_seconds']}s`\n"
        f"- Timeout on trigger: `{cfg['guard_timeout_seconds']}s`\n"
        f"- Delete trigger message: `{cfg['guard_delete_trigger_message']}`"
    )


def _guard_advanced_summary(cfg: dict[str, Any]) -> str:
    return (
        f"⚙️ Guard Advanced\n"
        f"- Join detection: `{cfg['guard_detect_joins']}` • `{cfg['guard_join_threshold']}` in `{cfg['guard_join_window_seconds']}s`\n"
        f"- Mention detection: `{cfg['guard_detect_mentions']}` • `{cfg['guard_mention_burst_threshold']}` flagged msgs in `{cfg['guard_mention_window_seconds']}s` (per-msg `{cfg['guard_mention_per_message']}`)\n"
        f"- Duplicate detection: `{cfg['guard_detect_duplicates']}` • `{cfg['guard_duplicate_threshold']}` in `{cfg['guard_duplicate_window_seconds']}s` (min chars `{cfg['guard_duplicate_min_chars']}`)\n"
        f"- Link detection: `{cfg['guard_detect_links']}` • `{cfg['guard_link_threshold']}` in `{cfg['guard_link_window_seconds']}s`"
    )


def setup_guard_module(
    bot: commands.Bot,
    *,
    require_mod_context: RequireModContextCallback,
    save_settings: SaveSettingsCallback,
) -> None:
    @bot.hybrid_command(name="guard")
    async def guard(
        ctx: commands.Context,
        enabled: bool | None = None,
        preset: str | None = None,
        threshold: int | None = None,
        window_seconds: int | None = None,
        slowmode_seconds: int | None = None,
        new_account_hours: int | None = None,
        cooldown_seconds: int | None = None,
        scope: str | None = None,
        timeout_seconds: int | None = None,
    ):
        """Configure anti-raid guard baseline settings and presets."""
        result = await require_mod_context(ctx)
        if not result:
            return
        _, guild_cfg = result

        if preset is not None:
            preset_name = resolve_guard_preset_name(preset)
            if preset_name is None:
                valid = ", ".join(sorted(GUARD_PRESETS))
                await ctx.send(f"⚠️ Unknown preset `{preset}`. Valid presets: {valid}")
                return
            apply_guard_preset(guild_cfg, preset_name)

        if enabled is not None:
            guild_cfg["guard_enabled"] = enabled
        if threshold is not None:
            guild_cfg["guard_threshold"] = max(3, min(100, int(threshold)))
        if window_seconds is not None:
            guild_cfg["guard_window_seconds"] = max(5, min(300, int(window_seconds)))
        if slowmode_seconds is not None:
            guild_cfg["guard_slowmode_seconds"] = max(0, min(21600, int(slowmode_seconds)))
        if new_account_hours is not None:
            guild_cfg["guard_new_account_hours"] = max(1, min(168, int(new_account_hours)))
        if cooldown_seconds is not None:
            guild_cfg["guard_cooldown_seconds"] = max(30, min(3600, int(cooldown_seconds)))
        if scope is not None:
            normalized_scope = _normalize_scope(scope)
            guild_cfg["guard_slowmode_scope"] = normalized_scope
        if timeout_seconds is not None:
            guild_cfg["guard_timeout_seconds"] = max(0, min(MAX_TIMEOUT_SECONDS, int(timeout_seconds)))

        sync_guard_settings(guild_cfg)
        save_settings()
        await ctx.send(_guard_summary(normalize_guard_settings(guild_cfg)))

    @bot.hybrid_command(name="guardadvanced")
    async def guardadvanced(
        ctx: commands.Context,
        join_threshold: int | None = None,
        join_window_seconds: int | None = None,
        mention_per_message: int | None = None,
        mention_burst_threshold: int | None = None,
        mention_window_seconds: int | None = None,
        duplicate_threshold: int | None = None,
        duplicate_window_seconds: int | None = None,
        duplicate_min_chars: int | None = None,
        link_threshold: int | None = None,
        link_window_seconds: int | None = None,
        max_slowmode_channels: int | None = None,
        critical_slowmode_seconds: int | None = None,
        detect_joins: bool | None = None,
        detect_mentions: bool | None = None,
        detect_duplicates: bool | None = None,
        detect_links: bool | None = None,
        delete_trigger_message: bool | None = None,
    ):
        """Tune advanced anti-raid detectors and mitigation behavior."""
        result = await require_mod_context(ctx)
        if not result:
            return
        _, guild_cfg = result

        if join_threshold is not None:
            guild_cfg["guard_join_threshold"] = max(2, min(100, int(join_threshold)))
        if join_window_seconds is not None:
            guild_cfg["guard_join_window_seconds"] = max(5, min(600, int(join_window_seconds)))
        if mention_per_message is not None:
            guild_cfg["guard_mention_per_message"] = max(1, min(50, int(mention_per_message)))
        if mention_burst_threshold is not None:
            guild_cfg["guard_mention_burst_threshold"] = max(1, min(50, int(mention_burst_threshold)))
        if mention_window_seconds is not None:
            guild_cfg["guard_mention_window_seconds"] = max(5, min(600, int(mention_window_seconds)))
        if duplicate_threshold is not None:
            guild_cfg["guard_duplicate_threshold"] = max(2, min(30, int(duplicate_threshold)))
        if duplicate_window_seconds is not None:
            guild_cfg["guard_duplicate_window_seconds"] = max(5, min(300, int(duplicate_window_seconds)))
        if duplicate_min_chars is not None:
            guild_cfg["guard_duplicate_min_chars"] = max(4, min(200, int(duplicate_min_chars)))
        if link_threshold is not None:
            guild_cfg["guard_link_threshold"] = max(1, min(50, int(link_threshold)))
        if link_window_seconds is not None:
            guild_cfg["guard_link_window_seconds"] = max(5, min(600, int(link_window_seconds)))
        if max_slowmode_channels is not None:
            guild_cfg["guard_max_slowmode_channels"] = max(1, min(25, int(max_slowmode_channels)))
        if critical_slowmode_seconds is not None:
            guild_cfg["guard_critical_slowmode_seconds"] = max(0, min(21600, int(critical_slowmode_seconds)))
        if detect_joins is not None:
            guild_cfg["guard_detect_joins"] = detect_joins
        if detect_mentions is not None:
            guild_cfg["guard_detect_mentions"] = detect_mentions
        if detect_duplicates is not None:
            guild_cfg["guard_detect_duplicates"] = detect_duplicates
        if detect_links is not None:
            guild_cfg["guard_detect_links"] = detect_links
        if delete_trigger_message is not None:
            guild_cfg["guard_delete_trigger_message"] = delete_trigger_message

        sync_guard_settings(guild_cfg)
        save_settings()
        await ctx.send(_guard_advanced_summary(normalize_guard_settings(guild_cfg)))

    @bot.hybrid_command(name="guardstatus")
    async def guardstatus(ctx: commands.Context):
        """Show guard configuration and current live detector metrics."""
        result = await require_mod_context(ctx)
        if not result:
            return
        guild, guild_cfg = result

        cfg = normalize_guard_settings(guild_cfg)
        snapshot = get_guard_runtime_snapshot(guild.id, guild_cfg)

        embed_color = discord.Color.red() if cfg.get("guard_enabled") else discord.Color.dark_grey()
        embed = discord.Embed(title=f"Guard Status: {guild.name}", color=embed_color)
        embed.add_field(
            name="Mode",
            value=(
                f"Enabled: `{cfg['guard_enabled']}`\n"
                f"Preset-ready values active\n"
                f"Cooldown: `{cfg['guard_cooldown_seconds']}s`"
            ),
            inline=True,
        )
        embed.add_field(
            name="Mitigation",
            value=(
                f"Slowmode: `{cfg['guard_slowmode_seconds']}s` (critical `{cfg['guard_critical_slowmode_seconds']}s`)\n"
                f"Scope: `{cfg['guard_slowmode_scope']}` / `{cfg['guard_max_slowmode_channels']}` channel(s)\n"
                f"Timeout: `{cfg['guard_timeout_seconds']}s`"
            ),
            inline=True,
        )
        embed.add_field(
            name="Detection",
            value=(
                f"New accounts: `{cfg['guard_threshold']}` in `{cfg['guard_window_seconds']}s` (<=`{cfg['guard_new_account_hours']}h`)\n"
                f"Joins: `{cfg['guard_detect_joins']}` (`{cfg['guard_join_threshold']}`/{cfg['guard_join_window_seconds']}s)\n"
                f"Mentions: `{cfg['guard_detect_mentions']}` (`{cfg['guard_mention_burst_threshold']}`/{cfg['guard_mention_window_seconds']}s)"
            ),
            inline=False,
        )
        embed.add_field(
            name="Live Window",
            value=(
                f"New-account msgs: `{snapshot['new_account_window_count']}`\n"
                f"Joins: `{snapshot['join_window_count']}`\n"
                f"Mention spikes: `{snapshot['mention_window_count']}`\n"
                f"Link spikes: `{snapshot['link_window_count']}`\n"
                f"Duplicate clusters: `{snapshot['duplicate_clusters']}`\n"
                f"Tracked channels: `{snapshot['active_channels']}`"
            ),
            inline=True,
        )
        last_trigger_at = snapshot.get("last_trigger_at")
        if isinstance(last_trigger_at, datetime):
            last_trigger_token = f"<t:{int(last_trigger_at.timestamp())}:R>"
        else:
            last_trigger_token = "never"
        last_reasons = ", ".join(snapshot.get("last_trigger_reasons") or []) or "none"
        embed.add_field(
            name="Runtime",
            value=(
                f"Triggers: `{snapshot['triggers_total']}`\n"
                f"Suppressed (cooldown): `{snapshot['suppressed_total']}`\n"
                f"Last trigger: {last_trigger_token}\n"
                f"Severity: `{snapshot.get('last_trigger_severity') or 'n/a'}`\n"
                f"Signals: `{last_reasons}`"
            ),
            inline=True,
        )
        await ctx.send(embed=embed)

    @bot.hybrid_command(name="guardreset")
    async def guardreset(ctx: commands.Context):
        """Clear in-memory guard trackers and counters for this server."""
        result = await require_mod_context(ctx)
        if not result:
            return
        guild, _ = result
        clear_guard_runtime(guild.id)
        await ctx.send("✅ Guard runtime trackers were reset for this server.")
