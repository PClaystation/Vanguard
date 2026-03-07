# Copyright (c) 2026 Continental. All rights reserved.
# Licensed under the Vanguard Proprietary Source-Available License (see /LICENSE).

import asyncio
from collections import defaultdict, deque
import json
import os
import platform
import random
import re
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import discord
from discord import app_commands
from discord.ext import commands
from mcstatus import JavaServer
import requests
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False

from data_paths import resolve_data_file
from vote import (
    ballot_to_text,
    option_label,
    restore_vote_state,
    setup_vote_module,
    tally_vote,
    votes as vote_store,
)

load_dotenv()

BOT_PREFIX = os.getenv("BOT_PREFIX", "/")
MAX_PREFIX_LENGTH = 5


def _parse_env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_env_int(name: str, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        return default
    if maximum is not None and value > maximum:
        return default
    return value


def _parse_env_optional_int(name: str, minimum: int | None = None, maximum: int | None = None) -> int | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    if minimum is not None and value < minimum:
        return None
    if maximum is not None and value > maximum:
        return None
    return value


def _parse_env_optional_float(
    name: str,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float | None:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if minimum is not None and value < minimum:
        return None
    if maximum is not None and value > maximum:
        return None
    return value


def _parse_env_int_set(name: str) -> set[int]:
    raw = os.getenv(name, "")
    values: set[int] = set()
    for chunk in raw.split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            parsed = int(token)
        except ValueError:
            continue
        if parsed > 0:
            values.add(parsed)
    return values


def _resolve_ai_base_url(explicit_base_url: str, legacy_url: str) -> str:
    explicit = explicit_base_url.strip().rstrip("/")
    if explicit:
        return explicit

    candidate = legacy_url.strip().rstrip("/")
    if not candidate:
        return "http://localhost:3001"

    known_suffixes = (
        "/chat/stream",
        "/reload-context",
        "/health",
        "/models",
        "/stats",
        "/chat",
        "/ask",
    )
    for suffix in known_suffixes:
        if candidate.endswith(suffix):
            base = candidate[: -len(suffix)].rstrip("/")
            return base if base else "http://localhost:3001"
    return candidate


def _extract_ai_answer(payload: Any) -> str:
    if isinstance(payload, str):
        return payload.strip()
    if not isinstance(payload, dict):
        return ""

    fields = ("answer", "response", "message", "text", "output")
    for field in fields:
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()

    nested = payload.get("data")
    if isinstance(nested, dict):
        return _extract_ai_answer(nested)
    return ""


def _extract_model_count(payload: Any) -> int | None:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        models = payload.get("models")
        if isinstance(models, list):
            return len(models)
    return None


def _build_ai_session_id(guild_id: int | None, channel_id: int | None, user_id: int) -> str:
    scope = guild_id if guild_id is not None else "dm"
    return f"discord:{scope}:{channel_id or 0}:{user_id}"


def _build_ai_options() -> dict[str, Any]:
    options: dict[str, Any] = {}
    if AI_TEMPERATURE is not None:
        options["temperature"] = AI_TEMPERATURE
    if AI_TOP_P is not None:
        options["top_p"] = AI_TOP_P
    if AI_NUM_PREDICT is not None:
        options["num_predict"] = AI_NUM_PREDICT
    if AI_REPEAT_PENALTY is not None:
        options["repeat_penalty"] = AI_REPEAT_PENALTY
    return options


_legacy_ai_url = os.getenv("AI_SERVER_URL", "http://localhost:3001/ask")
AI_SERVER_BASE_URL = _resolve_ai_base_url(os.getenv("AI_SERVER_BASE_URL", ""), _legacy_ai_url)
AI_ASK_URL = os.getenv("AI_ASK_URL", f"{AI_SERVER_BASE_URL}/ask").strip()
AI_CHAT_URL = os.getenv("AI_CHAT_URL", f"{AI_SERVER_BASE_URL}/chat").strip()
AI_HEALTH_URL = os.getenv("AI_HEALTH_URL", f"{AI_SERVER_BASE_URL}/health").strip()
AI_MODELS_URL = os.getenv("AI_MODELS_URL", f"{AI_SERVER_BASE_URL}/models").strip()
AI_SESSION_URL = os.getenv("AI_SESSION_URL", f"{AI_SERVER_BASE_URL}/session").strip().rstrip("/")
AI_SERVER_URL = AI_ASK_URL
AI_REQUEST_TIMEOUT_SECONDS = _parse_env_int("AI_REQUEST_TIMEOUT_SECONDS", 20, minimum=2, maximum=120)
AI_CHAT_STYLE = os.getenv("AI_CHAT_STYLE", "balanced").strip().lower()
if AI_CHAT_STYLE not in {"concise", "balanced", "detailed"}:
    AI_CHAT_STYLE = "balanced"
AI_HISTORY_MESSAGES = _parse_env_int("AI_HISTORY_MESSAGES", 12, minimum=1, maximum=24)
AI_USE_CONTEXT = _parse_env_bool("AI_USE_CONTEXT", True)
AI_USE_CACHE = _parse_env_bool("AI_USE_CACHE", True)
AI_INCLUDE_DEBUG = _parse_env_bool("AI_INCLUDE_DEBUG", False)
AI_MODEL = os.getenv("AI_MODEL", "").strip() or None
AI_TEMPERATURE = _parse_env_optional_float("AI_TEMPERATURE", minimum=0.0, maximum=2.0)
AI_TOP_P = _parse_env_optional_float("AI_TOP_P", minimum=0.0, maximum=1.0)
AI_NUM_PREDICT = _parse_env_optional_int("AI_NUM_PREDICT", minimum=1, maximum=4096)
AI_REPEAT_PENALTY = _parse_env_optional_float("AI_REPEAT_PENALTY", minimum=0.8, maximum=2.0)
FLAG_USER_URL = os.getenv("FLAG_USER_URL", "http://localhost:3001/fuck")
UNFLAG_USER_URL = os.getenv("UNFLAG_USER_URL", "http://localhost:3001/unfuck")
VANGUARD_INSTANCE_ID = os.getenv("VANGUARD_INSTANCE_ID", "").strip()
VANGUARD_BACKEND_API_KEY = os.getenv("VANGUARD_BACKEND_API_KEY", "").strip()
VANGUARD_BACKEND_KEY_HEADER = (
    os.getenv("VANGUARD_BACKEND_KEY_HEADER", "X-Vanguard-Api-Key").strip()
    or "X-Vanguard-Api-Key"
)
VANGUARD_INSTANCE_HEADER = (
    os.getenv("VANGUARD_INSTANCE_HEADER", "X-Vanguard-Instance-Id").strip()
    or "X-Vanguard-Instance-Id"
)
VANGUARD_ALLOWED_GUILD_IDS = _parse_env_int_set("VANGUARD_ALLOWED_GUILD_IDS")
VANGUARD_LICENSE_VERIFY_URL = os.getenv("VANGUARD_LICENSE_VERIFY_URL", "").strip()
VANGUARD_LICENSE_KEY = os.getenv("VANGUARD_LICENSE_KEY", "").strip()
VANGUARD_REQUIRE_LICENSE = _parse_env_bool("VANGUARD_REQUIRE_LICENSE", False)
VANGUARD_LICENSE_RECHECK_SECONDS = _parse_env_int(
    "VANGUARD_LICENSE_RECHECK_SECONDS",
    900,
    minimum=60,
    maximum=86400,
)
MC_DEFAULT_HOST = os.getenv("MC_DEFAULT_HOST")
try:
    MC_DEFAULT_PORT = int(os.getenv("MC_DEFAULT_PORT", "25565"))
except ValueError:
    MC_DEFAULT_PORT = 25565

SETTINGS_FILE = resolve_data_file("settings.json")
REMINDERS_FILE = resolve_data_file("reminders.json")
MOD_LOG_FILE = resolve_data_file("modlog.json")
START_TIME = datetime.now(timezone.utc)
REMINDER_CHECK_SECONDS = 15
MAX_REMINDER_SECONDS = 60 * 60 * 24 * 30
MAX_TIMEOUT_SECONDS = 60 * 60 * 24 * 28
MAX_CASES_PER_GUILD = 1000
GUARD_COOLDOWN_SECONDS = 300
MAX_DISCORD_MESSAGE_CHARS = 1900
MAX_DISCORD_EMBED_DESCRIPTION_CHARS = 3900

PRIVACY_URL = os.getenv("PRIVACY_POLICY_URL", "").strip()
TOS_URL = os.getenv("TERMS_OF_SERVICE_URL", "").strip()

ID_RE = re.compile(r"(\d{17,20})")
DURATION_TOKEN_RE = re.compile(r"(\d+)([smhdw])")

AppCommandChannelType = getattr(app_commands, "AppCommandChannel", discord.TextChannel)
AppCommandThreadType = getattr(app_commands, "AppCommandThread", discord.Thread)

ConfigChannelInput = (
    discord.TextChannel
    | discord.VoiceChannel
    | discord.StageChannel
    | discord.CategoryChannel
    | discord.ForumChannel
    | discord.Thread
    | AppCommandChannelType
    | AppCommandThreadType
)


def default_guild_settings() -> dict[str, Any]:
    return {
        "prefix": None,
        "welcome_channel_id": None,
        "welcome_role_id": None,
        "welcome_message": None,
        "ops_channel_id": None,
        "log_channel_id": None,
        "lockdown_role_id": None,
        "mod_role_ids": [],
        "mc_host": None,
        "mc_port": 25565,
        "guard_enabled": False,
        "guard_window_seconds": 30,
        "guard_threshold": 8,
        "guard_new_account_hours": 24,
        "guard_slowmode_seconds": 30,
    }


def default_settings() -> dict[str, Any]:
    return {
        "owner_only": False,
        "guilds": {},
    }


def as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def clamp_text(value: Any, limit: int) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3] + "..."


def write_json_atomic(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)
    os.replace(temp_path, path)


def reset_json_file(path: str, payload: Any) -> Any:
    try:
        write_json_atomic(path, payload)
    except OSError as exc:
        print(f"[FILE] Failed writing {path}: {exc}")
    return payload


def parse_datetime_utc(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_settings(raw: Any) -> dict[str, Any]:
    normalized = default_settings()
    if not isinstance(raw, dict):
        return normalized

    normalized["owner_only"] = bool(raw.get("owner_only", False))
    raw_guilds = raw.get("guilds", {})
    if not isinstance(raw_guilds, dict):
        return normalized

    guilds: dict[str, dict[str, Any]] = {}
    for guild_id, cfg in raw_guilds.items():
        if not isinstance(cfg, dict):
            continue

        guild_cfg = default_guild_settings()

        prefix = cfg.get("prefix")
        if isinstance(prefix, str):
            prefix = prefix.strip()
            guild_cfg["prefix"] = prefix[:MAX_PREFIX_LENGTH] if prefix else None

        guild_cfg["welcome_channel_id"] = as_int(cfg.get("welcome_channel_id"))
        guild_cfg["welcome_role_id"] = as_int(cfg.get("welcome_role_id"))
        guild_cfg["ops_channel_id"] = as_int(cfg.get("ops_channel_id"))
        guild_cfg["log_channel_id"] = as_int(cfg.get("log_channel_id"))
        guild_cfg["lockdown_role_id"] = as_int(cfg.get("lockdown_role_id"))

        welcome_message = cfg.get("welcome_message")
        if isinstance(welcome_message, str):
            welcome_message = welcome_message.strip()
            guild_cfg["welcome_message"] = welcome_message[:500] if welcome_message else None

        role_ids = cfg.get("mod_role_ids", [])
        if isinstance(role_ids, list):
            guild_cfg["mod_role_ids"] = [
                role_id for role_id in (as_int(value) for value in role_ids) if role_id
            ]

        host = cfg.get("mc_host")
        guild_cfg["mc_host"] = host.strip() if isinstance(host, str) and host.strip() else None

        port = as_int(cfg.get("mc_port"))
        guild_cfg["mc_port"] = port if port and 1 <= port <= 65535 else 25565

        guard_enabled = cfg.get("guard_enabled")
        guild_cfg["guard_enabled"] = bool(guard_enabled)

        guard_window_seconds = as_int(cfg.get("guard_window_seconds"))
        guild_cfg["guard_window_seconds"] = (
            guard_window_seconds if guard_window_seconds and 5 <= guard_window_seconds <= 300 else 30
        )

        guard_threshold = as_int(cfg.get("guard_threshold"))
        guild_cfg["guard_threshold"] = (
            guard_threshold if guard_threshold and 3 <= guard_threshold <= 100 else 8
        )

        guard_new_account_hours = as_int(cfg.get("guard_new_account_hours"))
        guild_cfg["guard_new_account_hours"] = (
            guard_new_account_hours if guard_new_account_hours and 1 <= guard_new_account_hours <= 168 else 24
        )

        guard_slowmode_seconds = as_int(cfg.get("guard_slowmode_seconds"))
        guild_cfg["guard_slowmode_seconds"] = (
            guard_slowmode_seconds
            if guard_slowmode_seconds is not None and 0 <= guard_slowmode_seconds <= 21600
            else 30
        )

        guilds[str(guild_id)] = guild_cfg

    normalized["guilds"] = guilds
    return normalized


def load_settings() -> dict[str, Any]:
    if not os.path.exists(SETTINGS_FILE):
        return reset_json_file(SETTINGS_FILE, default_settings())

    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as file:
            return normalize_settings(json.load(file))
    except (json.JSONDecodeError, OSError):
        return reset_json_file(SETTINGS_FILE, default_settings())


settings = load_settings()


def save_settings() -> None:
    try:
        write_json_atomic(SETTINGS_FILE, settings)
    except OSError as exc:
        print(f"[FILE] Failed saving settings: {exc}")


def get_guild_config(guild_id: int) -> dict[str, Any]:
    guilds = settings.setdefault("guilds", {})
    key = str(guild_id)
    cfg = guilds.get(key)
    if not isinstance(cfg, dict):
        cfg = default_guild_settings()
        guilds[key] = cfg
    return cfg


def normalize_reminders(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    normalized: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        reminder_id = as_int(item.get("id"))
        user_id = as_int(item.get("user_id"))
        channel_id = as_int(item.get("channel_id"))
        guild_id = as_int(item.get("guild_id"))
        message = item.get("message", "")
        due_at = parse_datetime_utc(item.get("due_at"))
        if (
            reminder_id is None
            or user_id is None
            or channel_id is None
            or due_at is None
            or not isinstance(message, str)
            or not message.strip()
        ):
            continue
        normalized.append(
            {
                "id": reminder_id,
                "user_id": user_id,
                "channel_id": channel_id,
                "guild_id": guild_id,
                "message": message.strip()[:300],
                "due_at": due_at.isoformat(),
            }
        )
    normalized.sort(key=lambda reminder: reminder["due_at"])
    return normalized


def load_reminders() -> list[dict[str, Any]]:
    if not os.path.exists(REMINDERS_FILE):
        return reset_json_file(REMINDERS_FILE, [])
    try:
        with open(REMINDERS_FILE, "r", encoding="utf-8") as file:
            return normalize_reminders(json.load(file))
    except (json.JSONDecodeError, OSError):
        return reset_json_file(REMINDERS_FILE, [])


reminders = load_reminders()
next_reminder_id = max((reminder["id"] for reminder in reminders), default=0) + 1


def normalize_modlog(raw: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(raw, dict):
        return {}
    normalized: dict[str, list[dict[str, Any]]] = {}
    for guild_id, entries in raw.items():
        if not isinstance(entries, list):
            continue
        clean_entries: list[dict[str, Any]] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            case_id = as_int(entry.get("case_id"))
            action = entry.get("action")
            actor_id = as_int(entry.get("actor_id"))
            target_id = as_int(entry.get("target_id"))
            created_at = parse_datetime_utc(entry.get("created_at"))
            if (
                case_id is None
                or actor_id is None
                or not isinstance(action, str)
                or created_at is None
            ):
                continue
            clean_entries.append(
                {
                    "case_id": case_id,
                    "action": action[:64],
                    "actor_id": actor_id,
                    "target_id": target_id,
                    "reason": str(entry.get("reason") or "")[:300],
                    "details": str(entry.get("details") or "")[:500],
                    "created_at": created_at.isoformat(),
                    "undoable": bool(entry.get("undoable", False)),
                    "undone": bool(entry.get("undone", False)),
                }
            )
        clean_entries.sort(key=lambda item: item["case_id"])
        normalized[str(guild_id)] = clean_entries[-MAX_CASES_PER_GUILD:]
    return normalized


def load_modlog() -> dict[str, list[dict[str, Any]]]:
    if not os.path.exists(MOD_LOG_FILE):
        return reset_json_file(MOD_LOG_FILE, {})
    try:
        with open(MOD_LOG_FILE, "r", encoding="utf-8") as file:
            return normalize_modlog(json.load(file))
    except (json.JSONDecodeError, OSError):
        return reset_json_file(MOD_LOG_FILE, {})


modlog = load_modlog()
message_rate_tracker: dict[int, deque[datetime]] = defaultdict(deque)
guard_last_trigger: dict[int, datetime] = {}


def save_reminders() -> None:
    reminders.sort(key=lambda reminder: reminder["due_at"])
    try:
        write_json_atomic(REMINDERS_FILE, reminders)
    except OSError as exc:
        print(f"[FILE] Failed saving reminders: {exc}")


def save_modlog() -> None:
    try:
        write_json_atomic(MOD_LOG_FILE, modlog)
    except OSError as exc:
        print(f"[FILE] Failed saving mod log: {exc}")


def get_next_case_id(guild_id: int) -> int:
    entries = modlog.get(str(guild_id), [])
    return max((entry.get("case_id", 0) for entry in entries), default=0) + 1


def log_moderation_action(
    guild_id: int,
    action: str,
    actor_id: int,
    target_id: int | None = None,
    reason: str | None = None,
    details: str | None = None,
    undoable: bool = False,
) -> int:
    case_id = get_next_case_id(guild_id)
    entries = modlog.setdefault(str(guild_id), [])
    entries.append(
        {
            "case_id": case_id,
            "action": action[:64],
            "actor_id": actor_id,
            "target_id": target_id,
            "reason": (reason or "")[:300],
            "details": (details or "")[:500],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "undoable": undoable,
            "undone": False,
        }
    )
    modlog[str(guild_id)] = entries[-MAX_CASES_PER_GUILD:]
    save_modlog()
    return case_id


def create_reminder_id() -> int:
    global next_reminder_id
    current_id = next_reminder_id
    next_reminder_id += 1
    return current_id


def parse_duration_to_seconds(duration: str) -> int | None:
    cleaned = duration.strip().lower().replace(" ", "")
    if not cleaned:
        return None
    if cleaned.isdigit():
        # Plain integer defaults to minutes.
        return int(cleaned) * 60

    matches = list(DURATION_TOKEN_RE.finditer(cleaned))
    if not matches:
        return None
    if "".join(match.group(0) for match in matches) != cleaned:
        return None

    unit_seconds = {
        "s": 1,
        "m": 60,
        "h": 3600,
        "d": 86400,
        "w": 604800,
    }
    total_seconds = 0
    for match in matches:
        amount = int(match.group(1))
        unit = match.group(2)
        total_seconds += amount * unit_seconds[unit]
    return total_seconds


def format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    units = [
        ("d", 86400),
        ("h", 3600),
        ("m", 60),
        ("s", 1),
    ]
    parts: list[str] = []
    remaining = seconds
    for label, unit_seconds in units:
        if remaining >= unit_seconds:
            amount, remaining = divmod(remaining, unit_seconds)
            parts.append(f"{amount}{label}")
    return " ".join(parts) if parts else "0s"


def extract_id(target: str) -> str | None:
    if not target:
        return None
    if target.isdigit():
        return target
    match = ID_RE.search(target)
    return match.group(1) if match else None


def get_bot_member(guild: discord.Guild) -> discord.Member | None:
    member = guild.me
    if member:
        return member
    if bot.user:
        return guild.get_member(bot.user.id)
    return None


def resolve_role(guild: discord.Guild, role_id: int | None) -> discord.Role | None:
    if not role_id:
        return None
    return guild.get_role(role_id)


def resolve_default_role(guild: discord.Guild) -> discord.Role | None:
    if guild.default_role is not None:
        return guild.default_role
    return guild.get_role(guild.id)


def ensure_text_channel(channel: ConfigChannelInput | None) -> ConfigChannelInput | None:
    if channel is None:
        return None
    if isinstance(channel, discord.TextChannel):
        return channel
    channel_type = getattr(channel, "type", None)
    channel_type_name = str(getattr(channel_type, "name", channel_type)).lower()
    channel_id = as_int(getattr(channel, "id", None))
    if channel_id is not None and channel_type_name in {"text", "news"}:
        return channel
    return None


def get_channel_id(channel: ConfigChannelInput | None) -> int | None:
    if channel is None:
        return None
    return as_int(getattr(channel, "id", None))


def format_channel_mention(channel: ConfigChannelInput | None) -> str:
    if channel is None:
        return "#unknown"
    mention = getattr(channel, "mention", None)
    if isinstance(mention, str) and mention:
        return mention
    channel_id = get_channel_id(channel)
    if channel_id is not None:
        return f"<#{channel_id}>"
    return "#unknown"


def is_channel_transform_error(error: Exception) -> bool:
    if not isinstance(error, app_commands.TransformerError):
        return False
    option_type = getattr(error, "type", None)
    if option_type is None:
        return False
    option_name = getattr(option_type, "name", None)
    if isinstance(option_name, str):
        return option_name.lower() == "channel"
    return str(option_type).lower().endswith("channel")


def has_elevated_permissions(perms: discord.Permissions | None) -> bool:
    if perms is None:
        return False
    return bool(
        perms.administrator
        or perms.manage_guild
        or perms.moderate_members
        or perms.manage_roles
    )


def has_mod_access(
    member: discord.Member,
    guild_cfg: dict[str, Any],
    interaction_perms: discord.Permissions | None = None,
) -> bool:
    if member.guild.owner_id == member.id:
        return True

    if has_elevated_permissions(interaction_perms):
        return True

    perms = member.guild_permissions
    if has_elevated_permissions(perms):
        return True

    mod_role_ids = set(guild_cfg.get("mod_role_ids", []))
    return any(role.id in mod_role_ids for role in member.roles)


def can_manage_target(actor: discord.Member, target: discord.Member) -> bool:
    if actor.guild.owner_id == actor.id:
        return True
    return actor.top_role > target.top_role


def render_welcome_message(guild_cfg: dict[str, Any], member: discord.Member) -> str:
    template = guild_cfg.get("welcome_message")
    if not isinstance(template, str) or not template.strip():
        return f"Welcome {member.mention} to **{member.guild.name}**."
    return (
        template.replace("{user}", member.mention)
        .replace("{server}", member.guild.name)
        .replace("{username}", member.display_name)
    )


def build_welcome_embed(member: discord.Member, guild_cfg: dict[str, Any]) -> discord.Embed:
    embed = discord.Embed(
        title="Welcome",
        description=render_welcome_message(guild_cfg, member),
        color=discord.Color.red(),
    )
    embed.add_field(
        name="Get Started",
        value="Read the server rules and introduce yourself.",
        inline=False,
    )
    embed.add_field(
        name="Tip",
        value="Use the right channel for the right topic.",
        inline=False,
    )
    embed.set_footer(text="Glad to have you here.")
    try:
        embed.set_thumbnail(url=member.display_avatar.url)
    except Exception:
        pass
    return embed


def resolve_welcome_channel(
    guild: discord.Guild,
    preferred_channel_id: int | None,
) -> discord.TextChannel | None:
    bot_member = get_bot_member(guild)
    if not bot_member:
        return None

    def can_send(channel: discord.abc.GuildChannel) -> bool:
        if not isinstance(channel, discord.TextChannel):
            return False
        perms = channel.permissions_for(bot_member)
        return perms.send_messages and perms.embed_links

    if preferred_channel_id:
        preferred = guild.get_channel(preferred_channel_id)
        if preferred and can_send(preferred):
            return preferred

    if guild.system_channel and can_send(guild.system_channel):
        return guild.system_channel

    for channel in guild.text_channels:
        if can_send(channel):
            return channel
    return None


async def send_ops_log(guild: discord.Guild, message: str) -> None:
    guild_cfg = get_guild_config(guild.id)
    channel_id = guild_cfg.get("log_channel_id")
    if not channel_id:
        return
    channel = guild.get_channel(channel_id)
    if channel and hasattr(channel, "send"):
        try:
            await channel.send(message)
        except Exception:
            pass


def count_guard_window(guild_id: int, window_seconds: int, now: datetime) -> int:
    tracker = message_rate_tracker[guild_id]
    cutoff = now - timedelta(seconds=window_seconds)
    while tracker and tracker[0] < cutoff:
        tracker.popleft()
    return len(tracker)


def should_trigger_guard(guild_id: int, now: datetime) -> bool:
    last = guard_last_trigger.get(guild_id)
    if not last:
        return True
    return (now - last).total_seconds() >= GUARD_COOLDOWN_SECONDS


def get_active_prefix(guild: discord.Guild | None) -> str:
    if guild is None:
        return BOT_PREFIX
    guild_cfg = get_guild_config(guild.id)
    guild_prefix = guild_cfg.get("prefix")
    if isinstance(guild_prefix, str) and guild_prefix:
        return guild_prefix
    return BOT_PREFIX


def build_backend_headers(include_license: bool = False) -> dict[str, str]:
    headers: dict[str, str] = {}
    if VANGUARD_BACKEND_API_KEY:
        headers[VANGUARD_BACKEND_KEY_HEADER] = VANGUARD_BACKEND_API_KEY
    if VANGUARD_INSTANCE_ID:
        headers[VANGUARD_INSTANCE_HEADER] = VANGUARD_INSTANCE_ID
    if include_license and VANGUARD_LICENSE_KEY:
        headers["Authorization"] = f"Bearer {VANGUARD_LICENSE_KEY}"
    return headers


def parse_allowed_guild_ids(value: Any) -> set[int]:
    if not isinstance(value, list):
        return set()
    parsed: set[int] = set()
    for item in value:
        try:
            guild_id = int(item)
        except (TypeError, ValueError):
            continue
        if guild_id > 0:
            parsed.add(guild_id)
    return parsed


def get_effective_allowed_guild_ids() -> set[int]:
    allowed = set(VANGUARD_ALLOWED_GUILD_IDS)
    allowed.update(license_allowed_guild_ids)
    return allowed


def is_guild_authorized(guild_id: int | None) -> bool:
    if guild_id is None:
        return True
    allowed = get_effective_allowed_guild_ids()
    if not allowed:
        return True
    return guild_id in allowed


def get_access_block_reason() -> str | None:
    if VANGUARD_REQUIRE_LICENSE and not license_authorized:
        return license_reason or "license verification failed"
    return None


def verify_license_sync(
    bot_user_id: int | None,
    guild_count: int,
) -> tuple[bool, str, set[int]]:
    if not VANGUARD_LICENSE_VERIFY_URL:
        if VANGUARD_REQUIRE_LICENSE:
            return False, "VANGUARD_LICENSE_VERIFY_URL is not configured.", set()
        return True, "license checks disabled", set()

    payload = {
        "instanceId": VANGUARD_INSTANCE_ID or platform.node() or "vanguard-instance",
        "botUserId": str(bot_user_id or ""),
        "guildCount": guild_count,
    }
    headers = build_backend_headers(include_license=True)

    response = requests.post(
        VANGUARD_LICENSE_VERIFY_URL,
        json=payload,
        headers=headers or None,
        timeout=8,
    )
    if response.status_code != 200:
        return False, f"license endpoint returned HTTP {response.status_code}", set()

    try:
        body = response.json()
    except ValueError:
        return False, "license endpoint returned non-JSON response", set()
    if not isinstance(body, dict):
        return False, "license endpoint returned malformed response", set()

    authorized = bool(body.get("authorized", False))
    reason = str(body.get("reason") or ("authorized" if authorized else "unauthorized"))
    allowed_ids = parse_allowed_guild_ids(
        body.get("allowedGuildIds", body.get("allowed_guild_ids"))
    )
    return authorized, reason[:240], allowed_ids


async def refresh_license_state() -> None:
    global license_authorized, license_reason, license_allowed_guild_ids, license_last_checked_at
    try:
        authorized, reason, allowed_ids = await asyncio.to_thread(
            verify_license_sync,
            bot.user.id if bot.user else None,
            len(bot.guilds),
        )
    except requests.exceptions.RequestException as exc:
        authorized, reason, allowed_ids = False, f"license request error: {exc}", set()
    except Exception as exc:
        authorized, reason, allowed_ids = False, f"license check error: {exc}", set()

    if VANGUARD_REQUIRE_LICENSE:
        license_authorized = authorized
    else:
        license_authorized = True

    license_reason = reason
    license_allowed_guild_ids = allowed_ids
    license_last_checked_at = datetime.now(timezone.utc)


async def enforce_guild_allowlist() -> None:
    for guild in list(bot.guilds):
        if is_guild_authorized(guild.id):
            continue
        try:
            await guild.leave()
            print(f"[ACCESS] Left unauthorized guild {guild.id} ({guild.name})")
        except Exception as exc:
            print(f"[ACCESS] Failed leaving unauthorized guild {guild.id}: {exc}")


async def license_worker() -> None:
    while not bot.is_closed():
        await asyncio.sleep(VANGUARD_LICENSE_RECHECK_SECONDS)
        try:
            await refresh_license_state()
            await enforce_guild_allowlist()
            reason = get_access_block_reason()
            if reason:
                print(f"[ACCESS] Commands blocked: {reason}")
        except Exception as exc:
            print(f"[ACCESS] License worker error: {exc}")


def disabled_message_prefix(_: commands.Bot, __: discord.Message) -> tuple[str, ...]:
    # Disable message-based command parsing; this bot is slash-command only.
    return ()


async def send_backend_user_update(
    ctx: commands.Context,
    target: str,
    endpoint: str,
    success_text: str,
) -> None:
    user_id = extract_id(target)
    if not user_id:
        await safe_ctx_send(ctx, "⚠️ Provide a mention (`@user`) or the raw numeric ID.")
        return

    payload = {"userId": user_id}
    backend_headers = build_backend_headers()
    try:
        response = await asyncio.to_thread(
            requests.post,
            endpoint,
            json=payload,
            headers=backend_headers or None,
            timeout=6,
        )
    except requests.exceptions.RequestException as exc:
        await safe_ctx_send(ctx, f"❌ Request error: `{exc}`.")
        return

    if response.status_code != 200:
        response_excerpt = clamp_text(response.text, 800)
        await safe_ctx_send(
            ctx,
            f"⚠️ Request failed for <@{user_id}>. "
            f"Status: {response.status_code}. Response: ```{response_excerpt}```"
        )
        return

    mention = f"<@{user_id}>"
    if ctx.guild:
        try:
            member = await ctx.guild.fetch_member(int(user_id))
            mention = member.mention
        except Exception:
            pass

    backend_msg = ""
    try:
        backend_msg = clamp_text(response.json().get("message", ""), 800)
    except Exception:
        backend_msg = clamp_text(response.text, 800)

    await safe_ctx_send(ctx, f"✅ {mention} {success_text}. `{backend_msg}`")


async def require_guild_context(
    ctx: commands.Context,
) -> tuple[discord.Guild, dict[str, Any]] | None:
    if ctx.guild is None:
        await safe_ctx_send(ctx, "⚠️ This command can only be used in a server.")
        return None
    return ctx.guild, get_guild_config(ctx.guild.id)


async def resolve_context_member(
    ctx: commands.Context,
    guild: discord.Guild,
) -> discord.Member | None:
    if isinstance(ctx.author, discord.Member):
        return ctx.author
    member = guild.get_member(ctx.author.id)
    if member:
        return member
    try:
        return await guild.fetch_member(ctx.author.id)
    except Exception:
        return None


async def require_mod_context(
    ctx: commands.Context,
) -> tuple[discord.Guild, dict[str, Any]] | None:
    result = await require_guild_context(ctx)
    if not result:
        return None

    guild, guild_cfg = result
    interaction_perms: discord.Permissions | None = None
    if ctx.interaction is not None:
        interaction_perms = ctx.interaction.permissions

    member = await resolve_context_member(ctx, guild)
    if member is None and has_elevated_permissions(interaction_perms):
        return guild, guild_cfg
    if member is None:
        await safe_ctx_send(ctx, "⚠️ Unable to verify your server membership. Try again in this server.")
        return None
    if not has_mod_access(member, guild_cfg, interaction_perms):
        await safe_ctx_send(
            ctx,
            "⛔ You do not have permission to run this command. "
            "Required: Server Owner, Administrator, Manage Server, Manage Roles, "
            "Moderate Members, or a configured mod role."
        )
        return None
    return guild, guild_cfg


async def set_lockdown_state(ctx: commands.Context, locked: bool) -> None:
    await safe_ctx_defer(ctx)

    result = await require_mod_context(ctx)
    if not result:
        return

    guild, guild_cfg = result
    target_role_id = guild_cfg.get("lockdown_role_id")
    target_role = resolve_role(guild, target_role_id) if target_role_id else resolve_default_role(guild)
    if target_role is None:
        await safe_ctx_send(ctx, "⚠️ Configured lockdown role no longer exists. Set it again with `/setlockdownrole`.")
        return

    updated = 0
    failed = 0
    for channel in guild.text_channels:
        overwrite = channel.overwrites_for(target_role)
        overwrite.send_messages = not locked
        try:
            await channel.set_permissions(target_role, overwrite=overwrite)
            updated += 1
        except Exception:
            failed += 1

    title = "EMERGENCY LOCKDOWN" if locked else "LOCKDOWN LIFTED"
    description = (
        "Member communication has been disabled for the configured role."
        if locked
        else "Members can communicate normally again."
    )
    color = discord.Color.red() if locked else discord.Color.green()
    embed = discord.Embed(title=title, description=description, color=color)
    embed.add_field(name="Channels updated", value=str(updated), inline=True)
    embed.add_field(name="Channels failed", value=str(failed), inline=True)
    embed.set_footer(text=f"Triggered by {ctx.author.display_name}")
    await safe_ctx_send(ctx, embed=embed)
    case_id = log_moderation_action(
        guild_id=guild.id,
        action="lockdown" if locked else "unlock",
        actor_id=ctx.author.id,
        reason=f"Changed communication state for role {target_role.name}",
        details=f"updated={updated},failed={failed},role_id={target_role.id}",
        undoable=False,
    )
    await send_ops_log(
        guild,
        f"📘 Case `{case_id}` {ctx.author.mention} ran `{ 'lockdown' if locked else 'unlock' }` "
        f"for role `{target_role.name}`.",
    )


async def dispatch_due_reminders() -> None:
    global reminders
    now = datetime.now(timezone.utc)
    due: list[dict[str, Any]] = []
    future: list[dict[str, Any]] = []

    for reminder in reminders:
        due_at = parse_datetime_utc(reminder.get("due_at"))
        if due_at is None or due_at <= now:
            due.append(reminder)
        else:
            future.append(reminder)

    if len(future) != len(reminders):
        reminders = future
        save_reminders()

    for reminder in due:
        user_id = reminder["user_id"]
        channel_id = reminder["channel_id"]
        body = reminder["message"]
        content = f"⏰ Reminder for <@{user_id}>: {body}"

        delivered = False
        channel = bot.get_channel(channel_id)
        if channel and hasattr(channel, "send"):
            try:
                await channel.send(content)
                delivered = True
            except Exception:
                delivered = False

        if delivered:
            continue

        user = bot.get_user(user_id)
        if user is None:
            try:
                user = await bot.fetch_user(user_id)
            except Exception:
                user = None
        if user:
            try:
                await user.send(content)
            except Exception:
                pass


async def send_chunked_message(ctx: commands.Context, text: str, chunk_size: int = MAX_DISCORD_MESSAGE_CHARS) -> None:
    payload = str(text)
    if len(payload) <= chunk_size:
        await safe_ctx_send(ctx, payload)
        return
    for index in range(0, len(payload), chunk_size):
        await safe_ctx_send(ctx, payload[index : index + chunk_size])


async def safe_ctx_defer(ctx: commands.Context, *, ephemeral: bool = False) -> bool:
    interaction = getattr(ctx, "interaction", None)
    if interaction is None or interaction.response.is_done():
        return False
    try:
        await interaction.response.defer(ephemeral=ephemeral)
        return True
    except (discord.NotFound, discord.HTTPException):
        return False


async def safe_ctx_send(ctx: commands.Context, *args: Any, **kwargs: Any):
    try:
        return await ctx.send(*args, **kwargs)
    except discord.NotFound:
        # Interaction responses can expire on long-running slash/hybrid commands.
        channel = getattr(ctx, "channel", None)
        if channel is None or not hasattr(channel, "send"):
            return None
        fallback_kwargs = dict(kwargs)
        fallback_kwargs.pop("ephemeral", None)
        try:
            return await channel.send(*args, **fallback_kwargs)
        except Exception:
            return None


async def reminder_worker() -> None:
    while not bot.is_closed():
        try:
            await dispatch_due_reminders()
        except Exception as exc:
            print(f"[REMINDER] Worker error: {exc}")
        await asyncio.sleep(REMINDER_CHECK_SECONDS)


intents = discord.Intents.default()
# Message content is still used by moderation guard logic.
intents.message_content = True
intents.guilds = True
intents.members = True

bot = commands.Bot(
    command_prefix=disabled_message_prefix,
    intents=intents,
    help_command=None,
    case_insensitive=True,
)
setup_vote_module(bot)

startup_initialized = False
reminder_loop_task: asyncio.Task | None = None
license_loop_task: asyncio.Task | None = None
license_authorized = not VANGUARD_REQUIRE_LICENSE
license_reason = "license checks disabled"
license_allowed_guild_ids: set[int] = set()
license_last_checked_at: datetime | None = None


@bot.check
async def global_owner_check(ctx: commands.Context) -> bool:
    if not settings.get("owner_only", False):
        return True
    return await bot.is_owner(ctx.author)


@bot.check
async def global_access_policy_check(ctx: commands.Context) -> bool:
    block_reason = get_access_block_reason()
    if block_reason:
        await safe_ctx_send(ctx, f"⛔ This Vanguard instance is not authorized: {block_reason}")
        return False
    if ctx.guild and not is_guild_authorized(ctx.guild.id):
        await safe_ctx_send(ctx, "⛔ This server is not authorized to use this Vanguard instance.")
        return False
    return True


@bot.event
async def on_ready():
    global startup_initialized, reminder_loop_task, license_loop_task
    if bot.user is not None:
        print(f"[READY] Logged in as {bot.user} ({bot.user.id}) in {len(bot.guilds)} guild(s)")
    else:
        print(f"[READY] Logged in. Guild count: {len(bot.guilds)}")

    if startup_initialized:
        return

    await refresh_license_state()
    block_reason = get_access_block_reason()
    if block_reason:
        print(f"[ACCESS] Commands blocked: {block_reason}")
    await enforce_guild_allowlist()

    try:
        await restore_vote_state(bot)
    except Exception as exc:
        print(f"[VOTE] Restore failed: {exc}")

    try:
        synced_global = await bot.tree.sync()
        print(f"[SYNC] Synced {len(synced_global)} global slash command(s)")
    except Exception as exc:
        print(f"[SYNC] Global slash command sync failed: {exc}")

    # Clear stale guild-scoped copies to avoid duplicate commands (global + guild).
    for guild in bot.guilds:
        try:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
            print(f"[SYNC] Guild {guild.id} cleared stale guild-scoped commands")
        except Exception as exc:
            print(f"[SYNC] Guild {guild.id} clear failed: {exc}")

    if reminder_loop_task is None or reminder_loop_task.done():
        reminder_loop_task = asyncio.create_task(reminder_worker())
    if (
        VANGUARD_LICENSE_VERIFY_URL
        or VANGUARD_REQUIRE_LICENSE
        or VANGUARD_ALLOWED_GUILD_IDS
    ) and (license_loop_task is None or license_loop_task.done()):
        license_loop_task = asyncio.create_task(license_worker())

    startup_initialized = True


@bot.event
async def on_guild_join(guild: discord.Guild):
    if is_guild_authorized(guild.id):
        return
    try:
        await guild.leave()
        print(f"[ACCESS] Left unauthorized guild on join: {guild.id} ({guild.name})")
    except Exception as exc:
        print(f"[ACCESS] Failed leaving unauthorized guild on join {guild.id}: {exc}")


@bot.event
async def on_member_join(member: discord.Member):
    guild_cfg = get_guild_config(member.guild.id)
    channel = resolve_welcome_channel(member.guild, guild_cfg.get("welcome_channel_id"))
    if channel is None:
        return

    try:
        await channel.send(embed=build_welcome_embed(member, guild_cfg))
    except Exception:
        return

    role = resolve_role(member.guild, guild_cfg.get("welcome_role_id"))
    if role:
        try:
            await member.add_roles(role, reason="Auto-assign welcome role")
        except Exception:
            pass

    mentions = []
    for role_id in guild_cfg.get("mod_role_ids", []):
        mod_role = resolve_role(member.guild, role_id)
        if mod_role:
            mentions.append(mod_role.mention)
    if mentions:
        await channel.send(f"New member joined: {member.mention}. Heads up {', '.join(mentions)}.")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if message.guild and not is_guild_authorized(message.guild.id):
        return

    if isinstance(message.author, discord.Member) and message.guild:
        guild = message.guild
        guild_cfg = get_guild_config(guild.id)
        if guild_cfg.get("guard_enabled", False):
            now = datetime.now(timezone.utc)
            new_account_hours = guild_cfg.get("guard_new_account_hours", 24)
            account_age = now - message.author.created_at.astimezone(timezone.utc)
            if account_age <= timedelta(hours=new_account_hours):
                message_rate_tracker[guild.id].append(now)
                window_seconds = guild_cfg.get("guard_window_seconds", 30)
                threshold = guild_cfg.get("guard_threshold", 8)
                current_rate = count_guard_window(guild.id, window_seconds, now)
                if current_rate >= threshold and should_trigger_guard(guild.id, now):
                    guard_last_trigger[guild.id] = now
                    details = (
                        f"Detected {current_rate} messages from new accounts in "
                        f"{window_seconds}s window."
                    )
                    alert_channel = message.channel
                    configured_alert = guild.get_channel(guild_cfg.get("ops_channel_id"))
                    if configured_alert and hasattr(configured_alert, "send"):
                        alert_channel = configured_alert

                    slowmode_seconds = guild_cfg.get("guard_slowmode_seconds", 30)
                    if (
                        isinstance(message.channel, discord.TextChannel)
                        and slowmode_seconds >= 0
                    ):
                        try:
                            await message.channel.edit(slowmode_delay=slowmode_seconds)
                        except Exception:
                            pass

                    alert_text = (
                        "🚨 **Guard Triggered**\n"
                        f"{details}\n"
                        f"Applied slowmode: `{slowmode_seconds}s` in {message.channel.mention}."
                    )
                    try:
                        if hasattr(alert_channel, "send"):
                            await alert_channel.send(alert_text)
                    except Exception:
                        pass

                    case_id = log_moderation_action(
                        guild_id=guild.id,
                        action="guard_trigger",
                        actor_id=bot.user.id if bot.user else 0,
                        reason="Automated guard defense",
                        details=details,
                        undoable=False,
                    )
                    await send_ops_log(
                        guild,
                        f"🛡️ Case `{case_id}` guard trigger in {message.channel.mention}: {details}",
                    )

    # Slash commands are handled via interactions; ignore message command parsing.
    return


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    if hasattr(ctx.command, "on_error"):
        return

    err = getattr(error, "original", error)

    if isinstance(err, commands.CommandNotFound):
        return
    if isinstance(err, commands.NotOwner):
        await safe_ctx_send(ctx, "⛔ This command is owner-only.")
        return
    if isinstance(err, commands.MissingPermissions):
        missing = ", ".join(err.missing_permissions)
        await safe_ctx_send(ctx, f"⛔ Missing required permissions: `{missing}`.")
        return
    if isinstance(err, commands.BotMissingPermissions):
        missing = ", ".join(err.missing_permissions)
        await safe_ctx_send(ctx, f"⛔ I am missing permissions: `{missing}`.")
        return
    if isinstance(err, commands.CheckFailure):
        await safe_ctx_send(ctx, "⛔ You do not have permission to run this command.")
        return
    if isinstance(err, commands.CommandOnCooldown):
        await safe_ctx_send(ctx, f"⏳ Slow down. Try again in `{err.retry_after:.1f}` seconds.")
        return
    if isinstance(err, app_commands.TransformerError):
        if is_channel_transform_error(err):
            await safe_ctx_send(ctx, "⚠️ Invalid channel type for that option. Choose a normal text channel.")
        else:
            await safe_ctx_send(ctx, "⚠️ Invalid argument. Check the command format and try again.")
        return
    if isinstance(err, commands.MissingRequiredArgument):
        usage = f"{ctx.clean_prefix}{ctx.command.qualified_name} {ctx.command.signature}"
        await safe_ctx_send(ctx, f"⚠️ Missing argument `{err.param.name}`.\nUsage: `{usage}`")
        return
    if isinstance(
        err,
        (
            commands.BadArgument,
            commands.BadUnionArgument,
            commands.MemberNotFound,
            commands.RoleNotFound,
            commands.ChannelNotFound,
        ),
    ):
        await safe_ctx_send(ctx, "⚠️ Invalid argument. Check the command format and try again.")
        return

    print("[ERROR] Unhandled command error:")
    traceback.print_exception(type(err), err, err.__traceback__)
    await safe_ctx_send(ctx, "❌ Unexpected error while running that command.")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    err = getattr(error, "original", error)
    message = "❌ Unexpected error while running that command."

    if isinstance(error, app_commands.MissingPermissions):
        missing = ", ".join(error.missing_permissions)
        message = f"⛔ Missing required permissions: `{missing}`."
    elif isinstance(error, app_commands.BotMissingPermissions):
        missing = ", ".join(error.missing_permissions)
        message = f"⛔ I am missing permissions: `{missing}`."
    elif isinstance(error, app_commands.CommandOnCooldown):
        message = f"⏳ Slow down. Try again in `{error.retry_after:.1f}` seconds."
    elif isinstance(error, app_commands.CheckFailure):
        message = "⛔ You do not have permission to run this command."
    elif isinstance(error, app_commands.TransformerError):
        if is_channel_transform_error(error):
            message = "⚠️ Invalid channel type for that option. Choose a normal text channel."
        else:
            message = "⚠️ Invalid argument. Check the command format and try again."
    elif isinstance(err, commands.NotOwner):
        message = "⛔ This command is owner-only."

    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.NotFound:
        channel = interaction.channel
        if channel and hasattr(channel, "send"):
            try:
                await channel.send(message)
            except Exception:
                pass
    except Exception:
        print("[ERROR] Unhandled app command error:")
        traceback.print_exception(type(error), error, error.__traceback__)


@bot.hybrid_command(name="help")
async def help_command(ctx: commands.Context, *, command_name: str | None = None):
    """Show help for all commands or a specific command."""
    if command_name:
        command = bot.get_command(command_name.lower())
        if command is None or command.hidden:
            await ctx.send("❌ Command not found.")
            return

        usage = f"/{command.qualified_name} {command.signature}".strip()
        embed = discord.Embed(title=f"Help: {command.qualified_name}", color=discord.Color.red())
        embed.add_field(name="Description", value=command.help or "No description.", inline=False)
        embed.add_field(name="Usage", value=f"`{usage}`", inline=False)
        if command.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{alias}`" for alias in command.aliases), inline=False)
        await ctx.send(embed=embed)
        return

    embed = discord.Embed(title="Vanguard Bot Help", color=discord.Color.red())
    embed.description = "Slash commands only. Use `/help <command>` for detailed help."
    embed.add_field(
        name="General",
        value=(
            "`help` `ping` `uptime` `botstats` `serverinfo` `userinfo` `avatar` "
            "`voteinfo` `activevotes` `myvote` `voteconfig` `ops` `health`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Community",
        value=(
            "`rules` `mcstatus` `poll` `choose` `roll` `remindme` `reminders` "
            "`cancelreminder` `startvote` `votecreate` `startelection` `voteextend` `voteclose`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Moderation",
        value=(
            "`lockdown` `unlock` `purge` `slowmode` `nick` `timeout` `untimeout` "
            "`guard` `warn` `cases` `undo`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Configuration",
        value=(
            "`showconfig` `setwelcomechannel` `setwelcomerole` "
            "`setwelcomemessage` `setlockdownrole` `setmodroles` `setmcserver` `clearmcserver` "
            "`setup` `setlogchannel` `setopschannel`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Legal",
        value="`privacy` `tos` `data`",
        inline=False,
    )
    embed.add_field(
        name="Owner",
        value="`owneronly` `flaguser` `unflaguser`",
        inline=False,
    )
    await ctx.send(embed=embed)


@bot.hybrid_command(name="setup")
async def setup_command(
    ctx: commands.Context,
    mod_role: discord.Role | None = None,
    welcome_channel: ConfigChannelInput | None = None,
    welcome_role: discord.Role | None = None,
    log_channel: ConfigChannelInput | None = None,
    ops_channel: ConfigChannelInput | None = None,
):
    """Quick server setup for Vanguard baseline configuration."""
    result = await require_mod_context(ctx)
    if not result:
        return
    guild, guild_cfg = result

    if mod_role:
        guild_cfg["mod_role_ids"] = sorted({mod_role.id})
    normalized_welcome_channel = ensure_text_channel(welcome_channel)
    normalized_log_channel = ensure_text_channel(log_channel)
    normalized_ops_channel = ensure_text_channel(ops_channel)

    if welcome_channel is not None and normalized_welcome_channel is None:
        await ctx.send("⚠️ `welcome_channel` must be a text channel.")
        return
    if log_channel is not None and normalized_log_channel is None:
        await ctx.send("⚠️ `log_channel` must be a text channel.")
        return
    if ops_channel is not None and normalized_ops_channel is None:
        await ctx.send("⚠️ `ops_channel` must be a text channel.")
        return

    welcome_channel_id_input = get_channel_id(normalized_welcome_channel)
    log_channel_id_input = get_channel_id(normalized_log_channel)
    ops_channel_id_input = get_channel_id(normalized_ops_channel)

    if welcome_channel is not None and welcome_channel_id_input is None:
        await ctx.send("⚠️ Could not resolve `welcome_channel` ID.")
        return
    if log_channel is not None and log_channel_id_input is None:
        await ctx.send("⚠️ Could not resolve `log_channel` ID.")
        return
    if ops_channel is not None and ops_channel_id_input is None:
        await ctx.send("⚠️ Could not resolve `ops_channel` ID.")
        return

    if normalized_welcome_channel:
        guild_cfg["welcome_channel_id"] = welcome_channel_id_input
    if welcome_role:
        guild_cfg["welcome_role_id"] = welcome_role.id
    if normalized_log_channel:
        guild_cfg["log_channel_id"] = log_channel_id_input
    if normalized_ops_channel:
        guild_cfg["ops_channel_id"] = ops_channel_id_input
    if guild_cfg.get("lockdown_role_id") is None:
        default_role = resolve_default_role(guild)
        if default_role is not None:
            guild_cfg["lockdown_role_id"] = default_role.id

    save_settings()
    mod_roles_text = ", ".join(f"<@&{rid}>" for rid in guild_cfg.get("mod_role_ids", [])) or "not set"
    welcome_channel_id = guild_cfg.get("welcome_channel_id")
    log_channel_id = guild_cfg.get("log_channel_id")
    ops_channel_id = guild_cfg.get("ops_channel_id")
    await ctx.send(
        "✅ Setup complete.\n"
        "- Commands: slash-only (`/`)\n"
        f"- Mod roles: {mod_roles_text}\n"
        f"- Welcome channel: {f'<#{welcome_channel_id}>' if welcome_channel_id else 'fallback mode'}\n"
        f"- Log channel: {f'<#{log_channel_id}>' if log_channel_id else 'not set'}\n"
        f"- Ops channel: {f'<#{ops_channel_id}>' if ops_channel_id else 'not set'}"
    )


@bot.hybrid_command(name="setlogchannel")
async def setlogchannel(ctx: commands.Context, channel: ConfigChannelInput | None = None):
    """Set moderation log channel. Omit to clear."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    normalized_channel = ensure_text_channel(channel)
    if channel is not None and normalized_channel is None:
        await ctx.send("⚠️ `channel` must be a text channel.")
        return
    channel_id = get_channel_id(normalized_channel)
    if channel is not None and channel_id is None:
        await ctx.send("⚠️ Could not resolve channel ID.")
        return
    guild_cfg["log_channel_id"] = channel_id if normalized_channel else None
    save_settings()
    await ctx.send(
        f"✅ Log channel set to {format_channel_mention(normalized_channel)}."
        if normalized_channel
        else "✅ Log channel cleared."
    )


@bot.hybrid_command(name="setopschannel")
async def setopschannel(ctx: commands.Context, channel: ConfigChannelInput | None = None):
    """Set ops/alerts channel. Omit to clear."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    normalized_channel = ensure_text_channel(channel)
    if channel is not None and normalized_channel is None:
        await ctx.send("⚠️ `channel` must be a text channel.")
        return
    channel_id = get_channel_id(normalized_channel)
    if channel is not None and channel_id is None:
        await ctx.send("⚠️ Could not resolve channel ID.")
        return
    guild_cfg["ops_channel_id"] = channel_id if normalized_channel else None
    save_settings()
    await ctx.send(
        f"✅ Ops channel set to {format_channel_mention(normalized_channel)}."
        if normalized_channel
        else "✅ Ops channel cleared."
    )


@bot.hybrid_command(name="guard")
async def guard(
    ctx: commands.Context,
    enabled: bool | None = None,
    threshold: int | None = None,
    window_seconds: int | None = None,
    slowmode_seconds: int | None = None,
    new_account_hours: int | None = None,
):
    """Configure anti-raid guard thresholds."""
    result = await require_mod_context(ctx)
    if not result:
        return
    guild, guild_cfg = result

    if enabled is not None:
        guild_cfg["guard_enabled"] = enabled
    if threshold is not None:
        guild_cfg["guard_threshold"] = max(3, min(100, threshold))
    if window_seconds is not None:
        guild_cfg["guard_window_seconds"] = max(5, min(300, window_seconds))
    if slowmode_seconds is not None:
        guild_cfg["guard_slowmode_seconds"] = max(0, min(21600, slowmode_seconds))
    if new_account_hours is not None:
        guild_cfg["guard_new_account_hours"] = max(1, min(168, new_account_hours))

    save_settings()
    await ctx.send(
        f"🛡️ Guard for **{guild.name}**\n"
        f"- Enabled: `{guild_cfg.get('guard_enabled', False)}`\n"
        f"- Threshold: `{guild_cfg.get('guard_threshold', 8)}`\n"
        f"- Window: `{guild_cfg.get('guard_window_seconds', 30)}s`\n"
        f"- New account age: `{guild_cfg.get('guard_new_account_hours', 24)}h`\n"
        f"- Auto slowmode: `{guild_cfg.get('guard_slowmode_seconds', 30)}s`"
    )


@bot.hybrid_command(name="ops")
async def ops(ctx: commands.Context):
    """Operational intelligence summary for this server."""
    result = await require_guild_context(ctx)
    if not result:
        return
    guild, guild_cfg = result
    now = datetime.now(timezone.utc)
    active_votes = sum(1 for vote_id in vote_store if str(vote_id).startswith(f"{guild.id}-"))
    pending_reminders = sum(
        1
        for reminder in reminders
        if reminder.get("guild_id") == guild.id and (parse_datetime_utc(reminder.get("due_at")) or now) > now
    )

    case_entries = modlog.get(str(guild.id), [])
    recent_window = now - timedelta(hours=24)
    recent_cases = [
        case for case in case_entries if (parse_datetime_utc(case.get("created_at")) or now) >= recent_window
    ]
    actions_count: dict[str, int] = defaultdict(int)
    for case in recent_cases:
        actions_count[str(case.get("action", "unknown"))] += 1
    top_actions = sorted(actions_count.items(), key=lambda item: item[1], reverse=True)[:3]

    embed = discord.Embed(title=f"Ops Report: {guild.name}", color=discord.Color.red())
    embed.add_field(name="Members", value=str(guild.member_count), inline=True)
    embed.add_field(name="Active Votes", value=str(active_votes), inline=True)
    embed.add_field(name="Pending Reminders", value=str(pending_reminders), inline=True)
    embed.add_field(name="Mod Cases (24h)", value=str(len(recent_cases)), inline=True)
    embed.add_field(name="Guard", value="ON" if guild_cfg.get("guard_enabled") else "OFF", inline=True)
    embed.add_field(
        name="Top Actions (24h)",
        value=", ".join(f"{action}:{count}" for action, count in top_actions) if top_actions else "No recent actions",
        inline=False,
    )
    embed.set_footer(text=f"Generated at {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    await ctx.send(embed=embed)

    ops_channel_id = guild_cfg.get("ops_channel_id")
    if ops_channel_id and ctx.channel.id != ops_channel_id:
        ops_channel = guild.get_channel(ops_channel_id)
        if ops_channel and hasattr(ops_channel, "send"):
            try:
                await ops_channel.send(embed=embed)
            except Exception:
                pass


@bot.hybrid_command(name="health")
async def health(ctx: commands.Context):
    """Runtime health and dependency checks."""
    checks: list[tuple[str, str]] = []
    checks.append(("Discord", "OK"))
    checks.append(("Latency", f"{round(bot.latency * 1000)}ms"))
    checks.append(("Settings File", "OK" if os.path.exists(SETTINGS_FILE) else "MISSING"))
    checks.append(("Reminders File", "OK" if os.path.exists(REMINDERS_FILE) else "MISSING"))
    checks.append(("Mod Log File", "OK" if os.path.exists(MOD_LOG_FILE) else "MISSING"))

    ai_status = "DISABLED"
    model_status = "N/A"
    backend_headers = build_backend_headers()
    if AI_HEALTH_URL:
        try:
            response = await asyncio.to_thread(
                requests.get,
                AI_HEALTH_URL,
                headers=backend_headers or None,
                timeout=4,
            )
            if response.status_code == 200:
                ai_status = "OK"
                try:
                    health_payload = response.json()
                except ValueError:
                    health_payload = {}
                if isinstance(health_payload, dict):
                    ollama_status = str(health_payload.get("ollama") or "").strip()
                    if ollama_status:
                        ai_status = f"OK ({ollama_status})"
            else:
                ai_status = f"HTTP {response.status_code}"
        except Exception:
            ai_status = "UNREACHABLE"

        try:
            model_response = await asyncio.to_thread(
                requests.get,
                AI_MODELS_URL,
                headers=backend_headers or None,
                timeout=4,
            )
            if model_response.status_code == 200:
                try:
                    model_payload = model_response.json()
                except ValueError:
                    model_payload = {}
                model_count = _extract_model_count(model_payload)
                model_status = str(model_count) if model_count is not None else "UNKNOWN"
            else:
                model_status = f"HTTP {model_response.status_code}"
        except Exception:
            model_status = "UNREACHABLE"

    checks.append(("AI Backend", ai_status))
    checks.append(("AI Models", model_status))
    if VANGUARD_REQUIRE_LICENSE:
        license_status = "OK" if license_authorized else f"BLOCKED ({license_reason})"
    elif VANGUARD_LICENSE_VERIFY_URL:
        license_status = f"MONITOR ({license_reason})"
    else:
        license_status = "DISABLED"
    allowlist = get_effective_allowed_guild_ids()
    allowlist_status = f"{len(allowlist)} guild(s)" if allowlist else "OFF"
    checks.append(("License Gate", license_status))
    checks.append(("Guild Allowlist", allowlist_status))

    embed = discord.Embed(title="Vanguard Health", color=discord.Color.red())
    for key, value in checks:
        embed.add_field(name=key, value=value, inline=True)
    embed.set_footer(text=f"Uptime: {format_duration(int((datetime.now(timezone.utc) - START_TIME).total_seconds()))}")
    await ctx.send(embed=embed)


@bot.hybrid_command(name="privacy")
async def privacy(ctx: commands.Context):
    """Show Privacy Policy link."""
    if PRIVACY_URL:
        await ctx.send(f"Privacy Policy: {PRIVACY_URL}")
    else:
        await ctx.send("Privacy policy link not configured. Set `PRIVACY_POLICY_URL` in `.env`.")


@bot.hybrid_command(name="tos")
async def tos(ctx: commands.Context):
    """Show Terms of Service link."""
    if TOS_URL:
        await ctx.send(f"Terms of Service: {TOS_URL}")
    else:
        await ctx.send("ToS link not configured. Set `TERMS_OF_SERVICE_URL` in `.env`.")


@bot.hybrid_command(name="data")
async def data(ctx: commands.Context):
    """Explain what data the bot stores and how to request deletion."""
    await ctx.send(
        "I store server config, moderation cases, reminders, and vote state to operate features. "
        "Use `/privacy` and `/tos` for full policy links."
    )

@bot.hybrid_command()
async def ping(ctx: commands.Context):
    """Show bot latency."""
    await ctx.send(f"🏓 Pong: `{round(bot.latency * 1000)}ms`")


@bot.hybrid_command()
async def uptime(ctx: commands.Context):
    """Show bot uptime."""
    elapsed = int((datetime.now(timezone.utc) - START_TIME).total_seconds())
    await ctx.send(f"⏱️ Uptime: `{format_duration(elapsed)}`")


@bot.hybrid_command()
async def botstats(ctx: commands.Context):
    """Show overall bot stats."""
    unique_user_count = len({member.id for guild in bot.guilds for member in guild.members})
    embed = discord.Embed(title="Bot Stats", color=discord.Color.red())
    embed.add_field(name="Guilds", value=str(len(bot.guilds)), inline=True)
    embed.add_field(name="Unique Users", value=str(unique_user_count), inline=True)
    embed.add_field(name="Latency", value=f"{round(bot.latency * 1000)}ms", inline=True)
    embed.add_field(name="Uptime", value=format_duration(int((datetime.now(timezone.utc) - START_TIME).total_seconds())), inline=False)
    await ctx.send(embed=embed)


@bot.hybrid_command()
async def avatar(ctx: commands.Context, member: discord.Member | None = None):
    """Show a user's avatar."""
    target = member or ctx.author
    if not isinstance(target, (discord.Member, discord.User)):
        await ctx.send("❌ Could not resolve user.")
        return
    embed = discord.Embed(title=f"Avatar: {target.display_name if isinstance(target, discord.Member) else target.name}", color=discord.Color.red())
    embed.set_image(url=target.display_avatar.url)
    await ctx.send(embed=embed)


@bot.hybrid_command()
async def userinfo(ctx: commands.Context, member: discord.Member | None = None):
    """Show information about a server member."""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    target = member or ctx.author
    if not isinstance(target, discord.Member):
        await ctx.send("❌ Could not resolve member.")
        return
    roles = [role.mention for role in target.roles if role != ctx.guild.default_role]
    embed = discord.Embed(title=f"User Info: {target}", color=discord.Color.red())
    embed.add_field(name="User ID", value=str(target.id), inline=True)
    embed.add_field(name="Joined Server", value=target.joined_at.strftime("%Y-%m-%d %H:%M:%S") if target.joined_at else "Unknown", inline=True)
    embed.add_field(name="Account Created", value=target.created_at.strftime("%Y-%m-%d %H:%M:%S"), inline=True)
    embed.add_field(name="Top Role", value=target.top_role.mention if target.top_role else "None", inline=True)
    embed.add_field(name="Roles", value=", ".join(roles[-10:]) if roles else "None", inline=False)
    embed.set_thumbnail(url=target.display_avatar.url)
    await ctx.send(embed=embed)


@bot.hybrid_command()
async def choose(ctx: commands.Context, *, options: str):
    """Choose randomly from options separated by |."""
    choices = [choice.strip() for choice in options.split("|") if choice.strip()]
    if len(choices) < 2:
        await ctx.send("⚠️ Provide at least 2 options separated by `|`.")
        return
    await ctx.send(f"🎲 I choose: **{random.choice(choices)}**")


@bot.hybrid_command()
async def roll(ctx: commands.Context, notation: str = "1d6"):
    """Roll dice using NdS notation, e.g. 2d20."""
    match = re.fullmatch(r"(\d{1,2})d(\d{1,4})", notation.lower().strip())
    if not match:
        await ctx.send("⚠️ Invalid format. Use `NdS`, for example `2d6` or `1d20`.")
        return
    count = int(match.group(1))
    sides = int(match.group(2))
    if count < 1 or count > 20 or sides < 2 or sides > 1000:
        await ctx.send("⚠️ Dice limits: `1-20` dice and `2-1000` sides.")
        return
    rolls = [random.randint(1, sides) for _ in range(count)]
    await ctx.send(f"🎲 Rolled `{notation}`: {', '.join(map(str, rolls))} (total: **{sum(rolls)}**)")


@bot.hybrid_command()
async def poll(ctx: commands.Context, *, content: str):
    """Create a poll. Format: question | option1 | option2 ... (or yes/no with question only)."""
    parts = [part.strip() for part in content.split("|") if part.strip()]
    if not parts:
        await ctx.send("⚠️ Provide a poll question.")
        return

    if len(parts) == 1:
        question = parts[0]
        embed = discord.Embed(title="Poll", description=question, color=discord.Color.red())
        embed.set_footer(text=f"Poll by {ctx.author.display_name}")
        message = await ctx.send(embed=embed)
        await message.add_reaction("👍")
        await message.add_reaction("👎")
        return

    question = parts[0]
    options = parts[1:]
    if len(options) < 2 or len(options) > 10:
        await ctx.send("⚠️ Polls with options must have between 2 and 10 options.")
        return

    number_emojis = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
    description = "\n".join(f"{number_emojis[i]} {option}" for i, option in enumerate(options))
    embed = discord.Embed(title="Poll", description=f"**{question}**\n\n{description}", color=discord.Color.red())
    embed.set_footer(text=f"Poll by {ctx.author.display_name}")
    message = await ctx.send(embed=embed)
    for i in range(len(options)):
        await message.add_reaction(number_emojis[i])


@bot.hybrid_command()
async def remindme(ctx: commands.Context, duration: str, *, message: str):
    """Create a reminder. Example: /remindme 2h30m stretch."""
    seconds = parse_duration_to_seconds(duration)
    if seconds is None or seconds <= 0:
        await ctx.send("⚠️ Invalid duration. Example: `10m`, `2h30m`, `1d`.")
        return
    if seconds > MAX_REMINDER_SECONDS:
        await ctx.send("⚠️ Maximum reminder duration is 30 days.")
        return

    clean_message = message.strip()
    if not clean_message:
        await ctx.send("⚠️ Reminder message cannot be empty.")
        return

    reminder_id = create_reminder_id()
    due_at = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    reminders.append(
        {
            "id": reminder_id,
            "user_id": ctx.author.id,
            "channel_id": ctx.channel.id,
            "guild_id": ctx.guild.id if ctx.guild else None,
            "message": clean_message[:300],
            "due_at": due_at.isoformat(),
        }
    )
    save_reminders()
    await ctx.send(
        f"✅ Reminder `{reminder_id}` set for `{format_duration(seconds)}` from now "
        f"(<t:{int(due_at.timestamp())}:R>)."
    )


@bot.hybrid_command(name="reminders")
async def list_reminders(ctx: commands.Context):
    """List your active reminders."""
    mine = [reminder for reminder in reminders if reminder["user_id"] == ctx.author.id]
    if not mine:
        await ctx.send("You have no active reminders.")
        return

    lines = []
    now = datetime.now(timezone.utc)
    for reminder in sorted(mine, key=lambda item: item["due_at"])[:10]:
        due_at = parse_datetime_utc(reminder["due_at"]) or now
        remaining_seconds = int((due_at - now).total_seconds())
        lines.append(
            f"`{reminder['id']}` • in `{format_duration(remaining_seconds)}` • {reminder['message']}"
        )
    await send_chunked_message(ctx, "**Your reminders:**\n" + "\n".join(lines))


@bot.hybrid_command(name="cancelreminder", aliases=["delreminder"])
async def cancel_reminder(ctx: commands.Context, reminder_id: int):
    """Cancel one of your reminders by ID."""
    global reminders
    before_count = len(reminders)
    reminders = [
        reminder
        for reminder in reminders
        if not (reminder["id"] == reminder_id and reminder["user_id"] == ctx.author.id)
    ]
    if len(reminders) == before_count:
        await ctx.send("❌ Reminder not found.")
        return
    save_reminders()
    await ctx.send(f"✅ Reminder `{reminder_id}` canceled.")


@bot.hybrid_command()
@commands.has_permissions(manage_messages=True)
async def purge(ctx: commands.Context, amount: int):
    """Delete recent messages in the current channel."""
    if ctx.guild is None or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("⚠️ This command can only be used in a text channel.")
        return
    if amount < 1 or amount > 200:
        await ctx.send("⚠️ Amount must be between 1 and 200.")
        return

    include_invocation = ctx.interaction is None
    purge_limit = amount + (1 if include_invocation else 0)
    try:
        deleted = await ctx.channel.purge(limit=purge_limit)
    except discord.Forbidden:
        await ctx.send("⛔ I do not have permission to delete messages in this channel.")
        return
    except discord.HTTPException as exc:
        await ctx.send(f"❌ Failed to purge messages: {exc}")
        return

    deleted_count = max(len(deleted) - (1 if include_invocation else 0), 0)
    case_id = log_moderation_action(
        guild_id=ctx.guild.id,
        action="purge",
        actor_id=ctx.author.id,
        reason=f"Purged {deleted_count} messages",
        details=f"channel_id={ctx.channel.id}",
        undoable=False,
    )
    await send_ops_log(
        ctx.guild,
        f"📘 Case `{case_id}` {ctx.author.mention} purged `{deleted_count}` messages in {ctx.channel.mention}.",
    )
    confirmation = await ctx.send(f"🧹 Deleted `{deleted_count}` messages.")
    await asyncio.sleep(4)
    try:
        await confirmation.delete()
    except Exception:
        pass


@bot.hybrid_command()
@commands.has_permissions(manage_channels=True)
async def slowmode(ctx: commands.Context, seconds: int):
    """Set channel slowmode in seconds (0 disables)."""
    if ctx.guild is None or not isinstance(ctx.channel, discord.TextChannel):
        await ctx.send("⚠️ This command can only be used in a text channel.")
        return
    if seconds < 0 or seconds > 21600:
        await ctx.send("⚠️ Slowmode must be between 0 and 21600 seconds.")
        return
    old_slowmode = ctx.channel.slowmode_delay
    await ctx.channel.edit(slowmode_delay=seconds)
    case_id = log_moderation_action(
        guild_id=ctx.guild.id,
        action="slowmode",
        actor_id=ctx.author.id,
        reason=f"Set slowmode to {seconds}s",
        details=json.dumps(
            {"channel_id": ctx.channel.id, "old_slowmode": old_slowmode, "new_slowmode": seconds}
        ),
        undoable=True,
    )
    await send_ops_log(
        ctx.guild,
        f"📘 Case `{case_id}` {ctx.author.mention} set slowmode in {ctx.channel.mention} from `{old_slowmode}` to `{seconds}`.",
    )
    await ctx.send(f"✅ Slowmode set to `{seconds}` second(s).")


@bot.hybrid_command()
@commands.has_permissions(manage_nicknames=True)
async def nick(ctx: commands.Context, member: discord.Member, *, nickname: str | None = None):
    """Change or clear a member nickname."""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    actor = await resolve_context_member(ctx, ctx.guild)
    if actor is None:
        await ctx.send("⚠️ Unable to verify your server membership. Try again in this server.")
        return
    bot_member = get_bot_member(ctx.guild)
    if bot_member is None or bot_member.top_role <= member.top_role:
        await ctx.send("⛔ I cannot edit that member's nickname.")
        return
    if not can_manage_target(actor, member):
        await ctx.send("⛔ You cannot edit that member's nickname.")
        return
    old_nickname = member.nick
    try:
        await member.edit(nick=nickname[:32] if nickname else None, reason=f"Requested by {actor}")
    except discord.Forbidden:
        await ctx.send("⛔ I cannot edit that member's nickname.")
        return
    except discord.HTTPException as exc:
        await ctx.send(f"❌ Failed to update nickname: {exc}")
        return
    case_id = log_moderation_action(
        guild_id=ctx.guild.id,
        action="nick",
        actor_id=ctx.author.id,
        target_id=member.id,
        reason="Nickname updated",
        details=json.dumps({"old_nick": old_nickname, "new_nick": nickname[:32] if nickname else None}),
        undoable=True,
    )
    await send_ops_log(
        ctx.guild,
        f"📘 Case `{case_id}` {ctx.author.mention} changed nickname for {member.mention}.",
    )
    if nickname:
        await ctx.send(f"✅ Updated nickname for {member.mention} to `{nickname[:32]}`.")
    else:
        await ctx.send(f"✅ Cleared nickname for {member.mention}.")


@bot.hybrid_command()
@commands.has_permissions(moderate_members=True)
async def timeout(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str | None = None):
    """Timeout a member. Example: /timeout @user 30m spam"""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    actor = await resolve_context_member(ctx, ctx.guild)
    if actor is None:
        await ctx.send("⚠️ Unable to verify your server membership. Try again in this server.")
        return
    if member.id == actor.id:
        await ctx.send("⚠️ You cannot timeout yourself.")
        return
    seconds = parse_duration_to_seconds(duration)
    if seconds is None or seconds <= 0:
        await ctx.send("⚠️ Invalid duration. Example: `10m`, `2h`, `1d`.")
        return
    if seconds > MAX_TIMEOUT_SECONDS:
        await ctx.send("⚠️ Maximum timeout duration is 28 days.")
        return

    bot_member = get_bot_member(ctx.guild)
    if bot_member is None or bot_member.top_role <= member.top_role:
        await ctx.send("⛔ I cannot timeout that member.")
        return
    if not can_manage_target(actor, member):
        await ctx.send("⛔ You cannot timeout that member.")
        return

    until = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    try:
        await member.timeout(until, reason=reason or f"Timed out by {actor}")
    except discord.Forbidden:
        await ctx.send("⛔ I cannot timeout that member.")
        return
    except discord.HTTPException as exc:
        await ctx.send(f"❌ Failed to apply timeout: {exc}")
        return
    case_id = log_moderation_action(
        guild_id=ctx.guild.id,
        action="timeout",
        actor_id=ctx.author.id,
        target_id=member.id,
        reason=reason or "",
        details=json.dumps({"until": until.isoformat(), "duration_seconds": seconds}),
        undoable=True,
    )
    await send_ops_log(
        ctx.guild,
        f"📘 Case `{case_id}` {ctx.author.mention} timed out {member.mention} for `{format_duration(seconds)}`.",
    )
    await ctx.send(
        f"✅ {member.mention} timed out for `{format_duration(seconds)}` "
        f"(<t:{int(until.timestamp())}:R>)."
    )


@bot.hybrid_command()
@commands.has_permissions(moderate_members=True)
async def untimeout(ctx: commands.Context, member: discord.Member, *, reason: str | None = None):
    """Remove a member timeout."""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    actor = await resolve_context_member(ctx, ctx.guild)
    if actor is None:
        await ctx.send("⚠️ Unable to verify your server membership. Try again in this server.")
        return
    bot_member = get_bot_member(ctx.guild)
    if bot_member is None or bot_member.top_role <= member.top_role:
        await ctx.send("⛔ I cannot modify that member.")
        return
    if not can_manage_target(actor, member):
        await ctx.send("⛔ You cannot modify that member.")
        return

    try:
        await member.timeout(None, reason=reason or f"Timeout removed by {actor}")
    except discord.Forbidden:
        await ctx.send("⛔ I cannot modify that member.")
        return
    except discord.HTTPException as exc:
        await ctx.send(f"❌ Failed to remove timeout: {exc}")
        return
    case_id = log_moderation_action(
        guild_id=ctx.guild.id,
        action="untimeout",
        actor_id=ctx.author.id,
        target_id=member.id,
        reason=reason or "",
        details="",
        undoable=False,
    )
    await send_ops_log(
        ctx.guild,
        f"📘 Case `{case_id}` {ctx.author.mention} removed timeout for {member.mention}.",
    )
    await ctx.send(f"✅ Timeout removed for {member.mention}.")


@bot.hybrid_command()
@commands.has_permissions(moderate_members=True)
async def warn(ctx: commands.Context, member: discord.Member, *, reason: str):
    """Issue a warning and record a moderation case."""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    actor = await resolve_context_member(ctx, ctx.guild)
    if actor is None:
        await ctx.send("⚠️ Unable to verify your server membership. Try again in this server.")
        return
    if not can_manage_target(actor, member):
        await ctx.send("⛔ You cannot warn that member.")
        return
    case_id = log_moderation_action(
        guild_id=ctx.guild.id,
        action="warn",
        actor_id=ctx.author.id,
        target_id=member.id,
        reason=reason,
        undoable=False,
    )
    await send_ops_log(
        ctx.guild,
        f"📘 Case `{case_id}` {ctx.author.mention} warned {member.mention}: {reason}",
    )
    await ctx.send(f"⚠️ {member.mention} warned. Case `{case_id}`.")


@bot.hybrid_command(name="cases")
@commands.has_permissions(moderate_members=True)
async def cases(ctx: commands.Context, member: discord.Member | None = None, limit: int = 10):
    """List recent moderation cases, optionally filtered by member."""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    entries = modlog.get(str(ctx.guild.id), [])
    if member:
        entries = [entry for entry in entries if entry.get("target_id") == member.id]
    if not entries:
        await ctx.send("No cases found.")
        return
    limit = max(1, min(20, limit))
    lines = []
    for entry in sorted(entries, key=lambda item: item.get("case_id", 0), reverse=True)[:limit]:
        created_at = parse_datetime_utc(entry.get("created_at"))
        when = f"<t:{int(created_at.timestamp())}:R>" if created_at else "unknown"
        target_id = entry.get("target_id")
        target_text = f"<@{target_id}>" if target_id else "n/a"
        lines.append(
            f"`{entry.get('case_id')}` {entry.get('action')} • target: {target_text} • "
            f"by <@{entry.get('actor_id')}> • {when}"
        )
    await send_chunked_message(ctx, "**Moderation cases:**\n" + "\n".join(lines))


@bot.hybrid_command(name="undo")
@commands.has_permissions(moderate_members=True)
async def undo(ctx: commands.Context, case_id: int):
    """Undo a supported moderation action by case ID."""
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    entries = modlog.get(str(ctx.guild.id), [])
    case = next((entry for entry in entries if entry.get("case_id") == case_id), None)
    if case is None:
        await ctx.send("❌ Case not found.")
        return
    if not case.get("undoable"):
        await ctx.send("❌ This case is not undoable.")
        return
    if case.get("undone"):
        await ctx.send("ℹ️ This case has already been undone.")
        return

    action = case.get("action")
    details = case.get("details", "")
    target_id = case.get("target_id")
    target = ctx.guild.get_member(target_id) if target_id else None

    try:
        if action == "timeout" and target:
            await target.timeout(None, reason=f"Undo case {case_id} by {ctx.author}")
        elif action == "nick" and target:
            payload = {}
            if details:
                try:
                    payload = json.loads(details)
                except json.JSONDecodeError:
                    payload = {}
            old_nick = payload.get("old_nick")
            await target.edit(nick=old_nick, reason=f"Undo case {case_id} by {ctx.author}")
        elif action == "slowmode":
            payload = {}
            if details:
                try:
                    payload = json.loads(details)
                except json.JSONDecodeError:
                    payload = {}
            channel_id = payload.get("channel_id")
            old_slowmode = as_int(payload.get("old_slowmode"))
            if old_slowmode is None or not 0 <= old_slowmode <= 21600:
                old_slowmode = 0
            channel = ctx.guild.get_channel(channel_id) if channel_id else None
            if isinstance(channel, discord.TextChannel):
                await channel.edit(slowmode_delay=old_slowmode)
            else:
                await ctx.send("❌ Could not find channel for slowmode undo.")
                return
        else:
            await ctx.send("❌ Undo target is no longer available.")
            return
    except Exception as exc:
        await ctx.send(f"❌ Undo failed: {exc}")
        return

    case["undone"] = True
    save_modlog()
    undo_case = log_moderation_action(
        guild_id=ctx.guild.id,
        action="undo",
        actor_id=ctx.author.id,
        target_id=target_id,
        reason=f"Undo case {case_id}",
        details=f"source_case={case_id}",
        undoable=False,
    )
    await send_ops_log(
        ctx.guild,
        f"📘 Case `{undo_case}` {ctx.author.mention} undid case `{case_id}` ({action}).",
    )
    await ctx.send(f"✅ Undid case `{case_id}` ({action}).")


@bot.hybrid_command()
@commands.cooldown(3, 30, commands.BucketType.user)
async def vanguard(ctx: commands.Context, *, question: str):
    """Ask the AI server and keep channel-local memory for follow-up questions."""
    async with ctx.typing():
        backend_headers = build_backend_headers()
        ask_payload = {
            "question": question,
            "username": str(ctx.author),
            "userId": str(ctx.author.id),
        }
        session_id = _build_ai_session_id(
            ctx.guild.id if ctx.guild else None,
            getattr(ctx.channel, "id", None),
            ctx.author.id,
        )
        chat_payload: dict[str, Any] = {
            "message": question,
            "sessionId": session_id,
            "style": AI_CHAT_STYLE,
            "historyMessages": AI_HISTORY_MESSAGES,
            "useContext": AI_USE_CONTEXT,
            "useCache": AI_USE_CACHE,
        }
        if AI_MODEL:
            chat_payload["model"] = AI_MODEL
        if AI_INCLUDE_DEBUG:
            chat_payload["includeDebug"] = True
        chat_options = _build_ai_options()
        if chat_options:
            chat_payload["options"] = chat_options

        title_text = "AI Response"
        answer = ""
        used_chat_endpoint = False
        try:
            chat_status_code: int | None = None
            chat_response = await asyncio.to_thread(
                requests.post,
                AI_CHAT_URL,
                json=chat_payload,
                headers=backend_headers or None,
                timeout=AI_REQUEST_TIMEOUT_SECONDS,
            )
            if chat_response.status_code == 200:
                try:
                    data = chat_response.json()
                except ValueError:
                    data = {}
                answer = _extract_ai_answer(data)
                used_chat_endpoint = bool(answer)
            else:
                chat_status_code = chat_response.status_code

            if not answer:
                response = await asyncio.to_thread(
                    requests.post,
                    AI_ASK_URL,
                    json=ask_payload,
                    headers=backend_headers or None,
                    timeout=AI_REQUEST_TIMEOUT_SECONDS,
                )
                if response.status_code == 200:
                    try:
                        data = response.json()
                    except ValueError:
                        data = {}
                    answer = _extract_ai_answer(data)
                    if not answer:
                        answer = "No response from the AI service."
                else:
                    status_code = chat_status_code if chat_status_code is not None else response.status_code
                    title_text = f"AI service returned HTTP {status_code}."
                    answer = response.text
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            title_text = "AI service is currently unreachable."
        except Exception:
            title_text = "Unexpected error while contacting the AI service."

    answer = clamp_text(answer, MAX_DISCORD_EMBED_DESCRIPTION_CHARS)
    if not answer:
        answer = "No additional details."
    embed = discord.Embed(title=title_text, description=answer, color=discord.Color.red())
    if used_chat_endpoint:
        embed.set_footer(text=f"Powered by Vanguard AI • style={AI_CHAT_STYLE} • memory=on")
    else:
        embed.set_footer(text="Powered by Vanguard AI • compatibility mode")
    await ctx.send(embed=embed)


@bot.hybrid_command(name="vanguardreset")
@commands.cooldown(3, 30, commands.BucketType.user)
async def vanguardreset(ctx: commands.Context):
    """Clear your AI chat memory for this channel."""
    session_id = _build_ai_session_id(
        ctx.guild.id if ctx.guild else None,
        getattr(ctx.channel, "id", None),
        ctx.author.id,
    )
    delete_url = f"{AI_SESSION_URL}/{quote(session_id, safe='')}"
    backend_headers = build_backend_headers()
    try:
        response = await asyncio.to_thread(
            requests.delete,
            delete_url,
            headers=backend_headers or None,
            timeout=AI_REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code in {200, 204, 404}:
            await ctx.send("✅ AI session memory reset for this channel.")
        else:
            await ctx.send(f"⚠️ Could not reset AI session (HTTP {response.status_code}).")
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
        await ctx.send("⚠️ AI session reset service is unreachable right now.")
    except Exception:
        await ctx.send("⚠️ Unexpected error while resetting AI session memory.")


@bot.hybrid_command(name="flaguser", aliases=["fuck"])
@commands.is_owner()
async def flaguser(ctx: commands.Context, target: str):
    """Owner-only: mark a user in backend moderation service."""
    await send_backend_user_update(ctx, target, FLAG_USER_URL, "has been flagged")


@bot.hybrid_command(name="unflaguser", aliases=["unfuck"])
@commands.is_owner()
async def unflaguser(ctx: commands.Context, target: str):
    """Owner-only: remove a backend moderation flag for a user."""
    await send_backend_user_update(ctx, target, UNFLAG_USER_URL, "has been unflagged")


@bot.hybrid_command()
@commands.is_owner()
async def owneronly(ctx: commands.Context, state: str | None = None):
    """Owner-only: toggle global owner-only mode. Usage: /owneronly on|off"""
    if state is None:
        status = "ON" if settings.get("owner_only", False) else "OFF"
        await ctx.send(f"Owner-only mode is currently `{status}`.")
        return

    normalized = state.lower().strip()
    if normalized in {"on", "enable", "enabled", "true"}:
        settings["owner_only"] = True
    elif normalized in {"off", "disable", "disabled", "false"}:
        settings["owner_only"] = False
    else:
        await ctx.send("⚠️ Use `/owneronly on` or `/owneronly off`.")
        return

    save_settings()
    await ctx.send(f"Owner-only mode set to `{'ON' if settings['owner_only'] else 'OFF'}`.")


@bot.hybrid_command()
async def testwelcome(ctx: commands.Context, target: discord.Member | None = None):
    """Send a preview welcome embed."""
    result = await require_guild_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    member = target or ctx.author
    if not isinstance(member, discord.Member):
        await ctx.send("⚠️ Could not resolve member.")
        return
    await ctx.send(embed=build_welcome_embed(member, guild_cfg))


@bot.hybrid_command()
async def rules(ctx: commands.Context):
    embed = discord.Embed(
        title="Server Rules",
        description=(
            "1. Be respectful.\n"
            "2. No spam.\n"
            "3. Keep content in the correct channels.\n"
            "4. No harassment or hate speech.\n"
            "5. Follow moderator instructions."
        ),
        color=discord.Color.red(),
    )
    await ctx.send(embed=embed)


@bot.hybrid_command()
async def serverinfo(ctx: commands.Context):
    if ctx.guild is None:
        await ctx.send("⚠️ This command can only be used in a server.")
        return
    guild = ctx.guild
    owner = guild.get_member(guild.owner_id)
    if owner is None:
        try:
            owner = await guild.fetch_member(guild.owner_id)
        except Exception:
            owner = None

    embed = discord.Embed(title=f"Server Info: {guild.name}", color=discord.Color.red())
    embed.add_field(name="Owner", value=owner.mention if owner else "Unknown", inline=False)
    embed.add_field(name="Created", value=guild.created_at.strftime("%Y-%m-%d %H:%M:%S"), inline=False)
    embed.add_field(name="Members", value=str(guild.member_count), inline=False)
    embed.add_field(name="Server ID", value=str(guild.id), inline=False)
    if guild.icon:
        embed.set_thumbnail(url=guild.icon.url)
    await ctx.send(embed=embed)


@bot.hybrid_command()
async def mcstatus(ctx: commands.Context):
    result = await require_guild_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_host = guild_cfg.get("mc_host")
    if guild_host:
        host = guild_host
        port = guild_cfg.get("mc_port", 25565)
    else:
        host = MC_DEFAULT_HOST
        port = MC_DEFAULT_PORT

    if not host:
        await ctx.send("⚠️ Minecraft server is not configured. Use `/setmcserver <host> [port]`.")
        return

    server = JavaServer.lookup(f"{host}:{port}")
    try:
        status = await asyncio.to_thread(server.status)
        await ctx.send(
            f"✅ Minecraft server `{host}:{port}` is online. "
            f"Players: {status.players.online}/{status.players.max}"
        )
    except Exception:
        await ctx.send(f"❌ Minecraft server `{host}:{port}` is offline or unreachable.")


@bot.hybrid_command()
async def lockdown(ctx: commands.Context):
    await set_lockdown_state(ctx, True)


@bot.hybrid_command()
async def unlock(ctx: commands.Context):
    await set_lockdown_state(ctx, False)


@bot.hybrid_command(name="prefix")
async def prefix_command(ctx: commands.Context, new_prefix: str | None = None):
    """Legacy command retained for compatibility."""
    _ = new_prefix
    await ctx.send("ℹ️ Prefix commands are disabled. Use slash commands (`/`) only.")


@bot.hybrid_command(name="setwelcomechannel")
async def setwelcomechannel(ctx: commands.Context, channel: ConfigChannelInput | None = None):
    """Mod/admin: set welcome channel. Omit to clear."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    normalized_channel = ensure_text_channel(channel)
    if channel is not None and normalized_channel is None:
        await ctx.send("⚠️ `channel` must be a text channel.")
        return
    channel_id = get_channel_id(normalized_channel)
    if channel is not None and channel_id is None:
        await ctx.send("⚠️ Could not resolve channel ID.")
        return
    guild_cfg["welcome_channel_id"] = channel_id if normalized_channel else None
    save_settings()
    if normalized_channel:
        await ctx.send(f"✅ Welcome channel set to {format_channel_mention(normalized_channel)}.")
    else:
        await ctx.send("✅ Welcome channel cleared. System/default channel fallback will be used.")


@bot.hybrid_command(name="setwelcomerole")
async def setwelcomerole(ctx: commands.Context, role: discord.Role | None = None):
    """Mod/admin: set role to auto-assign on join. Omit to clear."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_cfg["welcome_role_id"] = role.id if role else None
    save_settings()
    if role:
        await ctx.send(f"✅ Welcome role set to `{role.name}`.")
    else:
        await ctx.send("✅ Welcome role cleared.")


@bot.hybrid_command(name="setwelcomemessage")
async def setwelcomemessage(ctx: commands.Context, *, message: str | None = None):
    """Mod/admin: set welcome message. Supports {user}, {username}, {server}. Use `clear` to reset."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    if message is None or message.strip().lower() in {"clear", "reset", "default"}:
        guild_cfg["welcome_message"] = None
        save_settings()
        await ctx.send("✅ Welcome message reset to default.")
        return
    guild_cfg["welcome_message"] = message.strip()[:500]
    save_settings()
    await ctx.send("✅ Welcome message updated.")


@bot.hybrid_command(name="setlockdownrole")
async def setlockdownrole(ctx: commands.Context, role: discord.Role | None = None):
    """Mod/admin: set role targeted by lockdown. Omit to use @everyone."""
    result = await require_mod_context(ctx)
    if not result:
        return
    guild, guild_cfg = result
    guild_cfg["lockdown_role_id"] = role.id if role else None
    save_settings()
    if role:
        await ctx.send(f"✅ Lockdown role set to `{role.name}`.")
    else:
        default_role = resolve_default_role(guild)
        default_name = default_role.name if default_role is not None else "@everyone"
        await ctx.send(f"✅ Lockdown role reset to default `{default_name}`.")


@bot.hybrid_command(name="setmodroles")
async def setmodroles(ctx: commands.Context, roles: str | None = None):
    """Mod/admin: set additional roles allowed to run moderation/config commands."""
    result = await require_mod_context(ctx)
    if not result:
        return
    guild, guild_cfg = result

    parsed_roles: list[discord.Role] = []
    if roles:
        for chunk in [part.strip() for part in roles.split(",") if part.strip()]:
            role: discord.Role | None = None
            role_id = extract_id(chunk)
            if role_id:
                role = guild.get_role(int(role_id))
            if role is None:
                role = discord.utils.get(guild.roles, name=chunk)
            if role and role not in parsed_roles:
                parsed_roles.append(role)

    guild_cfg["mod_role_ids"] = sorted({role.id for role in parsed_roles})
    save_settings()
    if parsed_roles:
        await ctx.send("✅ Mod roles set to: " + ", ".join(f"`{role.name}`" for role in parsed_roles))
    else:
        await ctx.send("✅ Mod role list cleared. Only Manage Server/Admin can run mod commands.")


@bot.hybrid_command(name="setmcserver")
async def setmcserver(ctx: commands.Context, host: str, port: int = 25565):
    """Mod/admin: configure this guild's Minecraft server host/port."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    if not 1 <= port <= 65535:
        await ctx.send("⚠️ Port must be between 1 and 65535.")
        return
    normalized_host = host.strip()
    if not normalized_host:
        await ctx.send("⚠️ Host cannot be empty.")
        return
    guild_cfg["mc_host"] = normalized_host
    guild_cfg["mc_port"] = port
    save_settings()
    await ctx.send(f"✅ Minecraft server set to `{guild_cfg['mc_host']}:{guild_cfg['mc_port']}`.")


@bot.hybrid_command(name="clearmcserver")
async def clearmcserver(ctx: commands.Context):
    """Mod/admin: clear this guild's Minecraft server settings."""
    result = await require_mod_context(ctx)
    if not result:
        return
    _, guild_cfg = result
    guild_cfg["mc_host"] = None
    guild_cfg["mc_port"] = 25565
    save_settings()
    await ctx.send("✅ Minecraft server setting cleared for this server.")


@bot.hybrid_command(name="showconfig")
async def showconfig(ctx: commands.Context):
    """Show active configuration for this server."""
    result = await require_guild_context(ctx)
    if not result:
        return
    guild, guild_cfg = result

    welcome_channel = guild.get_channel(guild_cfg.get("welcome_channel_id")) if guild_cfg.get("welcome_channel_id") else None
    welcome_role = resolve_role(guild, guild_cfg.get("welcome_role_id"))
    log_channel = guild.get_channel(guild_cfg.get("log_channel_id")) if guild_cfg.get("log_channel_id") else None
    ops_channel = guild.get_channel(guild_cfg.get("ops_channel_id")) if guild_cfg.get("ops_channel_id") else None
    lockdown_role = (
        resolve_role(guild, guild_cfg.get("lockdown_role_id"))
        if guild_cfg.get("lockdown_role_id")
        else resolve_default_role(guild)
    )
    mod_roles = [resolve_role(guild, role_id) for role_id in guild_cfg.get("mod_role_ids", [])]
    mod_roles = [role for role in mod_roles if role]

    if guild_cfg.get("mc_host"):
        host = guild_cfg["mc_host"]
        port = guild_cfg.get("mc_port", 25565)
    else:
        host = MC_DEFAULT_HOST
        port = MC_DEFAULT_PORT

    embed = discord.Embed(title="Server Bot Configuration", color=discord.Color.red())
    embed.add_field(name="Commands", value="Slash-only (`/`)", inline=False)
    embed.add_field(
        name="Welcome Channel",
        value=welcome_channel.mention if isinstance(welcome_channel, discord.TextChannel) else "Not set (fallback mode)",
        inline=False,
    )
    embed.add_field(name="Welcome Role", value=welcome_role.mention if welcome_role else "Not set", inline=False)
    embed.add_field(
        name="Welcome Message",
        value=guild_cfg.get("welcome_message") or "Default",
        inline=False,
    )
    embed.add_field(
        name="Log Channel",
        value=log_channel.mention if isinstance(log_channel, discord.TextChannel) else "Not set",
        inline=False,
    )
    embed.add_field(
        name="Ops Channel",
        value=ops_channel.mention if isinstance(ops_channel, discord.TextChannel) else "Not set",
        inline=False,
    )
    embed.add_field(
        name="Lockdown Role",
        value=lockdown_role.mention if lockdown_role else "Missing role",
        inline=False,
    )
    embed.add_field(
        name="Mod Roles",
        value=", ".join(role.mention for role in mod_roles) if mod_roles else "Not set (Manage Server/Admin only)",
        inline=False,
    )
    embed.add_field(
        name="Minecraft Server",
        value=f"`{host}:{port}`" if host else "Not set",
        inline=False,
    )
    embed.add_field(
        name="Guard",
        value=(
            f"enabled={guild_cfg.get('guard_enabled', False)}, "
            f"threshold={guild_cfg.get('guard_threshold', 8)}, "
            f"window={guild_cfg.get('guard_window_seconds', 30)}s"
        ),
        inline=False,
    )
    await ctx.send(embed=embed)


@bot.hybrid_command(name="voteinfo")
async def voteinfo(ctx: commands.Context, vote_id: str):
    """Show detailed state for a specific vote."""
    result = await require_guild_context(ctx)
    if not result:
        return
    guild, _ = result

    if not str(vote_id).startswith(f"{guild.id}-"):
        await ctx.send("❌ This vote ID does not belong to this server.")
        return

    vote = vote_store.get(vote_id)
    if not vote:
        await ctx.send("❌ Vote not found.")
        return

    tallies, turnout = tally_vote(vote)
    finish_at = parse_datetime_utc(vote.get("finish_at"))
    finish_text = f"<t:{int(finish_at.timestamp())}:R>" if finish_at else "unknown"

    option_lines = []
    for option in vote.get("options", []):
        if not isinstance(option, dict):
            continue
        option_id = str(option.get("id") or "").strip()
        if not option_id:
            continue
        count = tallies.get(option_id, 0)
        pct = (count / turnout * 100) if turnout > 0 else 0.0
        option_lines.append(f"{option_label(vote, option_id)}: {count} ({pct:.1f}%)")

    header_lines = [
        f"**{vote.get('title', 'Vote')}**",
        f"Type: `{vote.get('vote_type', 'proposal')}` • Ballot mode: `{vote.get('ballot_mode', 'single')}`",
        f"Turnout: {turnout} • Ends: {finish_text}",
        f"Anonymous: {'yes' if bool(vote.get('anonymous')) else 'no'}",
    ]
    if option_lines:
        header_lines.append("Tallies:")
        header_lines.extend(option_lines)

    is_anonymous = bool(vote.get("anonymous"))
    can_view_ballots = (not is_anonymous) or ctx.author.guild_permissions.manage_guild
    ballots = vote.get("ballots", {})
    if not isinstance(ballots, dict):
        ballots = {}

    if not ballots:
        header_lines.append("No ballots yet.")
        await send_chunked_message(ctx, "\n".join(header_lines))
        return

    if not can_view_ballots:
        header_lines.append("Individual ballots are hidden for this anonymous vote.")
        await send_chunked_message(ctx, "\n".join(header_lines))
        return

    ballot_lines = []
    for user_id, choice in ballots.items():
        try:
            numeric_id = int(user_id)
        except (TypeError, ValueError):
            continue
        member = guild.get_member(numeric_id)
        username = member.display_name if member else f"Unknown ({user_id})"
        ballot_lines.append(f"{username}: {ballot_to_text(vote, choice)}")

    if ballot_lines:
        header_lines.append("Ballots:")
        header_lines.extend(ballot_lines)
    await send_chunked_message(ctx, "\n".join(header_lines))


@bot.hybrid_command(name="activevotes")
async def activevotes(ctx: commands.Context):
    """List active votes in this server."""
    result = await require_guild_context(ctx)
    if not result:
        return
    guild, _ = result

    active = [
        (vote_id, vote)
        for vote_id, vote in vote_store.items()
        if str(vote_id).startswith(f"{guild.id}-")
    ]
    if not active:
        await ctx.send("No active votes in this server.")
        return

    lines = []
    for vote_id, vote in sorted(active, key=lambda item: item[0])[:10]:
        tallies, turnout = tally_vote(vote)
        finish_at = parse_datetime_utc(vote.get("finish_at"))
        finish_text = f"<t:{int(finish_at.timestamp())}:R>" if finish_at else "unknown"
        title = str(vote.get("title") or "Vote")
        vote_type = str(vote.get("vote_type") or "proposal")
        if vote_type == "confidence" and vote.get("target_id"):
            title = f"{title} (Target: <@{vote.get('target_id')}>)"

        if tallies and bool(vote.get("show_live_results", True)):
            top_option_id = max(
                tallies.keys(),
                key=lambda option_id: (tallies.get(option_id, 0), option_label(vote, option_id).lower()),
            )
            leader_text = f"{option_label(vote, top_option_id)} ({tallies.get(top_option_id, 0)})"
        elif tallies:
            leader_text = "hidden"
        else:
            leader_text = "no ballots"

        lines.append(
            f"`{vote_id}`\n{title} • Type: `{vote_type}` • Turnout: {turnout} • "
            f"Leader: {leader_text} • Ends: {finish_text}"
        )
    await send_chunked_message(ctx, "**Active votes:**\n" + "\n\n".join(lines))


if __name__ == "__main__":
    DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
    if not DISCORD_TOKEN:
        raise RuntimeError("Missing DISCORD_BOT_TOKEN environment variable.")

    bot.run(DISCORD_TOKEN)
