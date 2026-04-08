# Copyright (c) 2026 Continental. All rights reserved.
# Licensed under the Vanguard Proprietary Source-Available License (see /LICENSE).

import asyncio
from collections import defaultdict
import json
import os
import platform
import random
import re
from threading import local
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import discord
from discord import app_commands
from discord.ext import commands
import requests
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv() -> bool:
        return False

from control_center import (
    build_control_center_url,
    create_control_center_app,
    start_control_center_site,
)
from data_paths import resolve_data_file
from guard import (
    apply_guard_preset,
    guard_default_settings,
    guard_runtime_stats,
    handle_guard_member_join,
    handle_guard_message,
    normalize_guard_settings,
    resolve_guard_preset_name,
    setup_guard_module,
)
from vote import (
    ballot_to_text,
    option_label,
    restore_vote_state,
    setup_vote_module,
    tally_vote,
    votes as vote_store,
)

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

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


def _resolve_optional_base_url(explicit_base_url: str) -> str:
    return explicit_base_url.strip().rstrip("/")


def _resolve_service_url(
    explicit_url: str,
    base_url: str,
    path: str,
    legacy_default: str = "",
) -> str:
    explicit = explicit_url.strip()
    legacy = legacy_default.strip()
    if explicit and not (base_url and legacy and explicit == legacy):
        return explicit
    if base_url:
        return f"{base_url}{path}"
    return legacy


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
CONTINENTAL_ID_BASE_URL = _resolve_optional_base_url(os.getenv("CONTINENTAL_ID_BASE_URL", ""))
CONTINENTAL_ID_AUTH_BASE_URL = _resolve_optional_base_url(
    os.getenv("CONTINENTAL_ID_AUTH_BASE_URL", "")
) or CONTINENTAL_ID_BASE_URL
CONTINENTAL_ID_HEALTH_URL = _resolve_service_url(
    os.getenv("CONTINENTAL_ID_HEALTH_URL", ""),
    CONTINENTAL_ID_BASE_URL,
    "/api/vanguard/health",
)
CONTINENTAL_ID_RESOLVE_URL = _resolve_service_url(
    os.getenv("CONTINENTAL_ID_RESOLVE_URL", ""),
    CONTINENTAL_ID_BASE_URL,
    "/api/vanguard/users/resolve",
)
CONTINENTAL_ID_LOGIN_URL = (
    os.getenv("CONTINENTAL_ID_LOGIN_URL", "https://login.continental-hub.com/popup.html").strip()
)
CONTINENTAL_ID_DASHBOARD_URL = (
    os.getenv("CONTINENTAL_ID_DASHBOARD_URL", "https://dashboard.continental-hub.com/?tab=settings").strip()
)
AI_REQUEST_TIMEOUT_SECONDS = _parse_env_int("AI_REQUEST_TIMEOUT_SECONDS", 60, minimum=2, maximum=120)
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
FLAG_USER_URL = _resolve_service_url(
    os.getenv("FLAG_USER_URL", ""),
    CONTINENTAL_ID_BASE_URL,
    "/api/vanguard/users/flag",
    legacy_default="http://localhost:3001/fuck",
)
UNFLAG_USER_URL = _resolve_service_url(
    os.getenv("UNFLAG_USER_URL", ""),
    CONTINENTAL_ID_BASE_URL,
    "/api/vanguard/users/unflag",
    legacy_default="http://localhost:3001/unfuck",
)
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
VANGUARD_LICENSE_VERIFY_URL = _resolve_service_url(
    os.getenv("VANGUARD_LICENSE_VERIFY_URL", ""),
    CONTINENTAL_ID_BASE_URL,
    "/api/vanguard/license/verify",
)
VANGUARD_LICENSE_KEY = os.getenv("VANGUARD_LICENSE_KEY", "").strip()
VANGUARD_REQUIRE_LICENSE = _parse_env_bool("VANGUARD_REQUIRE_LICENSE", False)
VANGUARD_LICENSE_RECHECK_SECONDS = _parse_env_int(
    "VANGUARD_LICENSE_RECHECK_SECONDS",
    900,
    minimum=60,
    maximum=86400,
)
VANGUARD_CONTROL_CENTER_ENABLED = _parse_env_bool("VANGUARD_CONTROL_CENTER_ENABLED", False)
VANGUARD_CONTROL_CENTER_HOST = (
    os.getenv("VANGUARD_CONTROL_CENTER_HOST", "127.0.0.1").strip() or "127.0.0.1"
)
VANGUARD_CONTROL_CENTER_PORT = _parse_env_int(
    "VANGUARD_CONTROL_CENTER_PORT",
    8080,
    minimum=1,
    maximum=65535,
)
VANGUARD_CONTROL_CENTER_PUBLIC_URL = os.getenv("VANGUARD_CONTROL_CENTER_PUBLIC_URL", "").strip()
VANGUARD_GUILD_JOIN_NOTIFY_USER_ID = _parse_env_optional_int(
    "VANGUARD_GUILD_JOIN_NOTIFY_USER_ID",
    minimum=1,
)
SETTINGS_FILE = resolve_data_file("settings.json")
REMINDERS_FILE = resolve_data_file("reminders.json")
MOD_LOG_FILE = resolve_data_file("modlog.json")
CONTROL_CENTER_STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "control_center")
LANDING_SITE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs")
START_TIME = datetime.now(timezone.utc)
REMINDER_CHECK_SECONDS = 15
MAX_REMINDER_SECONDS = 60 * 60 * 24 * 30
MAX_TIMEOUT_SECONDS = 60 * 60 * 24 * 28
MAX_CASES_PER_GUILD = 1000
MAX_DISCORD_MESSAGE_CHARS = 1900
MAX_DISCORD_EMBED_DESCRIPTION_CHARS = 3900

PRIVACY_URL = os.getenv("PRIVACY_POLICY_URL", "").strip()
TOS_URL = os.getenv("TERMS_OF_SERVICE_URL", "").strip()

ID_RE = re.compile(r"(\d{17,20})")
DURATION_TOKEN_RE = re.compile(r"(\d+)([smhdw])")

ConfigChannelInput = discord.TextChannel


def default_guild_settings() -> dict[str, Any]:
    guild_cfg: dict[str, Any] = {
        "welcome_channel_id": None,
        "welcome_role_id": None,
        "welcome_message": None,
        "ops_channel_id": None,
        "log_channel_id": None,
        "lockdown_role_id": None,
        "mod_role_ids": [],
    }
    guild_cfg.update(guard_default_settings())
    return guild_cfg


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

        guild_cfg.update(normalize_guard_settings(cfg))

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


def save_settings() -> bool:
    try:
        write_json_atomic(SETTINGS_FILE, settings)
    except OSError as exc:
        print(f"[FILE] Failed saving settings: {exc}")
        return False
    return True


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


def build_guild_join_notification(guild: discord.Guild, *, authorized: bool) -> str:
    status = "authorized" if authorized else "unauthorized"
    lines = [
        "Vanguard joined a server.",
        f"Server: {guild.name}",
        f"Server ID: {guild.id}",
        f"Owner ID: {getattr(guild, 'owner_id', 'unknown')}",
        f"Members: {getattr(guild, 'member_count', 'unknown')}",
        f"Status: {status}",
    ]
    return "\n".join(lines)


async def notify_personal_account_guild_join(guild: discord.Guild, *, authorized: bool) -> bool:
    if VANGUARD_GUILD_JOIN_NOTIFY_USER_ID is None:
        return False

    user = bot.get_user(VANGUARD_GUILD_JOIN_NOTIFY_USER_ID)
    if user is None:
        try:
            user = await bot.fetch_user(VANGUARD_GUILD_JOIN_NOTIFY_USER_ID)
        except Exception as exc:
            print(f"[GUILD JOIN] Failed fetching notify user {VANGUARD_GUILD_JOIN_NOTIFY_USER_ID}: {exc}")
            return False

    try:
        await user.send(build_guild_join_notification(guild, authorized=authorized))
    except Exception as exc:
        print(f"[GUILD JOIN] Failed sending join notification for guild {guild.id}: {exc}")
        return False
    return True


def build_backend_headers(include_license: bool = False) -> dict[str, str]:
    headers: dict[str, str] = {}
    if VANGUARD_BACKEND_API_KEY:
        headers[VANGUARD_BACKEND_KEY_HEADER] = VANGUARD_BACKEND_API_KEY
    if VANGUARD_INSTANCE_ID:
        headers[VANGUARD_INSTANCE_HEADER] = VANGUARD_INSTANCE_ID
    if include_license and VANGUARD_LICENSE_KEY:
        headers["Authorization"] = f"Bearer {VANGUARD_LICENSE_KEY}"
    return headers


_http_session_local = local()


def _get_http_session() -> requests.Session:
    session = getattr(_http_session_local, "session", None)
    if session is not None:
        return session
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=16,
        pool_maxsize=16,
        max_retries=0,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    setattr(_http_session_local, "session", session)
    return session


def http_request(method: str, url: str, **kwargs: Any) -> requests.Response:
    return _get_http_session().request(method=method, url=url, **kwargs)


def continental_id_configured() -> bool:
    return bool(CONTINENTAL_ID_HEALTH_URL or CONTINENTAL_ID_RESOLVE_URL)


def continental_id_auth_configured() -> bool:
    return bool(CONTINENTAL_ID_AUTH_BASE_URL and CONTINENTAL_ID_LOGIN_URL)


def get_control_center_url() -> str:
    return build_control_center_url(
        VANGUARD_CONTROL_CENTER_HOST,
        VANGUARD_CONTROL_CENTER_PORT,
        VANGUARD_CONTROL_CENTER_PUBLIC_URL,
    )


def get_control_center_continental_status(discord_user_id: int | None) -> dict[str, Any]:
    if discord_user_id is None:
        return {
            "configured": continental_id_configured(),
            "ok": False,
            "linked": False,
            "message": (
                "Sign in with Continental ID to load account context."
                if continental_id_configured()
                else "Continental ID integration is not configured."
            ),
            "body": {},
        }
    return resolve_continental_user_sync(discord_user_id)


def get_control_center_license_state() -> dict[str, Any]:
    return {
        "configured": bool(VANGUARD_LICENSE_VERIFY_URL),
        "required": VANGUARD_REQUIRE_LICENSE,
        "authorized": license_authorized,
        "reason": license_reason,
        "allowed_guild_ids": sorted(get_effective_allowed_guild_ids()),
        "entitlements": dict(license_entitlements),
        "last_checked_at": license_last_checked_at.isoformat() if license_last_checked_at else None,
    }


def fetch_continental_me_sync(access_token: str) -> dict[str, Any]:
    token = str(access_token or "").strip()
    if not CONTINENTAL_ID_AUTH_BASE_URL:
        return {
            "configured": False,
            "ok": False,
            "message": "Continental ID auth is not configured.",
            "user": None,
        }
    if not token:
        return {
            "configured": True,
            "ok": False,
            "message": "Missing Continental ID access token.",
            "user": None,
        }

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    try:
        response = http_request(
            "GET",
            f"{CONTINENTAL_ID_AUTH_BASE_URL}/api/auth/me",
            headers=headers,
            timeout=6,
        )
    except requests.exceptions.RequestException as exc:
        return {
            "configured": True,
            "ok": False,
            "message": f"Request error: {exc}",
            "user": None,
        }

    try:
        payload = response.json()
    except ValueError:
        payload = {}

    body = payload if isinstance(payload, dict) else {}
    if response.status_code != 200:
        message = str(body.get("message") or "").strip()
        if not message:
            message = clamp_text(response.text, 240) or f"HTTP {response.status_code}"
        return {
            "configured": True,
            "ok": False,
            "message": message,
            "user": None,
        }

    user = body.get("user")
    if not isinstance(user, dict):
        return {
            "configured": True,
            "ok": False,
            "message": "Continental ID returned an invalid account payload.",
            "user": None,
        }

    return {
        "configured": True,
        "ok": True,
        "message": "",
        "user": user,
    }


def resolve_continental_user_sync(discord_user_id: int | str) -> dict[str, Any]:
    normalized_id = str(discord_user_id or "").strip()
    if not normalized_id:
        return {
            "configured": continental_id_configured(),
            "ok": False,
            "linked": False,
            "message": "Missing Discord user ID.",
            "status_code": None,
            "body": {},
        }
    if not CONTINENTAL_ID_RESOLVE_URL:
        return {
            "configured": False,
            "ok": False,
            "linked": False,
            "message": "Continental ID integration is not configured.",
            "status_code": None,
            "body": {},
        }

    headers = build_backend_headers()
    try:
        response = http_request(
            "POST",
            CONTINENTAL_ID_RESOLVE_URL,
            json={"discordUserId": normalized_id},
            headers=headers or None,
            timeout=6,
        )
    except requests.exceptions.RequestException as exc:
        return {
            "configured": True,
            "ok": False,
            "linked": False,
            "message": f"Request error: {exc}",
            "status_code": None,
            "body": {},
        }

    try:
        body = response.json()
    except ValueError:
        body = {}

    payload = body if isinstance(body, dict) else {}
    if response.status_code != 200:
        message = str(payload.get("message") or "").strip()
        if not message:
            message = clamp_text(response.text, 240) or f"HTTP {response.status_code}"
        return {
            "configured": True,
            "ok": False,
            "linked": False,
            "message": message,
            "status_code": response.status_code,
            "body": payload,
        }

    return {
        "configured": True,
        "ok": True,
        "linked": bool(payload.get("linked")),
        "message": "",
        "status_code": response.status_code,
        "body": payload,
    }


def format_continental_flags(flags: Any) -> str:
    if not isinstance(flags, dict):
        return "none"

    labels: list[str] = []
    if bool(flags.get("trusted")):
        labels.append("trusted")
    if bool(flags.get("staff")):
        labels.append("staff")
    if bool(flags.get("flagged")):
        labels.append("flagged")
    if bool(flags.get("bannedFromAi")):
        labels.append("ai-blocked")
    return ", ".join(labels) if labels else "none"


def format_continental_timestamp(value: Any) -> str:
    parsed = parse_datetime_utc(value)
    if parsed is None:
        return "Unknown"
    return parsed.strftime("%Y-%m-%d %H:%M:%S UTC")


def get_ai_access_requirement_message(continental_result: Any) -> str | None:
    result = continental_result if isinstance(continental_result, dict) else {}
    if not result.get("configured"):
        return "⛔ Vanguard AI requires a linked Continental ID account, but Continental ID integration is not configured on this Vanguard instance."

    if not result.get("ok"):
        message = str(result.get("message") or "Unknown error").strip()
        if message:
            return f"⚠️ I couldn't verify your Continental ID account right now. `{message}`"
        return "⚠️ I couldn't verify your Continental ID account right now."

    body = result.get("body")
    payload = body if isinstance(body, dict) else {}
    user_payload = payload.get("user")
    user = user_payload if isinstance(user_payload, dict) else {}
    flags_payload = payload.get("flags")
    flags = flags_payload if isinstance(flags_payload, dict) else {}

    if not bool(payload.get("linked")) or not bool(user.get("discordLinked")):
        return (
            "⛔ You must link your Continental ID account to your Discord account before you can use Vanguard AI. "
            "Use `/continentalid` to check your link status."
        )

    if bool(flags.get("bannedFromAi")):
        return "⛔ Your Continental ID account is not allowed to use Vanguard AI."

    return None


async def require_ai_access(ctx: commands.Context) -> bool:
    await safe_ctx_defer(ctx)
    result = await asyncio.to_thread(resolve_continental_user_sync, ctx.author.id)
    denial_message = get_ai_access_requirement_message(result)
    if denial_message:
        await safe_ctx_send(ctx, denial_message)
        return False
    return True


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


def normalize_license_entitlements(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "ai": False,
            "advancedVotes": False,
            "guardPresets": [],
        }

    raw_presets = payload.get("guardPresets", payload.get("guard_presets", []))
    guard_presets = (
        sorted({str(item).strip().lower() for item in raw_presets if str(item).strip()})
        if isinstance(raw_presets, list)
        else []
    )
    return {
        "ai": bool(payload.get("ai")),
        "advancedVotes": bool(payload.get("advancedVotes", payload.get("advanced_votes"))),
        "guardPresets": guard_presets,
    }


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
) -> tuple[bool, str, set[int], dict[str, Any]]:
    if not VANGUARD_LICENSE_VERIFY_URL:
        if VANGUARD_REQUIRE_LICENSE:
            return False, "VANGUARD_LICENSE_VERIFY_URL is not configured.", set(), normalize_license_entitlements({})
        return True, "license checks disabled", set(), normalize_license_entitlements({})

    payload = {
        "instanceId": VANGUARD_INSTANCE_ID or platform.node() or "vanguard-instance",
        "botUserId": str(bot_user_id or ""),
        "guildCount": guild_count,
    }
    headers = build_backend_headers(include_license=True)

    response = http_request(
        "POST",
        VANGUARD_LICENSE_VERIFY_URL,
        json=payload,
        headers=headers or None,
        timeout=8,
    )
    if response.status_code != 200:
        return False, f"license endpoint returned HTTP {response.status_code}", set(), normalize_license_entitlements({})

    try:
        body = response.json()
    except ValueError:
        return False, "license endpoint returned non-JSON response", set(), normalize_license_entitlements({})
    if not isinstance(body, dict):
        return False, "license endpoint returned malformed response", set(), normalize_license_entitlements({})

    authorized = bool(body.get("authorized", False))
    reason = str(body.get("reason") or ("authorized" if authorized else "unauthorized"))
    allowed_ids = parse_allowed_guild_ids(
        body.get("allowedGuildIds", body.get("allowed_guild_ids"))
    )
    entitlements = normalize_license_entitlements(body.get("entitlements"))
    return authorized, reason[:240], allowed_ids, entitlements


async def refresh_license_state() -> None:
    global license_authorized, license_reason, license_allowed_guild_ids, license_last_checked_at, license_entitlements
    try:
        authorized, reason, allowed_ids, entitlements = await asyncio.to_thread(
            verify_license_sync,
            bot.user.id if bot.user else None,
            len(bot.guilds),
        )
    except requests.exceptions.RequestException as exc:
        authorized, reason, allowed_ids, entitlements = (
            False,
            f"license request error: {exc}",
            set(),
            normalize_license_entitlements({}),
        )
    except Exception as exc:
        authorized, reason, allowed_ids, entitlements = (
            False,
            f"license check error: {exc}",
            set(),
            normalize_license_entitlements({}),
        )

    if VANGUARD_REQUIRE_LICENSE:
        license_authorized = authorized
    else:
        license_authorized = True

    license_reason = reason
    license_allowed_guild_ids = allowed_ids
    license_entitlements = entitlements
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
    await safe_ctx_defer(ctx)
    user_id = extract_id(target)
    if not user_id:
        await safe_ctx_send(ctx, "⚠️ Provide a mention (`@user`) or the raw numeric ID.")
        return

    payload = {"userId": user_id}
    backend_headers = build_backend_headers()
    try:
        response = await asyncio.to_thread(
            http_request,
            "POST",
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


async def can_user_manage_control_center_guild(guild: discord.Guild, user_id: int) -> bool:
    member = guild.get_member(user_id)
    if member is None:
        try:
            member = await guild.fetch_member(user_id)
        except Exception:
            return False
    guild_cfg = get_guild_config(guild.id)
    return has_mod_access(member, guild_cfg)


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
        # Interaction responses can expire on long-running slash commands.
        channel = getattr(ctx, "channel", None)
        if channel is None or not hasattr(channel, "send"):
            return None
        fallback_kwargs = dict(kwargs)
        fallback_kwargs.pop("ephemeral", None)
        try:
            return await channel.send(*args, **fallback_kwargs)
        except Exception:
            return None


def _user_cooldown_key(interaction: discord.Interaction) -> int:
    return interaction.user.id


async def _owner_app_command(interaction: discord.Interaction) -> bool:
    if await bot.is_owner(interaction.user):
        return True
    raise app_commands.CheckFailure("⛔ This command is owner-only.")


def _find_app_command(command_name: str):
    token = command_name.strip().lower()
    for command in bot.tree.walk_commands(type=discord.AppCommandType.chat_input):
        if command.qualified_name.lower() == token or command.name.lower() == token:
            return command
    return None


def _format_app_command_usage(command) -> str:
    parts = []
    for parameter in getattr(command, "parameters", []):
        display_name = getattr(parameter, "display_name", parameter.name)
        parts.append(f"<{display_name}>" if parameter.required else f"[{display_name}]")
    suffix = f" {' '.join(parts)}" if parts else ""
    return f"/{command.qualified_name}{suffix}"


ACCOUNT_INSTALL_COMMAND_NAMES = {
    "help",
    "status",
    "privacy",
    "tos",
    "avatar",
    "banner",
    "userinfo",
    "continentalid",
    "installcontext",
    "mutualservers",
    "choose",
    "roll",
    "poll",
    "remindme",
    "reminders",
    "cancelreminder",
    "ai",
    "aireset",
    "flaguser",
    "unflaguser",
    "owneronly",
}

GUILD_ONLY_COMMAND_NAMES = {
    "guard",
    "guardadvanced",
    "guardstatus",
    "guardreset",
    "votecreate",
    "voteaction",
    "startelection",
    "voteclose",
    "voteextend",
    "voteconfig",
    "setlogchannel",
    "setopschannel",
    "ops",
    "controlcenter",
    "purge",
    "slowmode",
    "nick",
    "timeout",
    "untimeout",
    "warn",
    "cases",
    "undo",
    "serverinfo",
    "lockdown",
    "unlock",
    "setwelcomechannel",
    "setwelcomerole",
    "setwelcomemessage",
    "setlockdownrole",
    "setmodroles",
    "showconfig",
    "voteinfo",
    "activevotes",
}

PERSONAL_HELP_COMMANDS = (
    "help",
    "installcontext",
    "status",
    "avatar",
    "banner",
    "userinfo",
    "continentalid",
    "mutualservers",
    "choose",
    "roll",
    "poll",
    "remindme",
    "reminders",
    "cancelreminder",
    "ai",
    "aireset",
    "privacy",
    "tos",
)

SERVER_HELP_COMMANDS = (
    "ops",
    "voteinfo",
    "activevotes",
    "voteconfig",
    "serverinfo",
    "lockdown",
    "unlock",
    "purge",
    "slowmode",
    "nick",
    "timeout",
    "untimeout",
    "warn",
    "cases",
    "undo",
    "guard",
    "guardadvanced",
    "guardstatus",
    "guardreset",
    "votecreate",
    "voteaction",
    "startelection",
    "voteextend",
    "voteclose",
    "showconfig",
    "setwelcomechannel",
    "setwelcomerole",
    "setwelcomemessage",
    "setlockdownrole",
    "setmodroles",
    "setlogchannel",
    "setopschannel",
    "controlcenter",
)


def _format_command_list(command_names: tuple[str, ...]) -> str:
    return " ".join(f"`{name}`" for name in command_names)


def describe_interaction_install_type(interaction: discord.Interaction | None) -> str:
    if interaction is None:
        return "Unknown"
    if interaction.is_guild_integration() and interaction.is_user_integration():
        return "Guild + User Install"
    if interaction.is_user_integration():
        return "User Install"
    if interaction.is_guild_integration():
        return "Guild Install"
    return "Unknown"


def find_mutual_guilds(
    guilds: list[discord.Guild] | tuple[discord.Guild, ...],
    user_id: int,
) -> list[discord.Guild]:
    shared: list[discord.Guild] = []
    for guild in guilds:
        if guild.get_member(user_id) is not None:
            shared.append(guild)
    return shared


def configure_app_command_visibility(tree: app_commands.CommandTree) -> None:
    unclassified: list[str] = []
    for command in tree.walk_commands(type=discord.AppCommandType.chat_input):
        command.guild_only = False
        if command.name in ACCOUNT_INSTALL_COMMAND_NAMES:
            command.allowed_installs = app_commands.AppInstallationType(guild=True, user=True)
            command.allowed_contexts = app_commands.AppCommandContext(
                guild=True,
                dm_channel=True,
                private_channel=True,
            )
            continue
        if command.name in GUILD_ONLY_COMMAND_NAMES:
            command.allowed_installs = app_commands.AppInstallationType(guild=True, user=False)
            command.allowed_contexts = app_commands.AppCommandContext(
                guild=True,
                dm_channel=False,
                private_channel=False,
            )
            continue
        unclassified.append(command.name)

    if unclassified:
        names = ", ".join(sorted(unclassified))
        raise RuntimeError(f"Unclassified app command install visibility: {names}")


def resolve_display_name(target: discord.User | discord.Member) -> str:
    if isinstance(target, discord.Member):
        return target.display_name
    return target.global_name or target.name


class VanguardCommandTree(app_commands.CommandTree):
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if settings.get("owner_only", False) and not await bot.is_owner(interaction.user):
            raise app_commands.CheckFailure("⛔ This Vanguard instance is currently owner-only.")

        block_reason = get_access_block_reason()
        if block_reason:
            raise app_commands.CheckFailure(
                f"⛔ This Vanguard instance is not authorized: {block_reason}"
            )

        if interaction.guild_id and not is_guild_authorized(interaction.guild_id):
            raise app_commands.CheckFailure(
                "⛔ This server is not authorized to use this Vanguard instance."
            )

        return True


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
    tree_cls=VanguardCommandTree,
    intents=intents,
    help_command=None,
    case_insensitive=True,
)
setup_vote_module(
    bot,
    get_guild_config=get_guild_config,
    save_settings=save_settings,
    send_ops_log=send_ops_log,
    resolve_guard_preset_name=resolve_guard_preset_name,
    apply_guard_preset=apply_guard_preset,
)
setup_guard_module(
    bot,
    require_mod_context=require_mod_context,
    save_settings=save_settings,
)

startup_initialized = False
reminder_loop_task: asyncio.Task | None = None
license_loop_task: asyncio.Task | None = None
control_center_runner: object | None = None
license_authorized = not VANGUARD_REQUIRE_LICENSE
license_reason = "license checks disabled"
license_allowed_guild_ids: set[int] = set()
license_last_checked_at: datetime | None = None
license_entitlements = normalize_license_entitlements({})


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
    global startup_initialized, reminder_loop_task, license_loop_task, control_center_runner
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

    # Remove guild-scoped command copies so only global commands remain.
    for guild in bot.guilds:
        try:
            bot.tree.clear_commands(guild=guild)
            await bot.tree.sync(guild=guild)
            print(f"[SYNC] Guild {guild.id} cleared stale guild-scoped commands")
        except Exception as exc:
            print(f"[SYNC] Guild {guild.id} sync failed: {exc}")

    if reminder_loop_task is None or reminder_loop_task.done():
        reminder_loop_task = asyncio.create_task(reminder_worker())
    if (
        VANGUARD_LICENSE_VERIFY_URL
        or VANGUARD_REQUIRE_LICENSE
        or VANGUARD_ALLOWED_GUILD_IDS
    ) and (license_loop_task is None or license_loop_task.done()):
        license_loop_task = asyncio.create_task(license_worker())
    if VANGUARD_CONTROL_CENTER_ENABLED and control_center_runner is None:
        try:
            control_center_app = create_control_center_app(
                bot=bot,
                get_guild_config=get_guild_config,
                save_settings=save_settings,
                normalize_guard_settings=normalize_guard_settings,
                resolve_guard_preset_name=resolve_guard_preset_name,
                apply_guard_preset=apply_guard_preset,
                guard_runtime_stats=guard_runtime_stats,
                reminders=reminders,
                modlog=modlog,
                vote_store=vote_store,
                parse_datetime_utc=parse_datetime_utc,
                http_request=http_request,
                can_access_guild=can_user_manage_control_center_guild,
                fetch_continental_profile=fetch_continental_me_sync,
                get_license_state=get_control_center_license_state,
                continental_login_url=CONTINENTAL_ID_LOGIN_URL,
                continental_dashboard_url=CONTINENTAL_ID_DASHBOARD_URL,
                public_url=VANGUARD_CONTROL_CENTER_PUBLIC_URL,
                site_host=VANGUARD_CONTROL_CENTER_HOST,
                site_port=VANGUARD_CONTROL_CENTER_PORT,
                static_dir=CONTROL_CENTER_STATIC_DIR,
                landing_dir=LANDING_SITE_DIR,
            )
            control_center_runner = await start_control_center_site(
                control_center_app,
                VANGUARD_CONTROL_CENTER_HOST,
                VANGUARD_CONTROL_CENTER_PORT,
            )
            print(f"[CONTROL] Control center running at {get_control_center_url()}/")
            if not continental_id_auth_configured():
                print(
                    "[CONTROL] Continental ID website auth is not configured. "
                    "Set CONTINENTAL_ID_AUTH_BASE_URL and CONTINENTAL_ID_LOGIN_URL."
                )
        except Exception as exc:
            print(f"[CONTROL] Failed to start control center: {exc}")

    startup_initialized = True


@bot.event
async def on_guild_join(guild: discord.Guild):
    authorized = is_guild_authorized(guild.id)
    await notify_personal_account_guild_join(guild, authorized=authorized)
    if authorized:
        return
    try:
        await guild.leave()
        print(f"[ACCESS] Left unauthorized guild on join: {guild.id} ({guild.name})")
    except Exception as exc:
        print(f"[ACCESS] Failed leaving unauthorized guild on join {guild.id}: {exc}")


@bot.event
async def on_member_join(member: discord.Member):
    guild_cfg = get_guild_config(member.guild.id)
    await handle_guard_member_join(
        bot=bot,
        member=member,
        guild_cfg=guild_cfg,
        log_moderation_action=log_moderation_action,
        send_ops_log=send_ops_log,
    )

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
        guild_cfg = get_guild_config(message.guild.id)
        await handle_guard_message(
            bot=bot,
            message=message,
            guild_cfg=guild_cfg,
            log_moderation_action=log_moderation_action,
            send_ops_log=send_ops_log,
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
    handled = False

    if isinstance(error, app_commands.MissingPermissions):
        missing = ", ".join(error.missing_permissions)
        message = f"⛔ Missing required permissions: `{missing}`."
        handled = True
    elif isinstance(error, app_commands.BotMissingPermissions):
        missing = ", ".join(error.missing_permissions)
        message = f"⛔ I am missing permissions: `{missing}`."
        handled = True
    elif isinstance(error, app_commands.CommandOnCooldown):
        message = f"⏳ Slow down. Try again in `{error.retry_after:.1f}` seconds."
        handled = True
    elif isinstance(error, app_commands.CheckFailure):
        message = str(error) or "⛔ You do not have permission to run this command."
        handled = True
    elif isinstance(error, app_commands.TransformerError):
        if is_channel_transform_error(error):
            message = "⚠️ Invalid channel type for that option. Choose a normal text channel."
        else:
            message = "⚠️ Invalid argument. Check the command format and try again."
        handled = True
    elif isinstance(err, commands.NotOwner):
        message = "⛔ This command is owner-only."
        handled = True

    if not handled:
        print("[ERROR] Unhandled app command error:")
        traceback.print_exception(type(err), err, err.__traceback__)

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
        pass


@bot.tree.command(name="help")
async def help_command(ctx: commands.Context, *, command_name: str | None = None):
    """Show help for all commands or a specific command."""
    ctx = await commands.Context.from_interaction(ctx)
    if command_name:
        command = _find_app_command(command_name)
        if command is None:
            await ctx.send("❌ Command not found.")
            return

        embed = discord.Embed(title=f"Help: {command.qualified_name}", color=discord.Color.red())
        embed.add_field(
            name="Description",
            value=getattr(command, "description", "") or "No description.",
            inline=False,
        )
        embed.add_field(name="Usage", value=f"`{_format_app_command_usage(command)}`", inline=False)
        await ctx.send(embed=embed)
        return

    embed = discord.Embed(title="Vanguard Bot Help", color=discord.Color.red())
    install_type = describe_interaction_install_type(ctx.interaction)
    source = ctx.guild.name if ctx.guild else "Direct Message"
    embed.description = "Slash commands only. Use `/help <command>` for detailed help."
    embed.add_field(name="Current Context", value=f"{install_type} in `{source}`", inline=False)
    embed.add_field(
        name="Personal / User Install",
        value=_format_command_list(PERSONAL_HELP_COMMANDS),
        inline=False,
    )
    embed.add_field(
        name="Server Only",
        value=_format_command_list(SERVER_HELP_COMMANDS),
        inline=False,
    )
    embed.add_field(
        name="Legal",
        value="`privacy` `tos`",
        inline=False,
    )
    embed.add_field(
        name="Owner",
        value="`owneronly` `flaguser` `unflaguser`",
        inline=False,
    )
    await ctx.send(embed=embed)

@bot.tree.command(name="setlogchannel")
async def setlogchannel(ctx: commands.Context, channel: ConfigChannelInput | None = None):
    """Set moderation log channel. Omit to clear."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="setopschannel")
async def setopschannel(ctx: commands.Context, channel: ConfigChannelInput | None = None):
    """Set ops/alerts channel. Omit to clear."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="ops")
async def ops(ctx: commands.Context):
    """Operational intelligence summary for this server."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="status")
async def status(ctx: commands.Context):
    """Runtime status, dependency checks, and key bot metrics."""
    ctx = await commands.Context.from_interaction(ctx)
    await safe_ctx_defer(ctx)
    checks: list[tuple[str, str]] = []
    checks.append(("Discord", "OK"))
    checks.append(("Latency", f"{round(bot.latency * 1000)}ms"))
    checks.append(("Uptime", format_duration(int((datetime.now(timezone.utc) - START_TIME).total_seconds()))))
    checks.append(("Guilds", str(len(bot.guilds))))
    unique_user_count = len({member.id for guild in bot.guilds for member in guild.members})
    checks.append(("Unique Users", str(unique_user_count)))
    checks.append(("Settings File", "OK" if os.path.exists(SETTINGS_FILE) else "MISSING"))
    checks.append(("Reminders File", "OK" if os.path.exists(REMINDERS_FILE) else "MISSING"))
    checks.append(("Mod Log File", "OK" if os.path.exists(MOD_LOG_FILE) else "MISSING"))

    ai_status = "DISABLED"
    model_status = "N/A"
    backend_headers = build_backend_headers()
    if AI_HEALTH_URL:
        try:
            response = await asyncio.to_thread(
                http_request,
                "GET",
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
                http_request,
                "GET",
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
    continental_status = "DISABLED"
    resolve_status = "ON" if CONTINENTAL_ID_RESOLVE_URL else "OFF"
    if CONTINENTAL_ID_HEALTH_URL:
        try:
            continental_response = await asyncio.to_thread(
                http_request,
                "GET",
                CONTINENTAL_ID_HEALTH_URL,
                headers=backend_headers or None,
                timeout=4,
            )
            if continental_response.status_code == 200:
                continental_status = "OK"
            else:
                continental_status = f"HTTP {continental_response.status_code}"
        except Exception:
            continental_status = "UNREACHABLE"
    elif continental_id_configured():
        continental_status = "CONFIGURED"
    checks.append(("Continental ID", continental_status))
    checks.append(("Resolve API", resolve_status))
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
    if VANGUARD_CONTROL_CENTER_ENABLED:
        control_status = (
            get_control_center_url()
            if control_center_runner is not None
            else "CONFIGURED (waiting for bot ready)"
        )
        if not continental_id_auth_configured():
            control_status = "MISCONFIGURED (missing Continental ID auth)"
    else:
        control_status = "DISABLED"
    checks.append(("Control Center", control_status))

    embed = discord.Embed(title="Vanguard Status", color=discord.Color.red())
    for key, value in checks:
        embed.add_field(name=key, value=value, inline=True)
    embed.set_footer(text=f"Generated at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    await ctx.send(embed=embed)


@bot.tree.command(name="privacy")
async def privacy(ctx: commands.Context):
    """Show data handling summary and Privacy Policy link."""
    ctx = await commands.Context.from_interaction(ctx)
    summary = (
        "I store server config, moderation cases, reminders, and vote state to operate features. "
        "Use `/tos` for terms."
    )
    if PRIVACY_URL:
        await ctx.send(f"{summary}\nPrivacy Policy: {PRIVACY_URL}")
    else:
        await ctx.send(f"{summary}\nPrivacy policy link not configured. Set `PRIVACY_POLICY_URL` in `.env`.")


@bot.tree.command(name="tos")
async def tos(ctx: commands.Context):
    """Show Terms of Service link."""
    ctx = await commands.Context.from_interaction(ctx)
    if TOS_URL:
        await ctx.send(f"Terms of Service: {TOS_URL}")
    else:
        await ctx.send("ToS link not configured. Set `TERMS_OF_SERVICE_URL` in `.env`.")


@bot.tree.command(name="controlcenter")
async def controlcenter(ctx: commands.Context):
    """Show the configured control center URL for moderators."""
    ctx = await commands.Context.from_interaction(ctx)
    result = await require_mod_context(ctx)
    if not result:
        return
    if not VANGUARD_CONTROL_CENTER_ENABLED:
        await ctx.send("⚠️ Control center is disabled on this Vanguard instance.")
        return
    message = [
        "Open the Vanguard control center here:",
        f"{get_control_center_url()}",
    ]
    if not continental_id_auth_configured():
        message.append(
            "Website auth is not configured on this instance yet. Set `CONTINENTAL_ID_AUTH_BASE_URL` "
            "and `CONTINENTAL_ID_LOGIN_URL` in `.env`."
        )
    else:
        message.append(
            "Sign in with Continental ID to use the website. That Continental account must have "
            "Discord linked, and only servers your linked Discord account can moderate will appear."
        )
    await ctx.send("\n".join(message))


@bot.tree.command()
async def avatar(ctx: commands.Context, user: discord.User | None = None):
    """Show a user's avatar."""
    ctx = await commands.Context.from_interaction(ctx)
    target = user or ctx.author
    if not isinstance(target, (discord.Member, discord.User)):
        await ctx.send("❌ Could not resolve user.")
        return
    embed = discord.Embed(title=f"Avatar: {resolve_display_name(target)}", color=discord.Color.red())
    embed.set_image(url=target.display_avatar.url)
    await ctx.send(embed=embed)


@bot.tree.command()
async def banner(ctx: commands.Context, user: discord.User | None = None):
    """Show a user's banner if they have one."""
    ctx = await commands.Context.from_interaction(ctx)
    target = user or ctx.author
    try:
        fetched_user = await bot.fetch_user(target.id)
    except (discord.NotFound, discord.HTTPException):
        await ctx.send("❌ Could not load that user's profile.")
        return

    accent = fetched_user.accent_color or discord.Color.red()
    embed = discord.Embed(title=f"Banner: {resolve_display_name(fetched_user)}", color=accent)
    if fetched_user.banner:
        embed.set_image(url=fetched_user.banner.url)
    else:
        embed.description = "This user does not have a profile banner set."
    if fetched_user.accent_color:
        embed.add_field(name="Accent Color", value=str(fetched_user.accent_color), inline=True)
    embed.set_thumbnail(url=fetched_user.display_avatar.url)
    await ctx.send(embed=embed)


@bot.tree.command()
async def userinfo(ctx: commands.Context, user: discord.User | None = None):
    """Show account details, with server-specific details when used in a server."""
    ctx = await commands.Context.from_interaction(ctx)
    target = user or ctx.author
    if not isinstance(target, (discord.Member, discord.User)):
        await ctx.send("❌ Could not resolve user.")
        return

    guild_member: discord.Member | None = None
    if ctx.guild is not None:
        guild_member = ctx.guild.get_member(target.id)
        if guild_member is None:
            try:
                guild_member = await ctx.guild.fetch_member(target.id)
            except (discord.NotFound, discord.HTTPException):
                guild_member = None

    embed = discord.Embed(title=f"User Info: {resolve_display_name(target)}", color=discord.Color.red())
    embed.add_field(name="User ID", value=str(target.id), inline=True)
    embed.add_field(name="Account Created", value=target.created_at.strftime("%Y-%m-%d %H:%M:%S"), inline=True)
    embed.add_field(
        name="Profile",
        value=f"Mention: {target.mention}\nBot: {'YES' if target.bot else 'NO'}",
        inline=False,
    )
    if guild_member is not None and ctx.guild is not None:
        roles = [role.mention for role in guild_member.roles if role != ctx.guild.default_role]
        embed.add_field(
            name="Joined Server",
            value=guild_member.joined_at.strftime("%Y-%m-%d %H:%M:%S") if guild_member.joined_at else "Unknown",
            inline=True,
        )
        embed.add_field(name="Top Role", value=guild_member.top_role.mention if guild_member.top_role else "None", inline=True)
        embed.add_field(name="Roles", value=", ".join(roles[-10:]) if roles else "None", inline=False)
    elif ctx.guild is not None:
        embed.add_field(name="Server Membership", value="User is not available as a member in this server cache.", inline=False)
    embed.set_thumbnail(url=target.display_avatar.url)
    await ctx.send(embed=embed)


@bot.tree.command(name="continentalid")
@app_commands.checks.cooldown(3, 30.0, key=_user_cooldown_key)
async def continentalid(ctx: commands.Context, user: discord.User | None = None):
    """Show Continental ID link status for yourself or, with Manage Server, another member."""
    ctx = await commands.Context.from_interaction(ctx)
    await safe_ctx_defer(ctx)
    target = user or ctx.author
    target_id = getattr(target, "id", None)
    if target_id is None:
        await safe_ctx_send(ctx, "❌ Could not resolve user.")
        return

    viewer_is_privileged = await bot.is_owner(ctx.author)
    if not viewer_is_privileged and ctx.guild and isinstance(ctx.author, discord.Member):
        viewer_is_privileged = ctx.author.guild_permissions.manage_guild

    if getattr(target, "id", None) != ctx.author.id and not viewer_is_privileged:
        await safe_ctx_send(
            ctx,
            "⛔ You can only check your own Continental ID link unless you can manage this server.",
        )
        return

    result = await asyncio.to_thread(resolve_continental_user_sync, target_id)
    if not result.get("configured"):
        await safe_ctx_send(ctx, "Continental ID integration is not configured on this Vanguard instance.")
        return
    if not result.get("ok"):
        status_code = result.get("status_code")
        message = str(result.get("message") or "Unknown error")
        if status_code is None:
            await safe_ctx_send(ctx, f"⚠️ Continental ID lookup failed. `{message}`")
        else:
            await safe_ctx_send(
                ctx,
                f"⚠️ Continental ID lookup failed (HTTP {status_code}). `{message}`",
            )
        return

    payload = result.get("body", {})
    user_payload = payload.get("user") if isinstance(payload, dict) else {}
    flags = payload.get("flags") if isinstance(payload, dict) else {}
    target_name = resolve_display_name(target)
    embed = discord.Embed(title=f"Continental ID: {target_name}", color=discord.Color.red())
    if hasattr(target, "display_avatar"):
        embed.set_thumbnail(url=target.display_avatar.url)

    if not bool(payload.get("linked")) or not isinstance(user_payload, dict):
        embed.description = (
            f"{target.mention} is not linked to Continental ID."
            if hasattr(target, "mention")
            else "This Discord account is not linked to Continental ID."
        )
        await ctx.send(embed=embed)
        return

    embed.add_field(name="Username", value=f"`@{user_payload.get('username') or 'unknown'}`", inline=True)
    embed.add_field(name="Display Name", value=str(user_payload.get("displayName") or "User"), inline=True)
    embed.add_field(name="Verified", value="YES" if user_payload.get("verified") else "NO", inline=True)

    if viewer_is_privileged:
        embed.add_field(name="Flags", value=format_continental_flags(flags), inline=False)
        embed.add_field(
            name="Discord User ID",
            value=f"`{user_payload.get('discordUserId') or target_id}`",
            inline=False,
        )
        embed.add_field(
            name="Linked At",
            value=format_continental_timestamp(user_payload.get("linkedAt")),
            inline=True,
        )
        embed.add_field(
            name="Last Used",
            value=format_continental_timestamp(user_payload.get("lastUsedAt")),
            inline=True,
        )
        flag_reason = str(flags.get("flagReason") or "").strip() if isinstance(flags, dict) else ""
        if flag_reason:
            embed.add_field(name="Flag Reason", value=flag_reason[:240], inline=False)

    await ctx.send(embed=embed)


@bot.tree.command(name="installcontext")
async def installcontext(ctx: commands.Context):
    """Show whether this command came from a user install or a server install."""
    ctx = await commands.Context.from_interaction(ctx)
    install_type = describe_interaction_install_type(ctx.interaction)
    source = ctx.guild.name if ctx.guild else "Direct Message"

    embed = discord.Embed(title="Vanguard Install Context", color=discord.Color.red())
    embed.add_field(name="Install Type", value=install_type, inline=True)
    embed.add_field(name="Source", value=source, inline=True)
    embed.add_field(
        name="Works In User Installs",
        value=_format_command_list(PERSONAL_HELP_COMMANDS),
        inline=False,
    )
    embed.add_field(
        name="Server-Only Commands",
        value="Server moderation, guard, vote management, and config commands stay guild-only.",
        inline=False,
    )
    await ctx.send(embed=embed)


@bot.tree.command(name="mutualservers")
async def mutualservers(ctx: commands.Context):
    """List servers shared between you and Vanguard."""
    ctx = await commands.Context.from_interaction(ctx)
    shared = find_mutual_guilds(list(bot.guilds), ctx.author.id)
    if not shared:
        await ctx.send("No shared servers found.")
        return

    lines = [f"`{guild.name}` • `{guild.id}`" for guild in shared[:15]]
    if len(shared) > 15:
        lines.append(f"...and `{len(shared) - 15}` more")
    await send_chunked_message(ctx, "**Shared servers with Vanguard:**\n" + "\n".join(lines))


@bot.tree.command()
async def choose(ctx: commands.Context, *, options: str):
    """Choose randomly from options separated by |."""
    ctx = await commands.Context.from_interaction(ctx)
    choices = [choice.strip() for choice in options.split("|") if choice.strip()]
    if len(choices) < 2:
        await ctx.send("⚠️ Provide at least 2 options separated by `|`.")
        return
    await ctx.send(f"🎲 I choose: **{random.choice(choices)}**")


@bot.tree.command()
async def roll(ctx: commands.Context, notation: str = "1d6"):
    """Roll dice using NdS notation, e.g. 2d20."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
async def poll(ctx: commands.Context, *, content: str):
    """Create a poll. Format: question | option1 | option2 ... (or yes/no with question only)."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
async def remindme(ctx: commands.Context, duration: str, *, message: str):
    """Create a reminder. Example: /remindme 2h30m stretch."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="reminders")
async def list_reminders(ctx: commands.Context):
    """List your active reminders."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="cancelreminder")
async def cancel_reminder(ctx: commands.Context, reminder_id: int):
    """Cancel one of your reminders by ID."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
@app_commands.checks.has_permissions(manage_messages=True)
async def purge(ctx: commands.Context, amount: int):
    """Delete recent messages in the current channel."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
@app_commands.checks.has_permissions(manage_channels=True)
async def slowmode(ctx: commands.Context, seconds: int):
    """Set channel slowmode in seconds (0 disables)."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
@app_commands.checks.has_permissions(manage_nicknames=True)
async def nick(ctx: commands.Context, member: discord.Member, *, nickname: str | None = None):
    """Change or clear a member nickname."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
@app_commands.checks.has_permissions(moderate_members=True)
async def timeout(ctx: commands.Context, member: discord.Member, duration: str, *, reason: str | None = None):
    """Timeout a member. Example: /timeout @user 30m spam"""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
@app_commands.checks.has_permissions(moderate_members=True)
async def untimeout(ctx: commands.Context, member: discord.Member, *, reason: str | None = None):
    """Remove a member timeout."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
@app_commands.checks.has_permissions(moderate_members=True)
async def warn(ctx: commands.Context, member: discord.Member, *, reason: str):
    """Issue a warning and record a moderation case."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="cases")
@app_commands.checks.has_permissions(moderate_members=True)
async def cases(ctx: commands.Context, member: discord.Member | None = None, limit: int = 10):
    """List recent moderation cases, optionally filtered by member."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="undo")
@app_commands.checks.has_permissions(moderate_members=True)
async def undo(ctx: commands.Context, case_id: int):
    """Undo a supported moderation action by case ID."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="ai")
@app_commands.checks.cooldown(3, 30.0, key=_user_cooldown_key)
async def ai(ctx: commands.Context, *, question: str):
    """Ask the AI server and keep channel-local memory for follow-up questions."""
    ctx = await commands.Context.from_interaction(ctx)
    if not await require_ai_access(ctx):
        return
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
        chat_timeout = False
        chat_unreachable = False
        try:
            chat_status_code: int | None = None
            try:
                chat_response = await asyncio.to_thread(
                    http_request,
                    "POST",
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
            except requests.exceptions.Timeout:
                chat_timeout = True
            except requests.exceptions.ConnectionError:
                chat_unreachable = True

            if not answer:
                try:
                    response = await asyncio.to_thread(
                        http_request,
                        "POST",
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
                        if chat_timeout:
                            title_text = "AI Response (chat fallback)"
                        elif chat_unreachable:
                            title_text = "AI Response (compatibility fallback)"
                    else:
                        status_code = chat_status_code if chat_status_code is not None else response.status_code
                        title_text = f"AI service returned HTTP {status_code}."
                        answer = response.text
                except requests.exceptions.Timeout:
                    if chat_timeout or chat_unreachable:
                        title_text = "AI service is currently unreachable."
                    else:
                        title_text = "AI compatibility endpoint timed out."
                except requests.exceptions.ConnectionError:
                    if chat_timeout or chat_unreachable:
                        title_text = "AI service is currently unreachable."
                    else:
                        title_text = "AI compatibility endpoint is unreachable."
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


@bot.tree.command(name="aireset")
@app_commands.checks.cooldown(3, 30.0, key=_user_cooldown_key)
async def aireset(ctx: commands.Context):
    """Clear your AI chat memory for this channel."""
    ctx = await commands.Context.from_interaction(ctx)
    if not await require_ai_access(ctx):
        return
    session_id = _build_ai_session_id(
        ctx.guild.id if ctx.guild else None,
        getattr(ctx.channel, "id", None),
        ctx.author.id,
    )
    delete_url = f"{AI_SESSION_URL}/{quote(session_id, safe='')}"
    backend_headers = build_backend_headers()
    try:
        response = await asyncio.to_thread(
            http_request,
            "DELETE",
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


@bot.tree.command(name="flaguser")
@app_commands.check(_owner_app_command)
async def flaguser(ctx: commands.Context, target: str):
    """Owner-only: mark a user in backend moderation service."""
    ctx = await commands.Context.from_interaction(ctx)
    await send_backend_user_update(ctx, target, FLAG_USER_URL, "has been flagged")


@bot.tree.command(name="unflaguser")
@app_commands.check(_owner_app_command)
async def unflaguser(ctx: commands.Context, target: str):
    """Owner-only: remove a backend moderation flag for a user."""
    ctx = await commands.Context.from_interaction(ctx)
    await send_backend_user_update(ctx, target, UNFLAG_USER_URL, "has been unflagged")


@bot.tree.command()
@app_commands.check(_owner_app_command)
async def owneronly(ctx: commands.Context, state: str | None = None):
    """Owner-only: toggle global owner-only mode. Usage: /owneronly on|off"""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
async def serverinfo(ctx: commands.Context):
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command()
async def lockdown(ctx: commands.Context):
    ctx = await commands.Context.from_interaction(ctx)
    await set_lockdown_state(ctx, True)


@bot.tree.command()
async def unlock(ctx: commands.Context):
    ctx = await commands.Context.from_interaction(ctx)
    await set_lockdown_state(ctx, False)


@bot.tree.command(name="setwelcomechannel")
async def setwelcomechannel(ctx: commands.Context, channel: ConfigChannelInput | None = None):
    """Mod/admin: set welcome channel. Omit to clear."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="setwelcomerole")
async def setwelcomerole(ctx: commands.Context, role: discord.Role | None = None):
    """Mod/admin: set role to auto-assign on join. Omit to clear."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="setwelcomemessage")
async def setwelcomemessage(ctx: commands.Context, *, message: str | None = None):
    """Mod/admin: set welcome message. Supports {user}, {username}, {server}. Use `clear` to reset."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="setlockdownrole")
async def setlockdownrole(ctx: commands.Context, role: discord.Role | None = None):
    """Mod/admin: set role targeted by lockdown. Omit to use @everyone."""
    ctx = await commands.Context.from_interaction(ctx)
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


@bot.tree.command(name="setmodroles")
async def setmodroles(ctx: commands.Context, roles: str | None = None):
    """Mod/admin: set additional roles allowed to run moderation/config commands."""
    ctx = await commands.Context.from_interaction(ctx)
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

@bot.tree.command(name="showconfig")
async def showconfig(ctx: commands.Context):
    """Show active configuration for this server."""
    ctx = await commands.Context.from_interaction(ctx)
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

    guard_cfg = normalize_guard_settings(guild_cfg)

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
        name="Guard",
        value=(
            f"enabled={guard_cfg['guard_enabled']}, "
            f"threshold={guard_cfg['guard_threshold']}/{guard_cfg['guard_window_seconds']}s, "
            f"cooldown={guard_cfg['guard_cooldown_seconds']}s, "
            f"scope={guard_cfg['guard_slowmode_scope']}"
        ),
        inline=False,
    )
    await ctx.send(embed=embed)


@bot.tree.command(name="voteinfo")
async def voteinfo(ctx: commands.Context, vote_id: str):
    """Show detailed state for a specific vote."""
    ctx = await commands.Context.from_interaction(ctx)
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
    execution_action = vote.get("execution_action")
    if isinstance(execution_action, dict):
        action_type = str(execution_action.get("type") or "").strip()
        if action_type:
            header_lines.append(f"On pass: `{action_type}`")
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


@bot.tree.command(name="activevotes")
async def activevotes(ctx: commands.Context):
    """List active votes in this server."""
    ctx = await commands.Context.from_interaction(ctx)
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


configure_app_command_visibility(bot.tree)


if __name__ == "__main__":
    DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
    if not DISCORD_TOKEN:
        raise RuntimeError("Missing DISCORD_BOT_TOKEN environment variable.")

    bot.run(DISCORD_TOKEN)
