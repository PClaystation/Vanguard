# Copyright (c) 2026 Continental. All rights reserved.
# Licensed under the Vanguard Proprietary Source-Available License (see /LICENSE).

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
from typing import Any, Awaitable, Callable, Mapping, Sequence

from aiohttp import web
import discord

from guard import GUARD_PRESETS, guard_default_settings, normalize_guard_settings as normalize_guard_profile

SESSION_COOKIE = "vanguard_control_session"
CONTROL_CENTER_PATH = "/control"
CONTROL_CENTER_API_PATH = f"{CONTROL_CENTER_PATH}/api"
CONTROL_CENTER_STATIC_PATH = f"{CONTROL_CENTER_PATH}/static"
CONTROL_CENTER_AUTH_PATH = f"{CONTROL_CENTER_PATH}/auth"


def _normalize_control_base(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    if normalized.endswith(CONTROL_CENTER_PATH):
        return normalized
    return f"{normalized}{CONTROL_CENTER_PATH}"


def build_control_center_url(host: str, port: int, public_url: str = "") -> str:
    explicit = public_url.strip().rstrip("/")
    if explicit:
        return _normalize_control_base(explicit)
    normalized_host = host.strip() or "127.0.0.1"
    if normalized_host in {"0.0.0.0", "::"}:
        normalized_host = "localhost"
    return f"http://{normalized_host}:{port}{CONTROL_CENTER_PATH}"


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "" or value == "null":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _serialize_snowflake(value: Any) -> str | None:
    snowflake = _coerce_optional_int(value)
    return str(snowflake) if snowflake is not None else None


def _serialize_snowflake_list(values: Any) -> list[str]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        return []
    serialized: list[str] = []
    seen: set[str] = set()
    for value in values:
        snowflake = _serialize_snowflake(value)
        if snowflake is None or snowflake in seen:
            continue
        seen.add(snowflake)
        serialized.append(snowflake)
    return serialized


def _coerce_role_id_list(value: Any) -> list[int] | None:
    if value is None or value == "" or value == "null":
        return []
    if not isinstance(value, list):
        return None
    role_ids: list[int] = []
    seen: set[int] = set()
    for item in value:
        role_id = _coerce_optional_int(item)
        if role_id is None or role_id in seen:
            continue
        seen.add(role_id)
        role_ids.append(role_id)
    return role_ids


def _is_text_channel(channel: Any) -> bool:
    if isinstance(channel, discord.TextChannel):
        return True
    return str(getattr(channel, "type", "")).lower() == "text"


def _safe_icon_url(guild: discord.Guild) -> str | None:
    icon = getattr(guild, "icon", None)
    url = getattr(icon, "url", None)
    return str(url) if url else None


def _serialize_channel(channel: Any) -> dict[str, Any]:
    return {
        "id": str(int(getattr(channel, "id"))),
        "name": str(getattr(channel, "name", "unknown")),
        "mention": str(getattr(channel, "mention", f"<#{getattr(channel, 'id', 0)}>")),
        "position": int(getattr(channel, "position", 0)),
    }


def _serialize_role(role: Any) -> dict[str, Any]:
    color = getattr(role, "color", None)
    color_value = getattr(color, "value", 0)
    return {
        "id": str(int(getattr(role, "id"))),
        "name": str(getattr(role, "name", "unknown")),
        "mention": str(getattr(role, "mention", f"<@&{getattr(role, 'id', 0)}>")),
        "position": int(getattr(role, "position", 0)),
        "color": int(color_value),
    }


def _match_guard_preset_name(guard_cfg: Mapping[str, Any]) -> str:
    for preset_name, preset_values in GUARD_PRESETS.items():
        candidate = guard_default_settings()
        candidate.update(preset_values)
        normalized_candidate = normalize_guard_profile(candidate)
        if all(guard_cfg.get(key) == value for key, value in normalized_candidate.items()):
            return preset_name
    return "custom"


def _serialize_runtime_stats(raw_stats: Mapping[str, Any] | None) -> dict[str, Any]:
    stats = dict(raw_stats or {})
    last_trigger_at = stats.get("last_trigger_at")
    if isinstance(last_trigger_at, datetime):
        last_trigger_text = last_trigger_at.astimezone(timezone.utc).isoformat()
    elif isinstance(last_trigger_at, str):
        last_trigger_text = last_trigger_at
    else:
        last_trigger_text = None

    return {
        "triggers_total": int(stats.get("triggers_total", 0) or 0),
        "suppressed_total": int(stats.get("suppressed_total", 0) or 0),
        "last_trigger_at": last_trigger_text,
        "last_trigger_reasons": list(stats.get("last_trigger_reasons", []) or []),
        "last_trigger_severity": str(stats.get("last_trigger_severity") or "none"),
        "last_trigger_actor_id": _serialize_snowflake(stats.get("last_trigger_actor_id")),
    }


def _count_active_votes(guild_id: int, vote_store: Mapping[str, Any]) -> int:
    prefix = f"{guild_id}-"
    return sum(1 for vote_id in vote_store if str(vote_id).startswith(prefix))


def _count_pending_reminders(
    guild_id: int,
    reminders: Sequence[Mapping[str, Any]],
    parse_datetime_utc: Callable[[Any], datetime | None],
) -> int:
    now = datetime.now(timezone.utc)
    total = 0
    for reminder in reminders:
        if reminder.get("guild_id") != guild_id:
            continue
        due_at = parse_datetime_utc(reminder.get("due_at"))
        if due_at and due_at > now:
            total += 1
    return total


def _count_recent_cases(
    guild_id: int,
    modlog: Mapping[str, Sequence[Mapping[str, Any]]],
    parse_datetime_utc: Callable[[Any], datetime | None],
) -> int:
    entries = modlog.get(str(guild_id), [])
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    total = 0
    for entry in entries:
        created_at = parse_datetime_utc(entry.get("created_at"))
        if created_at and created_at >= cutoff:
            total += 1
    return total


def build_guild_overview(
    guild: discord.Guild,
    guild_cfg: Mapping[str, Any],
    *,
    guard_runtime_stats: Mapping[int, Mapping[str, Any]],
    reminders: Sequence[Mapping[str, Any]],
    modlog: Mapping[str, Sequence[Mapping[str, Any]]],
    vote_store: Mapping[str, Any],
    parse_datetime_utc: Callable[[Any], datetime | None],
) -> dict[str, Any]:
    runtime_stats = _serialize_runtime_stats(guard_runtime_stats.get(guild.id))
    return {
        "id": str(guild.id),
        "name": guild.name,
        "icon_url": _safe_icon_url(guild),
        "member_count": int(getattr(guild, "member_count", 0) or 0),
        "guard_enabled": bool(guild_cfg.get("guard_enabled")),
        "guard_preset": _match_guard_preset_name(guild_cfg),
        "active_votes": _count_active_votes(guild.id, vote_store),
        "pending_reminders": _count_pending_reminders(guild.id, reminders, parse_datetime_utc),
        "recent_cases_24h": _count_recent_cases(guild.id, modlog, parse_datetime_utc),
        "runtime_stats": runtime_stats,
    }


def build_guild_detail(
    guild: discord.Guild,
    guild_cfg: Mapping[str, Any],
    *,
    guard_runtime_stats: Mapping[int, Mapping[str, Any]],
    reminders: Sequence[Mapping[str, Any]],
    modlog: Mapping[str, Sequence[Mapping[str, Any]]],
    vote_store: Mapping[str, Any],
    parse_datetime_utc: Callable[[Any], datetime | None],
    normalize_guard_settings: Callable[[Any], dict[str, Any]],
) -> dict[str, Any]:
    overview = build_guild_overview(
        guild,
        guild_cfg,
        guard_runtime_stats=guard_runtime_stats,
        reminders=reminders,
        modlog=modlog,
        vote_store=vote_store,
        parse_datetime_utc=parse_datetime_utc,
    )
    normalized_guard = normalize_guard_settings(guild_cfg)
    text_channels = [
        _serialize_channel(channel)
        for channel in sorted(
            getattr(guild, "text_channels", []),
            key=lambda item: (int(getattr(item, "position", 0)), str(getattr(item, "name", "")).lower()),
        )
        if _is_text_channel(channel)
    ]
    roles = [
        _serialize_role(role)
        for role in sorted(
            getattr(guild, "roles", []),
            key=lambda item: (int(getattr(item, "position", 0)), str(getattr(item, "name", "")).lower()),
            reverse=True,
        )
        if not getattr(role, "is_default", lambda: False)()
    ]
    detail = dict(overview)
    detail["settings"] = {
        "welcome_channel_id": _serialize_snowflake(guild_cfg.get("welcome_channel_id")),
        "welcome_role_id": _serialize_snowflake(guild_cfg.get("welcome_role_id")),
        "welcome_message": str(guild_cfg.get("welcome_message") or ""),
        "ops_channel_id": _serialize_snowflake(guild_cfg.get("ops_channel_id")),
        "log_channel_id": _serialize_snowflake(guild_cfg.get("log_channel_id")),
        "lockdown_role_id": _serialize_snowflake(guild_cfg.get("lockdown_role_id")),
        "mod_role_ids": _serialize_snowflake_list(guild_cfg.get("mod_role_ids", [])),
        "guard_preset": _match_guard_preset_name(normalized_guard),
        "guard": normalized_guard,
    }
    detail["channels"] = text_channels
    detail["roles"] = roles
    return detail


def serialize_continental_status(result: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(result or {})
    body = payload.get("body")
    body_map = body if isinstance(body, Mapping) else {}
    raw_user = body_map.get("user")
    raw_flags = body_map.get("flags")
    user = raw_user if isinstance(raw_user, Mapping) else {}
    flags = raw_flags if isinstance(raw_flags, Mapping) else {}

    return {
        "configured": bool(payload.get("configured")),
        "ok": bool(payload.get("ok")),
        "linked": bool(payload.get("linked")),
        "message": str(payload.get("message") or ""),
        "user": {
            "continental_id": str(user.get("continentalId") or user.get("userId") or ""),
            "username": str(user.get("username") or ""),
            "display_name": str(user.get("displayName") or ""),
            "verified": bool(user.get("verified")),
            "discord_linked": bool(user.get("discordLinked")),
        },
        "flags": {
            "trusted": bool(flags.get("trusted")),
            "staff": bool(flags.get("staff")),
            "flagged": bool(flags.get("flagged")),
            "banned_from_ai": bool(flags.get("bannedFromAi")),
            "flag_reason": str(flags.get("flagReason") or ""),
        },
    }


def serialize_continental_account_user(user_payload: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(user_payload or {})
    vanguard = payload.get("vanguard")
    vanguard_state = vanguard if isinstance(vanguard, Mapping) else {}
    oauth_providers = payload.get("oauthProviders")
    oauth_state = oauth_providers if isinstance(oauth_providers, Mapping) else {}
    discord_oauth = oauth_state.get("discord")
    discord_state = discord_oauth if isinstance(discord_oauth, Mapping) else {}

    linked = bool(vanguard_state.get("linkedDiscord"))
    return {
        "configured": True,
        "ok": True,
        "linked": linked,
        "message": "",
        "user": {
            "continental_id": str(payload.get("continentalId") or payload.get("userId") or ""),
            "username": str(payload.get("username") or ""),
            "display_name": str(payload.get("displayName") or ""),
            "verified": bool(payload.get("isVerified")),
            "discord_linked": linked,
            "discord_user_id": str(vanguard_state.get("discordUserId") or ""),
            "avatar_url": str(payload.get("profile", {}).get("avatar") or "")
            if isinstance(payload.get("profile"), Mapping)
            else "",
            "discord_username": str(discord_state.get("username") or ""),
        },
        "flags": {
            "trusted": bool(vanguard_state.get("trusted")),
            "staff": bool(vanguard_state.get("staff")),
            "flagged": bool(vanguard_state.get("flagged")),
            "banned_from_ai": bool(vanguard_state.get("bannedFromAi")),
            "flag_reason": str(vanguard_state.get("flagReason") or ""),
        },
    }


def serialize_license_state(raw_state: Mapping[str, Any] | None) -> dict[str, Any]:
    state = dict(raw_state or {})
    raw_entitlements = state.get("entitlements")
    entitlements = raw_entitlements if isinstance(raw_entitlements, Mapping) else {}
    raw_guard_presets = entitlements.get("guard_presets", entitlements.get("guardPresets", []))
    guard_presets = [
        str(item).strip().lower()
        for item in raw_guard_presets
        if str(item).strip()
    ] if isinstance(raw_guard_presets, Sequence) and not isinstance(raw_guard_presets, (str, bytes)) else []
    allowed_guild_ids = sorted(
        {
            guild_id
            for guild_id in (_coerce_optional_int(value) for value in state.get("allowed_guild_ids", state.get("allowedGuildIds", [])) or [])
            if guild_id
        }
    )
    last_checked_at = state.get("last_checked_at", state.get("lastCheckedAt"))
    if isinstance(last_checked_at, datetime):
        last_checked_text = last_checked_at.astimezone(timezone.utc).isoformat()
    elif isinstance(last_checked_at, str):
        last_checked_text = last_checked_at
    else:
        last_checked_text = None

    configured = bool(state.get("configured"))
    required = bool(state.get("required"))
    authorized = bool(state.get("authorized")) if configured or required else True
    if required:
        mode = "required"
    elif configured:
        mode = "monitor"
    else:
        mode = "disabled"

    return {
        "configured": configured,
        "required": required,
        "authorized": authorized,
        "mode": mode,
        "reason": str(state.get("reason") or ""),
        "allowed_guild_ids": allowed_guild_ids,
        "allowed_guild_count": len(allowed_guild_ids),
        "entitlements": {
            "ai": bool(entitlements.get("ai")),
            "advanced_votes": bool(entitlements.get("advanced_votes", entitlements.get("advancedVotes"))),
            "guard_presets": guard_presets,
        },
        "last_checked_at": last_checked_text,
    }


def build_guild_authorization(guild_id: int, raw_license_state: Mapping[str, Any] | None) -> dict[str, Any]:
    license_state = serialize_license_state(raw_license_state)
    if license_state["required"] and not license_state["authorized"]:
        return {
            "authorized": False,
            "source": "license",
            "reason": license_state["reason"] or "This Vanguard instance is blocked by its required license check.",
        }

    allowed_guild_ids = set(license_state["allowed_guild_ids"])
    if allowed_guild_ids and guild_id not in allowed_guild_ids:
        return {
            "authorized": False,
            "source": "allowlist",
            "reason": "This guild is outside the current Vanguard allowlist.",
        }

    return {
        "authorized": True,
        "source": "default",
        "reason": "",
    }


def _guild_from_request(
    request: web.Request,
    bot: discord.Client,
) -> tuple[discord.Guild | None, web.StreamResponse | None]:
    auth = request["auth"]
    guild_id = _coerce_optional_int(request.match_info.get("guild_id"))
    guild = bot.get_guild(guild_id or 0)
    if guild is None:
        return None, web.json_response({"error": "Guild not found."}, status=404)
    if guild.id not in set(auth.get("manageable_guild_ids", set())):
        return None, web.json_response({"error": "Forbidden."}, status=403)
    return guild, None


def apply_guild_control_update(
    guild: discord.Guild,
    guild_cfg: dict[str, Any],
    payload: Any,
    *,
    normalize_guard_settings: Callable[[Any], dict[str, Any]],
    resolve_guard_preset_name: Callable[[str | None], str | None],
    apply_guard_preset: Callable[[dict[str, Any], str], bool],
) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {"payload": "Expected a JSON object."}

    errors: dict[str, str] = {}
    updates: dict[str, Any] = {}

    def parse_text_channel_id(field_name: str) -> None:
        if field_name not in payload:
            return
        raw_value = payload.get(field_name)
        if raw_value is None or raw_value == "" or raw_value == "null":
            updates[field_name] = None
            return
        channel_id = _coerce_optional_int(raw_value)
        if channel_id is None:
            errors[field_name] = "Expected a channel ID or null."
            return
        channel = guild.get_channel(channel_id)
        if channel is None or not _is_text_channel(channel):
            errors[field_name] = "Channel must be a text channel in this server."
            return
        updates[field_name] = channel_id

    def parse_role_id(field_name: str) -> None:
        if field_name not in payload:
            return
        raw_value = payload.get(field_name)
        if raw_value is None or raw_value == "" or raw_value == "null":
            updates[field_name] = None
            return
        role_id = _coerce_optional_int(raw_value)
        if role_id is None:
            errors[field_name] = "Expected a role ID or null."
            return
        role = guild.get_role(role_id)
        if role is None:
            errors[field_name] = "Role must exist in this server."
            return
        updates[field_name] = role_id

    parse_text_channel_id("welcome_channel_id")
    parse_text_channel_id("ops_channel_id")
    parse_text_channel_id("log_channel_id")
    parse_role_id("welcome_role_id")
    parse_role_id("lockdown_role_id")

    if "welcome_message" in payload:
        raw_message = payload.get("welcome_message")
        if raw_message is None or raw_message == "null":
            updates["welcome_message"] = None
        else:
            message = str(raw_message).strip()
            updates["welcome_message"] = message[:500] if message else None

    if "mod_role_ids" in payload:
        role_ids = _coerce_role_id_list(payload.get("mod_role_ids"))
        if role_ids is None:
            errors["mod_role_ids"] = "Expected a list of role IDs."
        else:
            missing = [role_id for role_id in role_ids if guild.get_role(role_id) is None]
            if missing:
                errors["mod_role_ids"] = "All mod roles must exist in this server."
            else:
                updates["mod_role_ids"] = role_ids

    guard_payload = payload.get("guard")
    if guard_payload is not None and not isinstance(guard_payload, dict):
        errors["guard"] = "Expected a guard settings object."

    preset_name: str | None = None
    if "guard_preset" in payload:
        raw_preset = str(payload.get("guard_preset") or "").strip().lower()
        if raw_preset and raw_preset != "custom":
            preset_name = resolve_guard_preset_name(raw_preset)
            if preset_name is None:
                errors["guard_preset"] = "Unknown guard preset."

    if errors:
        return errors

    guild_cfg.update(updates)

    guard_source = dict(guild_cfg)
    if preset_name:
        apply_guard_preset(guard_source, preset_name)
    if isinstance(guard_payload, dict):
        guard_source.update(guard_payload)
    guild_cfg.update(normalize_guard_settings(guard_source))
    return {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _cookie_secure(redirect_uri: str, public_url: str) -> bool:
    candidate = (public_url or redirect_uri or "").strip().lower()
    return candidate.startswith("https://")


def _public_site_base(public_url: str, host: str, port: int) -> str:
    explicit = public_url.strip().rstrip("/")
    if explicit:
        if explicit.endswith(CONTROL_CENTER_PATH):
            return explicit[: -len(CONTROL_CENTER_PATH)] or "/"
        return explicit
    normalized_host = host.strip() or "127.0.0.1"
    if normalized_host in {"0.0.0.0", "::"}:
        normalized_host = "localhost"
    return f"http://{normalized_host}:{port}"


def create_control_center_app(
    *,
    bot: discord.Client,
    get_guild_config: Callable[[int], dict[str, Any]],
    save_settings: Callable[[], bool | None],
    normalize_guard_settings: Callable[[Any], dict[str, Any]],
    resolve_guard_preset_name: Callable[[str | None], str | None],
    apply_guard_preset: Callable[[dict[str, Any], str], bool],
    guard_runtime_stats: Mapping[int, Mapping[str, Any]],
    reminders: Sequence[Mapping[str, Any]],
    modlog: Mapping[str, Sequence[Mapping[str, Any]]],
    vote_store: Mapping[str, Any],
    parse_datetime_utc: Callable[[Any], datetime | None],
    http_request: Callable[..., Any],
    can_access_guild: Callable[[discord.Guild, int], Awaitable[bool]],
    fetch_continental_profile: Callable[[str], Mapping[str, Any]] | None,
    get_license_state: Callable[[], Mapping[str, Any]] | None,
    continental_login_url: str,
    continental_dashboard_url: str,
    public_url: str,
    site_host: str,
    site_port: int,
    static_dir: str | Path,
    landing_dir: str | Path,
    trigger_lockdown_action: Callable[[discord.Guild, int, str, bool], Awaitable[tuple[bool, str]]] | None = None,
) -> web.Application:
    static_root = Path(static_dir)
    landing_root = Path(landing_dir)
    secure_cookie = _cookie_secure(public_url, "")
    continental_auth_enabled = bool(fetch_continental_profile and continental_login_url)
    site_base_url = _public_site_base(public_url, site_host, site_port)
    public_site_root = site_base_url.rstrip("/") or site_base_url
    content_security_policy = (
        "default-src 'self'; "
        "base-uri 'self'; "
        "object-src 'none'; "
        "frame-ancestors 'none'; "
        "form-action 'self'; "
        "img-src 'self' data: https:; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "connect-src 'self'; "
        "manifest-src 'self'"
    )
    sessions: dict[str, dict[str, Any]] = {}

    def current_license_state() -> dict[str, Any]:
        if get_license_state is None:
            return serialize_license_state(None)
        return serialize_license_state(get_license_state())

    def clear_session_token(token: str | None) -> None:
        if token:
            sessions.pop(token, None)

    def build_session_payload(continental: Mapping[str, Any], manageable_guild_ids: Sequence[int]) -> dict[str, Any]:
        user = continental.get("user")
        user_state = user if isinstance(user, Mapping) else {}
        return {
            "continental": dict(continental),
            "continental_id": str(user_state.get("continental_id") or ""),
            "discord_user_id": _coerce_optional_int(user_state.get("discord_user_id")) or 0,
            "username": str(user_state.get("display_name") or user_state.get("username") or "Continental user"),
            "avatar_url": str(user_state.get("avatar_url") or ""),
            "manageable_guild_ids": sorted(
                {
                    guild_id
                    for guild_id in (_coerce_optional_int(item) for item in manageable_guild_ids)
                    if guild_id
                }
            ),
            "expires_at": (_utc_now() + timedelta(days=7)).isoformat(),
        }

    async def get_session_from_cookie(request: web.Request) -> dict[str, Any] | None:
        session_token = request.cookies.get(SESSION_COOKIE)
        if not session_token:
            return None
        session_payload = sessions.get(session_token)
        if not session_payload:
            return None

        expires_at = parse_datetime_utc(session_payload.get("expires_at"))
        if expires_at is None:
            clear_session_token(session_token)
            return None

        if expires_at <= _utc_now():
            clear_session_token(session_token)
            return None

        request["session_token"] = session_token
        return session_payload

    async def build_manageable_guild_ids(discord_user_id: int) -> set[int]:
        manageable: set[int] = set()
        if discord_user_id <= 0:
            return manageable
        for guild in bot.guilds:
            if await can_access_guild(guild, discord_user_id):
                manageable.add(guild.id)
        return manageable

    async def exchange_continental_token(access_token: str) -> tuple[dict[str, Any] | None, str]:
        if fetch_continental_profile is None:
            return None, "Continental ID auth is not configured for this control center."
        result = await asyncio.to_thread(fetch_continental_profile, access_token)
        payload = dict(result or {})
        if not payload.get("ok"):
            return None, str(payload.get("message") or "Continental ID authentication failed.")

        user = payload.get("user")
        if not isinstance(user, Mapping):
            return None, "Continental ID did not return an account payload."

        continental = serialize_continental_account_user(user)
        if not continental["user"]["verified"]:
            return None, "Verify your Continental ID email before using the Vanguard control center."
        if not continental["linked"] or not continental["user"]["discord_user_id"]:
            return (
                None,
                "Your Continental ID account must have Discord linked before it can access the Vanguard control center.",
            )

        discord_user_id = _coerce_optional_int(continental["user"]["discord_user_id"]) or 0
        manageable_guild_ids = await build_manageable_guild_ids(discord_user_id)
        if not manageable_guild_ids:
            return None, "Your linked Discord account does not currently have Vanguard control access in any server."

        session_payload = build_session_payload(continental, sorted(manageable_guild_ids))
        return session_payload, ""

    async def get_request_auth(request: web.Request) -> dict[str, Any] | None:
        session_payload = await get_session_from_cookie(request)
        if not session_payload:
            return None

        discord_user_id = _coerce_optional_int(session_payload.get("discord_user_id")) or 0
        manageable = await build_manageable_guild_ids(discord_user_id)
        continental = session_payload.get("continental")
        continental_state = continental if isinstance(continental, Mapping) else {}
        user_state = continental_state.get("user")
        flags_state = continental_state.get("flags")
        user = user_state if isinstance(user_state, Mapping) else {}
        flags = flags_state if isinstance(flags_state, Mapping) else {}

        return {
            "mode": "continental",
            "user_id": str(user.get("continental_id") or session_payload.get("continental_id") or ""),
            "discord_user_id": discord_user_id,
            "username": str(user.get("display_name") or user.get("username") or session_payload.get("username") or "Continental user"),
            "avatar_url": str(user.get("avatar_url") or session_payload.get("avatar_url") or ""),
            "continental": dict(continental_state),
            "flags": dict(flags),
            "manageable_guild_ids": manageable,
        }

    @web.middleware
    async def auth_middleware(request: web.Request, handler: Callable[[web.Request], Any]) -> web.StreamResponse:
        if not request.path.startswith(f"{CONTROL_CENTER_API_PATH}/"):
            return await handler(request)
        if request.path in {
            f"{CONTROL_CENTER_API_PATH}/session",
            f"{CONTROL_CENTER_API_PATH}/session/exchange",
        }:
            return await handler(request)
        auth = await get_request_auth(request)
        if auth is None:
            return web.json_response({"error": "Unauthorized."}, status=401)
        request["auth"] = auth
        return await handler(request)

    def apply_security_headers(response: web.StreamResponse) -> web.StreamResponse:
        response.headers.setdefault("Content-Security-Policy", content_security_policy)
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault(
            "Permissions-Policy",
            "camera=(), geolocation=(), microphone=(), payment=(), usb=()",
        )
        if public_site_root.startswith("https://"):
            response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        return response

    @web.middleware
    async def security_headers_middleware(
        request: web.Request,
        handler: Callable[[web.Request], Any],
    ) -> web.StreamResponse:
        try:
            response = await handler(request)
        except web.HTTPException as exc:
            response = exc
        return apply_security_headers(response)

    app = web.Application(middlewares=[security_headers_middleware, auth_middleware])

    def landing_file(name: str) -> web.FileResponse:
        return web.FileResponse(landing_root / name)

    async def landing_index(_: web.Request) -> web.StreamResponse:
        return landing_file("index.html")

    async def landing_styles(_: web.Request) -> web.StreamResponse:
        return landing_file("styles.css")

    async def landing_script(_: web.Request) -> web.StreamResponse:
        return landing_file("script.js")

    async def landing_404(_: web.Request) -> web.StreamResponse:
        return landing_file("404.html")

    async def landing_favicon(_: web.Request) -> web.StreamResponse:
        return landing_file("favicon.png")

    async def landing_manifest(_: web.Request) -> web.StreamResponse:
        return landing_file("manifest.json")

    async def landing_data(_: web.Request) -> web.StreamResponse:
        return landing_file("data.json")

    async def landing_privacy(_: web.Request) -> web.StreamResponse:
        return landing_file("privacy-policy.html")

    async def landing_terms(_: web.Request) -> web.StreamResponse:
        return landing_file("terms-of-service.html")

    async def landing_robots(_: web.Request) -> web.StreamResponse:
        body = "\n".join(
            [
                "User-agent: *",
                "Allow: /",
                "Disallow: /404.html",
                "Disallow: /control/",
                "Disallow: /control/api/",
                "Disallow: /control/static/",
                "",
                f"Sitemap: {public_site_root}/sitemap.xml",
                "",
            ]
        )
        return web.Response(text=body, content_type="text/plain")

    async def landing_sitemap(_: web.Request) -> web.StreamResponse:
        lastmod = _utc_now().date().isoformat()
        body = "\n".join(
            [
                '<?xml version="1.0" encoding="UTF-8"?>',
                '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
                "  <url>",
                f"    <loc>{public_site_root}/</loc>",
                f"    <lastmod>{lastmod}</lastmod>",
                "    <changefreq>weekly</changefreq>",
                "    <priority>1.0</priority>",
                "  </url>",
                "  <url>",
                f"    <loc>{public_site_root}/privacy-policy.html</loc>",
                f"    <lastmod>{lastmod}</lastmod>",
                "    <changefreq>monthly</changefreq>",
                "    <priority>0.5</priority>",
                "  </url>",
                "  <url>",
                f"    <loc>{public_site_root}/terms-of-service.html</loc>",
                f"    <lastmod>{lastmod}</lastmod>",
                "    <changefreq>monthly</changefreq>",
                "    <priority>0.5</priority>",
                "  </url>",
                "</urlset>",
                "",
            ]
        )
        return web.Response(text=body, content_type="application/xml")

    async def index(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(static_root / "index.html")

    async def session_info(request: web.Request) -> web.StreamResponse:
        auth = await get_request_auth(request)
        if auth is None:
            return web.json_response(
                {
                    "authenticated": False,
                    "control_path": CONTROL_CENTER_PATH,
                    "site_base_url": site_base_url,
                    "continental_required": True,
                    "continental_login_url": continental_login_url,
                    "continental_dashboard_url": continental_dashboard_url,
                    "continental_auth_enabled": continental_auth_enabled,
                    "continental": {
                        "configured": continental_auth_enabled,
                        "ok": False,
                        "linked": False,
                        "message": (
                            "Sign in with Continental ID, then make sure Discord is linked on that account."
                            if continental_auth_enabled
                            else "Continental ID auth is not configured for this control center."
                        ),
                    },
                    "license": current_license_state(),
                }
            )
        return web.json_response(
            {
                "authenticated": True,
                "control_path": CONTROL_CENTER_PATH,
                "site_base_url": site_base_url,
                "mode": auth["mode"],
                "user": {
                    "id": auth.get("user_id"),
                    "name": auth.get("username"),
                    "avatar_url": auth.get("avatar_url"),
                },
                "continental_required": True,
                "continental_login_url": continental_login_url,
                "continental_dashboard_url": continental_dashboard_url,
                "continental_auth_enabled": continental_auth_enabled,
                "continental": auth.get("continental"),
                "license": current_license_state(),
            }
        )

    async def session_exchange(request: web.Request) -> web.StreamResponse:
        authorization = str(request.headers.get("Authorization") or "").strip()
        access_token = ""
        if authorization.lower().startswith("bearer "):
            access_token = authorization[7:].strip()
        if not access_token:
            try:
                payload = await request.json()
            except Exception:
                payload = {}
            if isinstance(payload, Mapping):
                access_token = str(payload.get("accessToken") or payload.get("token") or "").strip()
        if not access_token:
            return web.json_response({"error": "Continental ID access token required."}, status=401)

        session_payload, error_message = await exchange_continental_token(access_token)
        if session_payload is None:
            return web.json_response(
                {
                    "error": error_message or "Continental ID sign-in could not be verified.",
                    "continental_dashboard_url": continental_dashboard_url,
                },
                status=403,
            )

        session_token = secrets.token_urlsafe(32)
        sessions[session_token] = session_payload
        response = web.json_response(
            {
                "authenticated": True,
                "mode": "continental",
                "user": {
                    "id": session_payload.get("continental_id"),
                    "name": session_payload.get("username"),
                    "avatar_url": session_payload.get("avatar_url"),
                },
                "continental": session_payload.get("continental"),
                "license": current_license_state(),
            }
        )
        response.set_cookie(
            SESSION_COOKIE,
            session_token,
            httponly=True,
            secure=secure_cookie,
            samesite="Lax",
            path="/",
            max_age=60 * 60 * 24 * 7,
        )
        return response

    async def auth_logout(request: web.Request) -> web.StreamResponse:
        clear_session_token(request.cookies.get(SESSION_COOKIE))
        response = web.json_response({"ok": True})
        response.del_cookie(SESSION_COOKIE, path="/")
        return response

    def get_manageable_guilds(auth: Mapping[str, Any]) -> list[discord.Guild]:
        manageable_ids = set(auth.get("manageable_guild_ids", set()))
        return sorted(
            [guild for guild in bot.guilds if guild.id in manageable_ids],
            key=lambda item: item.name.lower(),
        )

    async def guild_list(request: web.Request) -> web.StreamResponse:
        auth = request["auth"]
        guilds = get_manageable_guilds(auth)
        license_state = current_license_state()
        payload = {
            "bot": {
                "name": getattr(getattr(bot, "user", None), "name", "Vanguard"),
                "id": getattr(getattr(bot, "user", None), "id", None),
                "guild_count": len(bot.guilds),
            },
            "viewer": {
                "mode": auth.get("mode"),
                "name": auth.get("username"),
            },
            "license": license_state,
            "guilds": [
                {
                    **build_guild_overview(
                        guild,
                        get_guild_config(guild.id),
                        guard_runtime_stats=guard_runtime_stats,
                        reminders=reminders,
                        modlog=modlog,
                        vote_store=vote_store,
                        parse_datetime_utc=parse_datetime_utc,
                    ),
                    "authorization": build_guild_authorization(guild.id, license_state),
                }
                for guild in guilds
            ],
        }
        return web.json_response(payload)

    async def guild_detail(request: web.Request) -> web.StreamResponse:
        guild, error_response = _guild_from_request(request, bot)
        if error_response is not None:
            return error_response
        license_state = current_license_state()
        payload = build_guild_detail(
            guild,
            get_guild_config(guild.id),
            guard_runtime_stats=guard_runtime_stats,
            reminders=reminders,
            modlog=modlog,
            vote_store=vote_store,
            parse_datetime_utc=parse_datetime_utc,
            normalize_guard_settings=normalize_guard_settings,
        )
        payload["authorization"] = build_guild_authorization(guild.id, license_state)
        payload["license"] = license_state
        return web.json_response(payload)

    async def update_guild(request: web.Request) -> web.StreamResponse:
        guild, error_response = _guild_from_request(request, bot)
        if error_response is not None:
            return error_response
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "Request body must be valid JSON."}, status=400)

        guild_cfg = get_guild_config(guild.id)
        errors = apply_guild_control_update(
            guild,
            guild_cfg,
            payload,
            normalize_guard_settings=normalize_guard_settings,
            resolve_guard_preset_name=resolve_guard_preset_name,
            apply_guard_preset=apply_guard_preset,
        )
        if errors:
            return web.json_response({"errors": errors}, status=400)

        save_result = save_settings()
        if save_result is False:
            return web.json_response(
                {"error": "Settings could not be persisted on this Vanguard instance."},
                status=500,
            )
        license_state = current_license_state()
        response_payload = build_guild_detail(
            guild,
            guild_cfg,
            guard_runtime_stats=guard_runtime_stats,
            reminders=reminders,
            modlog=modlog,
            vote_store=vote_store,
            parse_datetime_utc=parse_datetime_utc,
            normalize_guard_settings=normalize_guard_settings,
        )
        response_payload["authorization"] = build_guild_authorization(guild.id, license_state)
        response_payload["license"] = license_state
        return web.json_response(response_payload)

    async def update_lockdown(request: web.Request) -> web.StreamResponse:
        if trigger_lockdown_action is None:
            return web.json_response(
                {"error": "Lockdown actions are not configured for this Vanguard instance."},
                status=501,
            )

        guild, error_response = _guild_from_request(request, bot)
        if error_response is not None:
            return error_response

        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "Request body must be valid JSON."}, status=400)
        if not isinstance(payload, Mapping):
            return web.json_response({"error": "Expected a JSON object."}, status=400)

        locked = payload.get("locked")
        if not isinstance(locked, bool):
            return web.json_response({"error": "`locked` must be true or false."}, status=400)

        auth = request["auth"]
        discord_user_id = _coerce_optional_int(auth.get("discord_user_id")) or 0
        username = str(auth.get("username") or "Continental user")
        ok, message = await trigger_lockdown_action(guild, discord_user_id, username, locked)
        if not ok:
            return web.json_response({"error": message or "Lockdown action failed."}, status=400)

        license_state = current_license_state()
        detail = build_guild_detail(
            guild,
            get_guild_config(guild.id),
            guard_runtime_stats=guard_runtime_stats,
            reminders=reminders,
            modlog=modlog,
            vote_store=vote_store,
            parse_datetime_utc=parse_datetime_utc,
            normalize_guard_settings=normalize_guard_settings,
        )
        detail["authorization"] = build_guild_authorization(guild.id, license_state)
        detail["license"] = license_state
        return web.json_response({"ok": True, "message": message, "detail": detail})

    app.router.add_get("/", landing_index)
    app.router.add_get("/styles.css", landing_styles)
    app.router.add_get("/script.js", landing_script)
    app.router.add_get("/404.html", landing_404)
    app.router.add_get("/favicon.ico", landing_favicon)
    app.router.add_get("/favicon.png", landing_favicon)
    app.router.add_get("/manifest.json", landing_manifest)
    app.router.add_get("/data.json", landing_data)
    app.router.add_get("/privacy-policy.html", landing_privacy)
    app.router.add_get("/terms-of-service.html", landing_terms)
    app.router.add_get("/robots.txt", landing_robots)
    app.router.add_get("/sitemap.xml", landing_sitemap)
    app.router.add_get(CONTROL_CENTER_PATH, index)
    app.router.add_get(CONTROL_CENTER_PATH + "/", index)
    app.router.add_post(f"{CONTROL_CENTER_AUTH_PATH}/logout", auth_logout)
    app.router.add_get(f"{CONTROL_CENTER_API_PATH}/session", session_info)
    app.router.add_post(f"{CONTROL_CENTER_API_PATH}/session/exchange", session_exchange)
    app.router.add_get(f"{CONTROL_CENTER_API_PATH}/guilds", guild_list)
    app.router.add_get(f"{CONTROL_CENTER_API_PATH}/guilds/{{guild_id}}", guild_detail)
    app.router.add_put(f"{CONTROL_CENTER_API_PATH}/guilds/{{guild_id}}", update_guild)
    app.router.add_post(f"{CONTROL_CENTER_API_PATH}/guilds/{{guild_id}}/lockdown", update_lockdown)
    app.router.add_static("/Images/", landing_root / "Images", show_index=False)
    app.router.add_static(CONTROL_CENTER_STATIC_PATH + "/", static_root, show_index=False)
    return app


async def start_control_center_site(app: web.Application, host: str, port: int) -> web.AppRunner:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
