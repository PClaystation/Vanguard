# Copyright (c) 2026 Continental. All rights reserved.
# Licensed under the Vanguard Proprietary Source-Available License (see /LICENSE).

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
import secrets
from typing import Any, Awaitable, Callable, Mapping, Sequence
from urllib.parse import urlencode

from aiohttp import web
import discord

from guard import GUARD_PRESETS, guard_default_settings, normalize_guard_settings as normalize_guard_profile

AUTH_HEADER = "X-Vanguard-Control-Token"
SESSION_COOKIE = "vanguard_control_session"
OAUTH_STATE_COOKIE = "vanguard_oauth_state"
DISCORD_API_BASE = "https://discord.com/api/v10"
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
        "id": int(getattr(channel, "id")),
        "name": str(getattr(channel, "name", "unknown")),
        "mention": str(getattr(channel, "mention", f"<#{getattr(channel, 'id', 0)}>")),
        "position": int(getattr(channel, "position", 0)),
    }


def _serialize_role(role: Any) -> dict[str, Any]:
    color = getattr(role, "color", None)
    color_value = getattr(color, "value", 0)
    return {
        "id": int(getattr(role, "id")),
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
        "last_trigger_actor_id": _coerce_optional_int(stats.get("last_trigger_actor_id")),
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
        "id": guild.id,
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
        "welcome_channel_id": _coerce_optional_int(guild_cfg.get("welcome_channel_id")),
        "welcome_role_id": _coerce_optional_int(guild_cfg.get("welcome_role_id")),
        "welcome_message": str(guild_cfg.get("welcome_message") or ""),
        "ops_channel_id": _coerce_optional_int(guild_cfg.get("ops_channel_id")),
        "log_channel_id": _coerce_optional_int(guild_cfg.get("log_channel_id")),
        "lockdown_role_id": _coerce_optional_int(guild_cfg.get("lockdown_role_id")),
        "mod_role_ids": list(guild_cfg.get("mod_role_ids", []) or []),
        "guard_preset": _match_guard_preset_name(normalized_guard),
        "guard": normalized_guard,
    }
    detail["channels"] = text_channels
    detail["roles"] = roles
    return detail


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


def _avatar_url(user_id: int | str, avatar_hash: str | None) -> str | None:
    if not avatar_hash:
        return None
    return f"https://cdn.discordapp.com/avatars/{user_id}/{avatar_hash}.png?size=128"


def _discord_authorize_url(client_id: str, redirect_uri: str, state: str) -> str:
    query = urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "scope": "identify guilds",
            "redirect_uri": redirect_uri,
            "state": state,
        }
    )
    return f"https://discord.com/oauth2/authorize?{query}"


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
    save_settings: Callable[[], None],
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
    oauth_client_id: str,
    oauth_client_secret: str,
    oauth_redirect_uri: str,
    public_url: str,
    site_host: str,
    site_port: int,
    control_token: str,
    static_dir: str | Path,
    landing_dir: str | Path,
) -> web.Application:
    static_root = Path(static_dir)
    landing_root = Path(landing_dir)
    secure_cookie = _cookie_secure(oauth_redirect_uri, public_url)
    oauth_enabled = bool(oauth_client_id and oauth_client_secret and oauth_redirect_uri)
    site_base_url = _public_site_base(public_url, site_host, site_port)
    sessions: dict[str, dict[str, Any]] = {}

    def clear_session_token(token: str | None) -> None:
        if token:
            sessions.pop(token, None)

    async def discord_form_post(form_data: Mapping[str, str]) -> dict[str, Any]:
        return await asyncio.to_thread(
            lambda: http_request(
                "POST",
                f"{DISCORD_API_BASE}/oauth2/token",
                data=form_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                auth=(oauth_client_id, oauth_client_secret),
                timeout=10,
            )
        )

    async def exchange_code(code: str) -> dict[str, Any]:
        response = await discord_form_post(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": oauth_redirect_uri,
            }
        )
        response.raise_for_status()
        return response.json()

    async def refresh_session_tokens(session_payload: dict[str, Any]) -> dict[str, Any] | None:
        refresh_token = str(session_payload.get("refresh_token") or "").strip()
        if not refresh_token:
            return None
        try:
            response = await discord_form_post(
                {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                }
            )
            response.raise_for_status()
        except Exception:
            return None
        token_payload = response.json()
        session_payload["access_token"] = str(token_payload.get("access_token") or "")
        session_payload["refresh_token"] = str(token_payload.get("refresh_token") or refresh_token)
        expires_in = int(token_payload.get("expires_in") or 0)
        session_payload["expires_at"] = (_utc_now() + timedelta(seconds=max(60, expires_in))).isoformat()
        session_payload["guild_cache"] = None
        return session_payload

    async def discord_api_get(path: str, access_token: str) -> Any:
        response = await asyncio.to_thread(
            lambda: http_request(
                "GET",
                f"{DISCORD_API_BASE}{path}",
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=10,
            )
        )
        response.raise_for_status()
        return response.json()

    def build_session_payload(token_payload: Mapping[str, Any], user_payload: Mapping[str, Any]) -> dict[str, Any]:
        expires_in = int(token_payload.get("expires_in") or 0)
        user_id = _coerce_optional_int(user_payload.get("id")) or 0
        return {
            "user_id": user_id,
            "username": str(user_payload.get("username") or "Discord user"),
            "global_name": str(user_payload.get("global_name") or ""),
            "avatar_url": _avatar_url(user_id, str(user_payload.get("avatar") or "")),
            "access_token": str(token_payload.get("access_token") or ""),
            "refresh_token": str(token_payload.get("refresh_token") or ""),
            "expires_at": (_utc_now() + timedelta(seconds=max(60, expires_in))).isoformat(),
            "guild_cache": None,
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

        if expires_at <= _utc_now() + timedelta(minutes=5):
            refreshed = await refresh_session_tokens(session_payload)
            if refreshed is None:
                clear_session_token(session_token)
                return None
            session_payload = refreshed

        request["session_token"] = session_token
        return session_payload

    async def get_session_user_guild_ids(session_payload: dict[str, Any]) -> set[int]:
        cached = session_payload.get("guild_cache")
        cached_at = parse_datetime_utc(cached.get("cached_at")) if isinstance(cached, dict) else None
        if cached_at and cached_at > _utc_now() - timedelta(minutes=2):
            return {
                guild_id
                for guild_id in (
                    _coerce_optional_int(item) for item in cached.get("guild_ids", [])
                )
                if guild_id
            }

        try:
            guild_payloads = await discord_api_get("/users/@me/guilds", str(session_payload.get("access_token") or ""))
        except Exception:
            return set()

        guild_ids = {
            guild_id
            for guild_id in (
                _coerce_optional_int(item.get("id")) if isinstance(item, dict) else None
                for item in guild_payloads if isinstance(guild_payloads, list)
            )
            if guild_id
        }
        session_payload["guild_cache"] = {
            "cached_at": _utc_now().isoformat(),
            "guild_ids": sorted(guild_ids),
        }
        return guild_ids

    async def get_request_auth(request: web.Request) -> dict[str, Any] | None:
        supplied = request.headers.get(AUTH_HEADER, "").strip()
        if not supplied:
            authorization = request.headers.get("Authorization", "").strip()
            if authorization.lower().startswith("bearer "):
                supplied = authorization[7:].strip()
        if control_token and supplied == control_token:
            return {
                "mode": "operator",
                "user_id": None,
                "username": "Operator",
                "avatar_url": None,
                "manageable_guild_ids": {guild.id for guild in bot.guilds},
            }

        session_payload = await get_session_from_cookie(request)
        if not session_payload:
            return None

        guild_ids = await get_session_user_guild_ids(session_payload)
        manageable: set[int] = set()
        user_id = int(session_payload["user_id"])
        for guild in bot.guilds:
            if guild.id not in guild_ids:
                continue
            if await can_access_guild(guild, user_id):
                manageable.add(guild.id)

        return {
            "mode": "discord",
            "user_id": user_id,
            "username": str(session_payload.get("global_name") or session_payload.get("username") or "Discord user"),
            "avatar_url": session_payload.get("avatar_url"),
            "manageable_guild_ids": manageable,
        }

    @web.middleware
    async def auth_middleware(request: web.Request, handler: Callable[[web.Request], Any]) -> web.StreamResponse:
        if not request.path.startswith(f"{CONTROL_CENTER_API_PATH}/"):
            return await handler(request)
        if request.path == f"{CONTROL_CENTER_API_PATH}/session":
            return await handler(request)
        auth = await get_request_auth(request)
        if auth is None:
            return web.json_response({"error": "Unauthorized."}, status=401)
        request["auth"] = auth
        return await handler(request)

    app = web.Application(middlewares=[auth_middleware])

    async def landing_index(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(landing_root / "index.html")

    async def landing_styles(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(landing_root / "styles.css")

    async def landing_script(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(landing_root / "script.js")

    async def landing_404(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(landing_root / "404.html")

    async def index(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(static_root / "index.html")

    async def session_info(request: web.Request) -> web.StreamResponse:
        auth = await get_request_auth(request)
        if auth is None:
            return web.json_response(
                {
                    "authenticated": False,
                    "oauth_enabled": oauth_enabled,
                    "operator_token_enabled": bool(control_token),
                    "control_path": CONTROL_CENTER_PATH,
                    "site_base_url": site_base_url,
                }
            )
        return web.json_response(
            {
                "authenticated": True,
                "oauth_enabled": oauth_enabled,
                "operator_token_enabled": bool(control_token),
                "control_path": CONTROL_CENTER_PATH,
                "site_base_url": site_base_url,
                "mode": auth["mode"],
                "user": {
                    "id": auth.get("user_id"),
                    "name": auth.get("username"),
                    "avatar_url": auth.get("avatar_url"),
                },
            }
        )

    async def auth_login(_: web.Request) -> web.StreamResponse:
        if not oauth_enabled:
            return web.Response(status=503, text="Discord OAuth is not configured.")
        state = secrets.token_urlsafe(24)
        response = web.HTTPFound(_discord_authorize_url(oauth_client_id, oauth_redirect_uri, state))
        response.set_cookie(
            OAUTH_STATE_COOKIE,
            state,
            httponly=True,
            secure=secure_cookie,
            samesite="Lax",
            path="/",
            max_age=600,
        )
        raise response

    async def auth_callback(request: web.Request) -> web.StreamResponse:
        if not oauth_enabled:
            return web.Response(status=503, text="Discord OAuth is not configured.")
        returned_state = str(request.query.get("state") or "")
        expected_state = request.cookies.get(OAUTH_STATE_COOKIE, "")
        code = str(request.query.get("code") or "")
        if not returned_state or not expected_state or returned_state != expected_state or not code:
            return web.Response(status=400, text="OAuth validation failed.")

        try:
            token_payload = await exchange_code(code)
            user_payload = await discord_api_get("/users/@me", str(token_payload.get("access_token") or ""))
        except Exception:
            return web.Response(status=502, text="Discord OAuth exchange failed.")

        session_token = secrets.token_urlsafe(32)
        sessions[session_token] = build_session_payload(token_payload, user_payload)
        response = web.HTTPFound(CONTROL_CENTER_PATH + "/")
        response.set_cookie(
            SESSION_COOKIE,
            session_token,
            httponly=True,
            secure=secure_cookie,
            samesite="Lax",
            path="/",
            max_age=60 * 60 * 24 * 7,
        )
        response.del_cookie(OAUTH_STATE_COOKIE, path="/")
        raise response

    async def auth_logout(request: web.Request) -> web.StreamResponse:
        clear_session_token(request.cookies.get(SESSION_COOKIE))
        response = web.json_response({"ok": True})
        response.del_cookie(SESSION_COOKIE, path="/")
        response.del_cookie(OAUTH_STATE_COOKIE, path="/")
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
            "guilds": [
                build_guild_overview(
                    guild,
                    get_guild_config(guild.id),
                    guard_runtime_stats=guard_runtime_stats,
                    reminders=reminders,
                    modlog=modlog,
                    vote_store=vote_store,
                    parse_datetime_utc=parse_datetime_utc,
                )
                for guild in guilds
            ],
        }
        return web.json_response(payload)

    async def guild_detail(request: web.Request) -> web.StreamResponse:
        auth = request["auth"]
        guild_id = _coerce_optional_int(request.match_info.get("guild_id"))
        guild = bot.get_guild(guild_id or 0)
        if guild is None:
            return web.json_response({"error": "Guild not found."}, status=404)
        if guild.id not in set(auth.get("manageable_guild_ids", set())):
            return web.json_response({"error": "Forbidden."}, status=403)
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
        return web.json_response(payload)

    async def update_guild(request: web.Request) -> web.StreamResponse:
        auth = request["auth"]
        guild_id = _coerce_optional_int(request.match_info.get("guild_id"))
        guild = bot.get_guild(guild_id or 0)
        if guild is None:
            return web.json_response({"error": "Guild not found."}, status=404)
        if guild.id not in set(auth.get("manageable_guild_ids", set())):
            return web.json_response({"error": "Forbidden."}, status=403)
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

        save_settings()
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
        return web.json_response(response_payload)

    app.router.add_get("/", landing_index)
    app.router.add_get("/styles.css", landing_styles)
    app.router.add_get("/script.js", landing_script)
    app.router.add_get("/404.html", landing_404)
    app.router.add_get(CONTROL_CENTER_PATH, index)
    app.router.add_get(CONTROL_CENTER_PATH + "/", index)
    app.router.add_get(f"{CONTROL_CENTER_AUTH_PATH}/login", auth_login)
    app.router.add_get(f"{CONTROL_CENTER_AUTH_PATH}/callback", auth_callback)
    app.router.add_post(f"{CONTROL_CENTER_AUTH_PATH}/logout", auth_logout)
    app.router.add_get(f"{CONTROL_CENTER_API_PATH}/session", session_info)
    app.router.add_get(f"{CONTROL_CENTER_API_PATH}/guilds", guild_list)
    app.router.add_get(f"{CONTROL_CENTER_API_PATH}/guilds/{{guild_id}}", guild_detail)
    app.router.add_put(f"{CONTROL_CENTER_API_PATH}/guilds/{{guild_id}}", update_guild)
    app.router.add_static("/Images/", landing_root / "Images", show_index=False)
    app.router.add_static(CONTROL_CENTER_STATIC_PATH + "/", static_root, show_index=False)
    return app


async def start_control_center_site(app: web.Application, host: str, port: int) -> web.AppRunner:
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner
