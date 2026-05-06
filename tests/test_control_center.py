import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from control_center import (
    apply_guild_control_update,
    build_guild_authorization,
    build_control_center_url,
    create_control_center_app,
    build_guild_detail,
    build_guild_overview,
    serialize_continental_account_user,
    serialize_continental_status,
    serialize_license_state,
)
from guard import (
    apply_guard_preset,
    guard_default_settings,
    normalize_guard_settings,
    resolve_guard_preset_name,
)


class FakeChannel:
    def __init__(self, channel_id: int, name: str, position: int = 0, channel_type: str = "text"):
        self.id = channel_id
        self.name = name
        self.position = position
        self.type = channel_type
        self.mention = f"<#{channel_id}>"


class FakeRole:
    def __init__(self, role_id: int, name: str, position: int = 0, default: bool = False):
        self.id = role_id
        self.name = name
        self.position = position
        self.mention = f"<@&{role_id}>"
        self.color = SimpleNamespace(value=0)
        self._default = default

    def is_default(self) -> bool:
        return self._default


class FakeGuild:
    def __init__(self):
        self.id = 123
        self.name = "Control Guild"
        self.member_count = 42
        self.icon = None
        self.text_channels = [
            FakeChannel(10, "arrivals", 1),
            FakeChannel(11, "ops", 2),
            FakeChannel(12, "mod-log", 3),
        ]
        self.roles = [
            FakeRole(1, "@everyone", default=True),
            FakeRole(20, "Member", 1),
            FakeRole(21, "Moderator", 3),
            FakeRole(22, "Raid Team", 2),
        ]

    def get_channel(self, channel_id: int):
        return next((channel for channel in self.text_channels if channel.id == channel_id), None)

    def get_role(self, role_id: int):
        return next((role for role in self.roles if role.id == role_id), None)

    def get_member(self, user_id: int):
        return None

    async def fetch_member(self, user_id: int):
        raise RuntimeError("not needed in tests")


class FakeBot:
    def __init__(self, guilds):
        self.guilds = guilds
        self.user = SimpleNamespace(name="Vanguard", id=999)

    def get_guild(self, guild_id: int):
        return next((guild for guild in self.guilds if guild.id == guild_id), None)


def default_guild_config():
    return {
        "welcome_channel_id": None,
        "welcome_role_id": None,
        "welcome_message": None,
        "ops_channel_id": None,
        "log_channel_id": None,
        "lockdown_role_id": None,
        "mod_role_ids": [],
        **guard_default_settings(),
    }


def parse_datetime(value):
    if not isinstance(value, str):
        return None
    return datetime.fromisoformat(value)


def test_apply_guild_control_update_applies_live_server_values():
    guild = FakeGuild()
    guild_cfg = default_guild_config()

    errors = apply_guild_control_update(
        guild,
        guild_cfg,
        {
            "welcome_channel_id": 10,
            "welcome_role_id": 20,
            "welcome_message": "Welcome {user}",
            "ops_channel_id": 11,
            "log_channel_id": 12,
            "lockdown_role_id": 22,
            "mod_role_ids": [21, 22],
            "guard_preset": "strict",
            "guard": {
                "guard_threshold": 9,
                "guard_slowmode_scope": "trigger",
                "guard_detect_links": False,
            },
        },
        normalize_guard_settings=normalize_guard_settings,
        resolve_guard_preset_name=resolve_guard_preset_name,
        apply_guard_preset=apply_guard_preset,
    )

    assert errors == {}
    assert guild_cfg["welcome_channel_id"] == 10
    assert guild_cfg["welcome_role_id"] == 20
    assert guild_cfg["ops_channel_id"] == 11
    assert guild_cfg["log_channel_id"] == 12
    assert guild_cfg["lockdown_role_id"] == 22
    assert guild_cfg["mod_role_ids"] == [21, 22]
    assert guild_cfg["welcome_message"] == "Welcome {user}"
    assert guild_cfg["guard_enabled"] is True
    assert guild_cfg["guard_threshold"] == 9
    assert guild_cfg["guard_slowmode_scope"] == "trigger"
    assert guild_cfg["guard_detect_links"] is False


def test_apply_guild_control_update_accepts_string_snowflakes():
    guild = FakeGuild()
    guild_cfg = default_guild_config()

    errors = apply_guild_control_update(
        guild,
        guild_cfg,
        {
            "welcome_channel_id": "10",
            "welcome_role_id": "20",
            "ops_channel_id": "11",
            "log_channel_id": "12",
            "lockdown_role_id": "22",
            "mod_role_ids": ["21", "22"],
        },
        normalize_guard_settings=normalize_guard_settings,
        resolve_guard_preset_name=resolve_guard_preset_name,
        apply_guard_preset=apply_guard_preset,
    )

    assert errors == {}
    assert guild_cfg["welcome_channel_id"] == 10
    assert guild_cfg["welcome_role_id"] == 20
    assert guild_cfg["ops_channel_id"] == 11
    assert guild_cfg["log_channel_id"] == 12
    assert guild_cfg["lockdown_role_id"] == 22
    assert guild_cfg["mod_role_ids"] == [21, 22]


def test_apply_guild_control_update_rejects_invalid_references():
    guild = FakeGuild()
    guild_cfg = default_guild_config()

    errors = apply_guild_control_update(
        guild,
        guild_cfg,
        {
            "welcome_channel_id": 999,
            "welcome_role_id": 999,
            "mod_role_ids": [21, 999],
            "guard_preset": "unknown",
        },
        normalize_guard_settings=normalize_guard_settings,
        resolve_guard_preset_name=resolve_guard_preset_name,
        apply_guard_preset=apply_guard_preset,
    )

    assert errors == {
        "welcome_channel_id": "Channel must be a text channel in this server.",
        "welcome_role_id": "Role must exist in this server.",
        "mod_role_ids": "All mod roles must exist in this server.",
        "guard_preset": "Unknown guard preset.",
    }


def test_build_guild_detail_reports_counts_and_settings():
    guild = FakeGuild()
    guild_cfg = default_guild_config()
    guild_cfg["welcome_channel_id"] = 10
    guild_cfg["mod_role_ids"] = [21]
    apply_guard_preset(guild_cfg, "balanced")

    now = datetime.now(timezone.utc)
    reminders = [
        {"guild_id": 123, "due_at": (now + timedelta(minutes=20)).isoformat()},
        {"guild_id": 123, "due_at": (now - timedelta(minutes=5)).isoformat()},
    ]
    modlog = {
        "123": [
            {"created_at": (now - timedelta(hours=2)).isoformat()},
            {"created_at": (now - timedelta(days=2)).isoformat()},
        ]
    }
    vote_store = {"123-1": {}, "123-2": {}, "999-1": {}}
    runtime_stats = {
        123: {
            "triggers_total": 4,
            "suppressed_total": 2,
            "last_trigger_at": now,
            "last_trigger_severity": "elevated",
        }
    }

    overview = build_guild_overview(
        guild,
        guild_cfg,
        guard_runtime_stats=runtime_stats,
        reminders=reminders,
        modlog=modlog,
        vote_store=vote_store,
        parse_datetime_utc=parse_datetime,
    )
    detail = build_guild_detail(
        guild,
        guild_cfg,
        guard_runtime_stats=runtime_stats,
        reminders=reminders,
        modlog=modlog,
        vote_store=vote_store,
        parse_datetime_utc=parse_datetime,
        normalize_guard_settings=normalize_guard_settings,
    )

    assert overview["active_votes"] == 2
    assert overview["pending_reminders"] == 1
    assert overview["recent_cases_24h"] == 1
    assert overview["guard_preset"] == "balanced"
    assert detail["id"] == "123"
    assert detail["settings"]["welcome_channel_id"] == "10"
    assert detail["settings"]["mod_role_ids"] == ["21"]
    assert detail["settings"]["guard_preset"] == "balanced"
    assert len(detail["channels"]) == 3
    assert len(detail["roles"]) == 3


def test_build_guild_detail_serializes_large_ids_as_strings():
    guild = FakeGuild()
    guild.id = 1417156603974779000
    guild.text_channels[0].id = 1409501007880257800
    guild.roles[1].id = 1411111111111111111

    guild_cfg = default_guild_config()
    guild_cfg["welcome_channel_id"] = guild.text_channels[0].id
    guild_cfg["welcome_role_id"] = guild.roles[1].id
    guild_cfg["mod_role_ids"] = [guild.roles[1].id]

    detail = build_guild_detail(
        guild,
        guild_cfg,
        guard_runtime_stats={},
        reminders=[],
        modlog={},
        vote_store={},
        parse_datetime_utc=parse_datetime,
        normalize_guard_settings=normalize_guard_settings,
    )

    assert detail["id"] == "1417156603974779000"
    assert detail["channels"][0]["id"] == "1409501007880257800"
    assert any(role["id"] == "1411111111111111111" for role in detail["roles"])
    assert detail["settings"]["welcome_channel_id"] == "1409501007880257800"
    assert detail["settings"]["welcome_role_id"] == "1411111111111111111"
    assert detail["settings"]["mod_role_ids"] == ["1411111111111111111"]


def test_build_control_center_url_appends_control_path_once():
    assert build_control_center_url("127.0.0.1", 8080) == "http://127.0.0.1:8080/control"
    assert (
        build_control_center_url("127.0.0.1", 8080, "https://vanguard.example.com")
        == "https://vanguard.example.com/control"
    )
    assert (
        build_control_center_url("127.0.0.1", 8080, "https://vanguard.example.com/control")
        == "https://vanguard.example.com/control"
    )


def test_serialize_continental_status_flattens_resolve_payload():
    payload = serialize_continental_status(
        {
            "configured": True,
            "ok": True,
            "linked": True,
            "message": "resolved",
            "body": {
                "user": {
                    "continentalId": "abc123",
                    "username": "linked.user",
                    "displayName": "Linked User",
                    "verified": True,
                    "discordLinked": True,
                },
                "flags": {
                    "trusted": True,
                    "staff": False,
                    "flagged": True,
                    "bannedFromAi": False,
                    "flagReason": "manual review",
                },
            },
        }
    )

    assert payload["configured"] is True
    assert payload["linked"] is True
    assert payload["user"]["continental_id"] == "abc123"
    assert payload["user"]["display_name"] == "Linked User"
    assert payload["flags"]["trusted"] is True
    assert payload["flags"]["flagged"] is True
    assert payload["flags"]["flag_reason"] == "manual review"


def test_control_center_update_surfaces_persistence_failures():
    async def scenario():
        guild = FakeGuild()
        bot = FakeBot([guild])
        guild_cfg = default_guild_config()

        app = create_control_center_app(
            bot=bot,
            get_guild_config=lambda guild_id: guild_cfg,
            save_settings=lambda: False,
            normalize_guard_settings=normalize_guard_settings,
            resolve_guard_preset_name=resolve_guard_preset_name,
            apply_guard_preset=apply_guard_preset,
            guard_runtime_stats={},
            reminders=[],
            modlog={},
            vote_store={},
            parse_datetime_utc=parse_datetime,
            http_request=lambda *args, **kwargs: None,
            can_access_guild=_allow_access,
            fetch_continental_profile=_fetch_continental_profile,
            get_license_state=lambda: None,
            continental_login_url="https://continental.example/login",
            continental_dashboard_url="https://continental.example/dashboard",
            public_url="",
            site_host="127.0.0.1",
            site_port=8080,
            static_dir="control_center",
            landing_dir="website",
        )

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            auth_response = await client.post(
                "/control/api/session/exchange",
                json={"accessToken": "token"},
            )
            assert auth_response.status == 200

            update_response = await client.put(
                "/control/api/guilds/123",
                json={"welcome_message": "Updated"},
            )
            assert update_response.status == 500
            payload = await update_response.json()
            assert payload == {
                "error": "Settings could not be persisted on this Vanguard instance."
            }
        finally:
            await client.close()

    asyncio.run(scenario())


def test_control_center_lockdown_action_uses_authenticated_actor_context():
    async def scenario():
        guild = FakeGuild()
        bot = FakeBot([guild])
        guild_cfg = default_guild_config()
        calls: list[tuple[int, str, bool]] = []

        async def trigger_lockdown_action(target_guild, discord_user_id: int, username: str, locked: bool):
            assert target_guild is guild
            calls.append((discord_user_id, username, locked))
            return True, "Lockdown enabled for role `Member` (3 updated, 0 failed)."

        app = create_control_center_app(
            bot=bot,
            get_guild_config=lambda guild_id: guild_cfg,
            save_settings=lambda: True,
            normalize_guard_settings=normalize_guard_settings,
            resolve_guard_preset_name=resolve_guard_preset_name,
            apply_guard_preset=apply_guard_preset,
            guard_runtime_stats={},
            reminders=[],
            modlog={},
            vote_store={},
            parse_datetime_utc=parse_datetime,
            http_request=lambda *args, **kwargs: None,
            can_access_guild=_allow_access,
            fetch_continental_profile=_fetch_continental_profile,
            get_license_state=lambda: None,
            continental_login_url="https://continental.example/login",
            continental_dashboard_url="https://continental.example/dashboard",
            public_url="",
            site_host="127.0.0.1",
            site_port=8080,
            static_dir="control_center",
            landing_dir="website",
            trigger_lockdown_action=trigger_lockdown_action,
        )

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            auth_response = await client.post(
                "/control/api/session/exchange",
                json={"accessToken": "token"},
            )
            assert auth_response.status == 200

            action_response = await client.post(
                "/control/api/guilds/123/lockdown",
                json={"locked": True},
            )
            assert action_response.status == 200
            payload = await action_response.json()
            assert payload["ok"] is True
            assert payload["message"] == "Lockdown enabled for role `Member` (3 updated, 0 failed)."
            assert payload["detail"]["id"] == "123"
            assert calls == [(555, "Linked User", True)]
        finally:
            await client.close()

    asyncio.run(scenario())


async def _allow_access(guild, user_id: int) -> bool:
    return guild.id == 123 and user_id == 555


def _fetch_continental_profile(access_token: str):
    assert access_token == "token"
    return {
        "ok": True,
        "user": {
            "continentalId": "continental-user",
            "username": "linked.user",
            "displayName": "Linked User",
            "isVerified": True,
            "vanguard": {
                "linkedDiscord": True,
                "discordUserId": "555",
            },
            "oauthProviders": {
                "discord": {
                    "username": "discord-user",
                }
            },
        },
    }


def test_serialize_license_state_and_guild_authorization():
    payload = serialize_license_state(
        {
            "configured": True,
            "required": True,
            "authorized": True,
            "reason": "active license",
            "allowed_guild_ids": [123, 456],
            "entitlements": {
                "ai": True,
                "advancedVotes": True,
                "guardPresets": ["balanced", "strict"],
            },
        }
    )

    assert payload["mode"] == "required"
    assert payload["allowed_guild_count"] == 2
    assert payload["entitlements"]["ai"] is True
    assert payload["entitlements"]["advanced_votes"] is True
    assert payload["entitlements"]["guard_presets"] == ["balanced", "strict"]

    authorized = build_guild_authorization(123, payload)
    blocked = build_guild_authorization(999, payload)

    assert authorized["authorized"] is True
    assert blocked["authorized"] is False
    assert blocked["source"] == "allowlist"


def test_serialize_continental_account_user_requires_linked_discord_state():
    payload = serialize_continental_account_user(
        {
            "continentalId": "user-1",
            "username": "charlie",
            "displayName": "Charlie",
            "isVerified": True,
            "profile": {"avatar": "https://cdn.example/avatar.png"},
            "oauthProviders": {
                "discord": {
                    "username": "charliediscord",
                }
            },
            "vanguard": {
                "linkedDiscord": True,
                "discordUserId": "1234567890",
                "trusted": True,
                "staff": False,
                "flagged": False,
                "bannedFromAi": True,
                "flagReason": "manual block",
            },
        }
    )

    assert payload["configured"] is True
    assert payload["linked"] is True
    assert payload["user"]["continental_id"] == "user-1"
    assert payload["user"]["discord_linked"] is True
    assert payload["user"]["discord_user_id"] == "1234567890"
    assert payload["user"]["discord_username"] == "charliediscord"
    assert payload["flags"]["trusted"] is True
    assert payload["flags"]["banned_from_ai"] is True
    assert payload["flags"]["flag_reason"] == "manual block"
