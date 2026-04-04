from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from control_center import (
    apply_guild_control_update,
    build_control_center_url,
    build_guild_detail,
    build_guild_overview,
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
    assert detail["settings"]["welcome_channel_id"] == 10
    assert detail["settings"]["mod_role_ids"] == [21]
    assert detail["settings"]["guard_preset"] == "balanced"
    assert len(detail["channels"]) == 3
    assert len(detail["roles"]) == 3


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
