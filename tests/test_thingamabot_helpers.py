import importlib
import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace


def load_thingamabot(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("VANGUARD_DATA_DIR", str(data_dir))
    for module_name in ("thingamabot", "guard", "vote", "data_paths"):
        sys.modules.pop(module_name, None)
    module = importlib.import_module("thingamabot")
    return module, data_dir


def test_parse_duration_to_seconds(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    assert bot.parse_duration_to_seconds("15") == 900
    assert bot.parse_duration_to_seconds("1h30m") == 5400
    assert bot.parse_duration_to_seconds("2d4h5m6s") == (2 * 86400 + 4 * 3600 + 5 * 60 + 6)
    assert bot.parse_duration_to_seconds("abc") is None
    assert bot.parse_duration_to_seconds("1h-30m") is None


def test_normalize_settings_clamps_values(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    raw = {
        "owner_only": "true",
        "guilds": {
            "123": {
                "welcome_channel_id": "42",
                "mod_role_ids": ["5", "not-an-id", 7],
                "guard_threshold": 1,
                "guard_window_seconds": 2,
                "guard_new_account_hours": 0,
                "guard_slowmode_seconds": 99999,
                "guard_cooldown_seconds": 2,
                "guard_slowmode_scope": "invalid",
                "guard_max_slowmode_channels": 999,
                "guard_timeout_seconds": -1,
                "guard_join_threshold": 1,
                "guard_join_window_seconds": 1,
                "guard_duplicate_min_chars": 1,
            }
        },
    }

    normalized = bot.normalize_settings(raw)
    cfg = normalized["guilds"]["123"]

    assert normalized["owner_only"] is True
    assert cfg["welcome_channel_id"] == 42
    assert cfg["mod_role_ids"] == [5, 7]
    assert "prefix" not in cfg
    assert "mc_port" not in cfg
    assert "mc_host" not in cfg
    assert cfg["guard_threshold"] == 8
    assert cfg["guard_window_seconds"] == 30
    assert cfg["guard_new_account_hours"] == 24
    assert cfg["guard_slowmode_seconds"] == 30
    assert cfg["guard_cooldown_seconds"] == 300
    assert cfg["guard_slowmode_scope"] == "trigger"
    assert cfg["guard_max_slowmode_channels"] == 3
    assert cfg["guard_timeout_seconds"] == 0
    assert cfg["guard_join_threshold"] == 6
    assert cfg["guard_join_window_seconds"] == 45
    assert cfg["guard_duplicate_min_chars"] == 12


def test_guard_preset_application(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    guard = importlib.import_module("guard")

    cfg = bot.default_guild_settings()
    ok = guard.apply_guard_preset(cfg, "strict")

    assert ok is True
    assert cfg["guard_enabled"] is True
    assert cfg["guard_threshold"] == 6
    assert cfg["guard_window_seconds"] == 20
    assert cfg["guard_detect_duplicates"] is True


def test_runtime_files_live_in_data_dir(monkeypatch, tmp_path):
    bot, data_dir = load_thingamabot(monkeypatch, tmp_path)

    assert Path(bot.SETTINGS_FILE).parent == data_dir
    assert Path(bot.REMINDERS_FILE).parent == data_dir
    assert Path(bot.MOD_LOG_FILE).parent == data_dir

    bot.write_json_atomic(bot.SETTINGS_FILE, {"ok": True})
    assert Path(bot.SETTINGS_FILE).exists()


def test_ai_endpoints_derive_from_legacy_ask_url(monkeypatch, tmp_path):
    for key in ("AI_SERVER_BASE_URL", "AI_ASK_URL", "AI_CHAT_URL", "AI_HEALTH_URL", "AI_MODELS_URL", "AI_SESSION_URL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("AI_SERVER_URL", "http://localhost:3001/ask")

    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    assert bot.AI_SERVER_BASE_URL == "http://localhost:3001"
    assert bot.AI_ASK_URL == "http://localhost:3001/ask"
    assert bot.AI_CHAT_URL == "http://localhost:3001/chat"
    assert bot.AI_HEALTH_URL == "http://localhost:3001/health"
    assert bot.AI_MODELS_URL == "http://localhost:3001/models"
    assert bot.AI_SESSION_URL == "http://localhost:3001/session"


def test_continental_endpoints_derive_from_base_url(monkeypatch, tmp_path):
    for key in (
        "CONTINENTAL_ID_BASE_URL",
        "CONTINENTAL_ID_HEALTH_URL",
        "CONTINENTAL_ID_RESOLVE_URL",
        "VANGUARD_LICENSE_VERIFY_URL",
        "FLAG_USER_URL",
        "UNFLAG_USER_URL",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CONTINENTAL_ID_BASE_URL", "http://localhost:5000")

    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    assert bot.CONTINENTAL_ID_HEALTH_URL == "http://localhost:5000/api/vanguard/health"
    assert bot.CONTINENTAL_ID_RESOLVE_URL == "http://localhost:5000/api/vanguard/users/resolve"
    assert bot.VANGUARD_LICENSE_VERIFY_URL == "http://localhost:5000/api/vanguard/license/verify"
    assert bot.FLAG_USER_URL == "http://localhost:5000/api/vanguard/users/flag"
    assert bot.UNFLAG_USER_URL == "http://localhost:5000/api/vanguard/users/unflag"


def test_extract_ai_answer_supports_nested_shapes(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    assert bot._extract_ai_answer({"answer": "Top level"}) == "Top level"
    assert bot._extract_ai_answer({"data": {"response": "Nested"}}) == "Nested"
    assert bot._extract_ai_answer("plain text") == "plain text"
    assert bot._extract_ai_answer({"foo": "bar"}) == ""


def test_build_ai_session_id_is_stable(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    assert bot._build_ai_session_id(123, 456, 789) == "discord:123:456:789"
    assert bot._build_ai_session_id(None, 456, 789) == "discord:dm:456:789"


def test_build_backend_headers_uses_configured_secrets(monkeypatch, tmp_path):
    monkeypatch.setenv("VANGUARD_BACKEND_API_KEY", "secret")
    monkeypatch.setenv("VANGUARD_INSTANCE_ID", "instance-1")

    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    headers = bot.build_backend_headers()

    assert headers["X-Vanguard-Api-Key"] == "secret"
    assert headers["X-Vanguard-Instance-Id"] == "instance-1"


def test_require_ai_access_defers_before_lookup(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    events: list[str] = []

    async def fake_defer(ctx, *, ephemeral=False):
        events.append("defer")
        return True

    async def fake_to_thread(func, *args, **kwargs):
        events.append("lookup")
        return {
            "configured": True,
            "ok": True,
            "body": {
                "linked": True,
                "user": {"discordLinked": True},
                "flags": {},
            },
        }

    monkeypatch.setattr(bot, "safe_ctx_defer", fake_defer)
    monkeypatch.setattr(bot.asyncio, "to_thread", fake_to_thread)

    ctx = SimpleNamespace(author=SimpleNamespace(id=123))

    assert asyncio.run(bot.require_ai_access(ctx)) is True
    assert events == ["defer", "lookup"]


def test_send_backend_user_update_defers_before_request(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    events: list[str] = []
    sent_messages: list[str] = []

    async def fake_defer(ctx, *, ephemeral=False):
        events.append("defer")
        return True

    async def fake_send(ctx, message, *args, **kwargs):
        sent_messages.append(message)
        return None

    async def fake_to_thread(func, *args, **kwargs):
        events.append("request")

        class FakeResponse:
            status_code = 200
            text = "ok"

            @staticmethod
            def json():
                return {"message": "updated"}

        return FakeResponse()

    monkeypatch.setattr(bot, "safe_ctx_defer", fake_defer)
    monkeypatch.setattr(bot, "safe_ctx_send", fake_send)
    monkeypatch.setattr(bot.asyncio, "to_thread", fake_to_thread)

    ctx = SimpleNamespace(guild=None)

    asyncio.run(
        bot.send_backend_user_update(
            ctx,
            "123",
            "http://localhost/test",
            "has been flagged",
        )
    )

    assert events == ["defer", "request"]
    assert sent_messages == ["✅ <@123> has been flagged. `updated`"]


def test_commands_are_registered_only_as_slash_commands(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    assert not bot.bot.commands
    tree_commands = {command.name for command in bot.bot.tree.walk_commands()}
    assert {"help", "status", "guard", "votecreate", "banner", "installcontext", "mutualservers"}.issubset(tree_commands)


def test_ai_commands_are_registered_with_new_names(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    tree_commands = {command.name for command in bot.bot.tree.walk_commands()}

    assert "ai" in tree_commands
    assert "aireset" in tree_commands
    assert "vanguard" not in tree_commands
    assert "vanguardreset" not in tree_commands


def test_account_install_commands_allow_user_installs(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    command = bot.bot.tree.get_command("installcontext")

    assert command is not None
    assert command.allowed_installs is not None
    assert command.allowed_contexts is not None
    assert command.allowed_installs.guild is True
    assert command.allowed_installs.user is True
    assert command.allowed_contexts.guild is True
    assert command.allowed_contexts.dm_channel is True
    assert command.allowed_contexts.private_channel is True


def test_guild_only_commands_block_user_installs(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    command = bot.bot.tree.get_command("guard")

    assert command is not None
    assert command.allowed_installs is not None
    assert command.allowed_contexts is not None
    assert command.allowed_installs.guild is True
    assert command.allowed_installs.user is False
    assert command.allowed_contexts.guild is True
    assert command.allowed_contexts.dm_channel is False
    assert command.allowed_contexts.private_channel is False


def test_describe_interaction_install_type_supports_user_and_guild_modes(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    assert (
        bot.describe_interaction_install_type(
            SimpleNamespace(is_guild_integration=lambda: False, is_user_integration=lambda: True)
        )
        == "User Install"
    )
    assert (
        bot.describe_interaction_install_type(
            SimpleNamespace(is_guild_integration=lambda: True, is_user_integration=lambda: False)
        )
        == "Guild Install"
    )


def test_find_mutual_guilds_filters_on_membership(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    class FakeGuild:
        def __init__(self, guild_id: int, member_ids: set[int]):
            self.id = guild_id
            self._member_ids = member_ids

        def get_member(self, user_id: int):
            return object() if user_id in self._member_ids else None

    guilds = [
        FakeGuild(1, {10, 20}),
        FakeGuild(2, {20}),
        FakeGuild(3, {30}),
    ]

    shared_ids = [guild.id for guild in bot.find_mutual_guilds(guilds, 20)]
    assert shared_ids == [1, 2]


def test_parse_allowed_guild_ids_skips_invalid_values(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    parsed = bot.parse_allowed_guild_ids(["123", "abc", -5, 456, None])
    assert parsed == {123, 456}


def test_verify_license_requires_verify_url_when_enforced(monkeypatch, tmp_path):
    monkeypatch.setenv("VANGUARD_REQUIRE_LICENSE", "true")
    monkeypatch.setenv("CONTINENTAL_ID_BASE_URL", "")
    monkeypatch.setenv("VANGUARD_LICENSE_VERIFY_URL", "")

    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    authorized, reason, allowed, entitlements = bot.verify_license_sync(bot_user_id=None, guild_count=0)

    assert authorized is False
    assert "not configured" in reason
    assert allowed == set()
    assert entitlements["ai"] is False


def test_resolve_continental_user_sync_returns_linked_payload(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTINENTAL_ID_RESOLVE_URL", "http://localhost:5000/api/vanguard/users/resolve")

    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "linked": True,
                "user": {
                    "username": "linked.user",
                    "displayName": "Linked User",
                },
                "flags": {
                    "trusted": True,
                    "flagged": False,
                },
            }

    monkeypatch.setattr(bot, "http_request", lambda *args, **kwargs: FakeResponse())

    result = bot.resolve_continental_user_sync("123456789012345678")

    assert result["configured"] is True
    assert result["ok"] is True
    assert result["linked"] is True
    assert result["body"]["user"]["username"] == "linked.user"


def test_get_ai_access_requirement_message_requires_continental_link(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    message = bot.get_ai_access_requirement_message(
        {
            "configured": True,
            "ok": True,
            "linked": False,
            "body": {
                "linked": False,
                "user": {
                    "discordLinked": False,
                },
                "flags": {},
            },
        }
    )

    assert message is not None
    assert "link your Continental ID account" in message


def test_get_ai_access_requirement_message_allows_linked_accounts(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    message = bot.get_ai_access_requirement_message(
        {
            "configured": True,
            "ok": True,
            "linked": True,
            "body": {
                "linked": True,
                "user": {
                    "discordLinked": True,
                },
                "flags": {},
            },
        }
    )

    assert message is None


def test_get_ai_access_requirement_message_blocks_ai_bans(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    message = bot.get_ai_access_requirement_message(
        {
            "configured": True,
            "ok": True,
            "linked": True,
            "body": {
                "linked": True,
                "user": {
                    "discordLinked": True,
                },
                "flags": {
                    "bannedFromAi": True,
                },
            },
        }
    )

    assert message == "⛔ Your Continental ID account is not allowed to use Vanguard AI."


def test_normalize_license_entitlements_supports_dashboard_shape(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    payload = bot.normalize_license_entitlements(
        {
            "ai": True,
            "advancedVotes": True,
            "guardPresets": ["balanced", "strict", "balanced"],
        }
    )

    assert payload == {
        "ai": True,
        "advancedVotes": True,
        "guardPresets": ["balanced", "strict"],
    }


def test_notify_personal_account_guild_join_sends_dm(monkeypatch, tmp_path):
    monkeypatch.setenv("VANGUARD_GUILD_JOIN_NOTIFY_USER_ID", "999")
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    sent_messages: list[str] = []

    class FakeUser:
        async def send(self, message: str):
            sent_messages.append(message)

    monkeypatch.setattr(bot.bot, "get_user", lambda user_id: FakeUser() if user_id == 999 else None)
    guild = SimpleNamespace(name="Alpha", id=123, owner_id=456, member_count=78)

    result = asyncio.run(bot.notify_personal_account_guild_join(guild, authorized=True))

    assert result is True
    assert sent_messages == [
        "Vanguard joined a server.\n"
        "Server: Alpha\n"
        "Server ID: 123\n"
        "Owner ID: 456\n"
        "Members: 78\n"
        "Status: authorized"
    ]


def test_on_guild_join_notifies_and_leaves_unauthorized_guild(monkeypatch, tmp_path):
    monkeypatch.setenv("VANGUARD_GUILD_JOIN_NOTIFY_USER_ID", "999")
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    notifications: list[tuple[int, bool]] = []
    monkeypatch.setattr(
        bot,
        "notify_personal_account_guild_join",
        lambda guild, authorized: notifications.append((guild.id, authorized)) or asyncio.sleep(0, result=True),
    )
    monkeypatch.setattr(bot, "is_guild_authorized", lambda guild_id: False)

    class FakeGuild:
        id = 321
        name = "Blocked Guild"

        def __init__(self):
            self.left = False

        async def leave(self):
            self.left = True

    guild = FakeGuild()

    asyncio.run(bot.on_guild_join(guild))

    assert notifications == [(321, False)]
    assert guild.left is True


def test_on_guild_join_notifies_authorized_guild_without_leaving(monkeypatch, tmp_path):
    monkeypatch.setenv("VANGUARD_GUILD_JOIN_NOTIFY_USER_ID", "999")
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    notifications: list[tuple[int, bool]] = []
    monkeypatch.setattr(
        bot,
        "notify_personal_account_guild_join",
        lambda guild, authorized: notifications.append((guild.id, authorized)) or asyncio.sleep(0, result=True),
    )
    monkeypatch.setattr(bot, "is_guild_authorized", lambda guild_id: True)

    class FakeGuild:
        id = 654
        name = "Allowed Guild"

        def __init__(self):
            self.left = False

        async def leave(self):
            self.left = True

    guild = FakeGuild()

    asyncio.run(bot.on_guild_join(guild))

    assert notifications == [(654, True)]
    assert guild.left is False


def test_should_send_welcome_event_dedupes_repeated_join(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    bot.recent_welcome_events.clear()

    assert bot.should_send_welcome_event(1, 2, now=100.0) is True
    assert bot.should_send_welcome_event(1, 2, now=105.0) is False
    assert bot.should_send_welcome_event(1, 3, now=105.0) is True
    assert bot.should_send_welcome_event(1, 2, now=131.0) is True


def test_on_member_join_ignores_duplicate_welcome_event(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    bot.recent_welcome_events.clear()

    calls: list[str] = []

    async def fake_handle_guard_member_join(**kwargs):
        calls.append("guard")

    monkeypatch.setattr(bot, "get_guild_config", lambda guild_id: {"welcome_channel_id": 99, "welcome_role_id": None, "mod_role_ids": []})
    monkeypatch.setattr(bot, "should_send_welcome_event", lambda guild_id, member_id: len(calls) == 0)
    monkeypatch.setattr(bot, "handle_guard_member_join", fake_handle_guard_member_join)
    monkeypatch.setattr(bot, "resolve_welcome_channel", lambda guild, channel_id: SimpleNamespace(send=lambda *args, **kwargs: asyncio.sleep(0)))
    monkeypatch.setattr(bot, "build_welcome_embed", lambda member, guild_cfg: object())
    monkeypatch.setattr(bot, "resolve_role", lambda guild, role_id: None)

    member = SimpleNamespace(
        id=22,
        mention="<@22>",
        guild=SimpleNamespace(id=11),
    )

    asyncio.run(bot.on_member_join(member))
    asyncio.run(bot.on_member_join(member))

    assert calls == ["guard"]
