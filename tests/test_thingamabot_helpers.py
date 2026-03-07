import importlib
import sys
from pathlib import Path


def load_thingamabot(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
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


def test_parse_allowed_guild_ids_skips_invalid_values(monkeypatch, tmp_path):
    bot, _ = load_thingamabot(monkeypatch, tmp_path)

    parsed = bot.parse_allowed_guild_ids(["123", "abc", -5, 456, None])
    assert parsed == {123, 456}


def test_verify_license_requires_verify_url_when_enforced(monkeypatch, tmp_path):
    monkeypatch.setenv("VANGUARD_REQUIRE_LICENSE", "true")
    monkeypatch.delenv("VANGUARD_LICENSE_VERIFY_URL", raising=False)

    bot, _ = load_thingamabot(monkeypatch, tmp_path)
    authorized, reason, allowed = bot.verify_license_sync(bot_user_id=None, guild_count=0)

    assert authorized is False
    assert "not configured" in reason
    assert allowed == set()
