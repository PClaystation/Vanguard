import importlib
import sys
from pathlib import Path


def load_thingamabot(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("VANGUARD_DATA_DIR", str(data_dir))
    for module_name in ("thingamabot", "vote", "data_paths"):
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
                "prefix": "abcdef",
                "welcome_channel_id": "42",
                "mod_role_ids": ["5", "not-an-id", 7],
                "mc_port": 99999,
                "guard_threshold": 1,
                "guard_window_seconds": 2,
                "guard_new_account_hours": 0,
                "guard_slowmode_seconds": 99999,
            }
        },
    }

    normalized = bot.normalize_settings(raw)
    cfg = normalized["guilds"]["123"]

    assert normalized["owner_only"] is True
    assert cfg["prefix"] == "abcde"
    assert cfg["welcome_channel_id"] == 42
    assert cfg["mod_role_ids"] == [5, 7]
    assert cfg["mc_port"] == 25565
    assert cfg["guard_threshold"] == 8
    assert cfg["guard_window_seconds"] == 30
    assert cfg["guard_new_account_hours"] == 24
    assert cfg["guard_slowmode_seconds"] == 30


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
