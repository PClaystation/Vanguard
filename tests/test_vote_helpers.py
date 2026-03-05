import importlib
import sys
from datetime import timezone
from pathlib import Path


def load_vote(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("VANGUARD_DATA_DIR", str(data_dir))
    for module_name in ("vote", "data_paths"):
        sys.modules.pop(module_name, None)
    module = importlib.import_module("vote")
    return module, data_dir


def test_normalize_vote_sanitizes_payload(monkeypatch, tmp_path):
    vote, _ = load_vote(monkeypatch, tmp_path)

    payload = {
        "channel_id": "123",
        "message_id": "456",
        "starter_id": "789",
        "target_id": "321",
        "target_name": "Target",
        "votes": {"1": "support", "2": "against", "x": "support", "3": "invalid"},
        "duration_hours": 999,
        "finish_at": "2026-01-01T12:00:00+00:00",
        "min_account_days": -10,
        "min_join_days": -3,
    }

    normalized = vote._normalize_vote("1-2-3", payload)

    assert normalized is not None
    assert normalized["channel_id"] == 123
    assert normalized["message_id"] == 456
    assert normalized["starter_id"] == 789
    assert normalized["target_id"] == 321
    assert normalized["votes"] == {"1": "support", "2": "against"}
    assert normalized["duration_hours"] == 168
    assert normalized["min_account_days"] == 0
    assert normalized["min_join_days"] == 0


def test_get_vote_finish_time_supports_legacy_vote_id(monkeypatch, tmp_path):
    vote, _ = load_vote(monkeypatch, tmp_path)

    vote_id = "111-222-1700000000"
    finish_time = vote._get_vote_finish_time(vote_id, {"duration_hours": 2})

    assert finish_time is not None
    assert int(finish_time.timestamp()) == 1700000000 + 7200
    assert finish_time.tzinfo == timezone.utc


def test_vote_file_uses_configured_data_dir(monkeypatch, tmp_path):
    vote, data_dir = load_vote(monkeypatch, tmp_path)

    assert Path(vote.DATA_FILE).parent == data_dir
    vote.votes.clear()
    vote.save_votes()
    assert Path(vote.DATA_FILE).exists()
