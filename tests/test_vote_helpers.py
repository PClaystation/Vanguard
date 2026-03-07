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


def test_normalize_vote_supports_multiple_choice_ballots(monkeypatch, tmp_path):
    vote, _ = load_vote(monkeypatch, tmp_path)

    payload = {
        "channel_id": 1,
        "message_id": 2,
        "starter_id": 3,
        "vote_type": "approval",
        "title": "Choose features",
        "options": [
            {"id": "a", "label": "A"},
            {"id": "b", "label": "B"},
            {"id": "c", "label": "C"},
        ],
        "ballot_mode": "multiple",
        "max_choices": 9,
        "ballots": {
            "10": ["a", "b", "a", "x"],
            "11": "c",
            "bad": ["a"],
        },
        "finish_at": "2026-01-01T12:00:00+00:00",
    }

    normalized = vote._normalize_vote("1-2-3", payload)

    assert normalized is not None
    assert normalized["ballot_mode"] == "multiple"
    assert normalized["max_choices"] == 3
    assert normalized["ballots"] == {"10": ["a", "b"], "11": ["c"]}
    assert normalized["votes"] == {"10": "a", "11": "c"}


def test_tally_vote_counts_multiple_choices(monkeypatch, tmp_path):
    vote, _ = load_vote(monkeypatch, tmp_path)

    payload = {
        "options": [
            {"id": "a", "label": "A"},
            {"id": "b", "label": "B"},
            {"id": "c", "label": "C"},
        ],
        "ballot_mode": "multiple",
        "max_choices": 2,
        "ballots": {
            "1": ["a", "b"],
            "2": ["a"],
            "3": ["b", "c"],
        },
    }

    tallies, turnout = vote.tally_vote(payload)
    assert turnout == 3
    assert tallies == {"a": 2, "b": 2, "c": 1}


def test_confidence_vote_requires_majority(monkeypatch, tmp_path):
    vote, _ = load_vote(monkeypatch, tmp_path)

    tied_payload = {
        "vote_type": "confidence",
        "ballot_mode": "single",
        "pass_threshold_percent": 50,
        "quorum": 0,
        "primary_option_id": "against",
        "options": [
            {"id": "against", "label": "Against"},
            {"id": "support", "label": "Support"},
        ],
        "ballots": {
            "1": ["against"],
            "2": ["support"],
        },
    }
    tied_tallies, tied_turnout = vote.tally_vote(tied_payload)
    tied_outcome = vote._compute_vote_outcome(tied_payload, tied_tallies, tied_turnout)
    assert tied_outcome["status"] == "failed"

    majority_payload = dict(tied_payload)
    majority_payload["ballots"] = {
        "1": ["against"],
        "2": ["against"],
        "3": ["support"],
    }
    majority_tallies, majority_turnout = vote.tally_vote(majority_payload)
    majority_outcome = vote._compute_vote_outcome(majority_payload, majority_tallies, majority_turnout)
    assert majority_outcome["status"] == "passed"


def test_election_outcome_can_trigger_runoff(monkeypatch, tmp_path):
    vote, _ = load_vote(monkeypatch, tmp_path)

    payload = {
        "vote_type": "election",
        "ballot_mode": "single",
        "runoff_enabled": True,
        "seats": 1,
        "options": [
            {"id": "a", "label": "Candidate A"},
            {"id": "b", "label": "Candidate B"},
            {"id": "c", "label": "Candidate C"},
        ],
        "ballots": {
            "1": ["a"],
            "2": ["a"],
            "3": ["b"],
            "4": ["b"],
            "5": ["c"],
        },
    }

    tallies, turnout = vote.tally_vote(payload)
    outcome = vote._compute_vote_outcome(payload, tallies, turnout)
    assert outcome["status"] == "runoff"
    assert outcome["runoff_candidates"] == ["a", "b"]
