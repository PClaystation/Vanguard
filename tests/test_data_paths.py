import importlib
import os
import sys
import uuid
from pathlib import Path


def load_data_paths(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("VANGUARD_DATA_DIR", str(data_dir))
    sys.modules.pop("data_paths", None)
    module = importlib.import_module("data_paths")
    return module, data_dir


def test_resolve_data_file_migrates_legacy_file(monkeypatch, tmp_path):
    data_paths, data_dir = load_data_paths(monkeypatch, tmp_path)

    legacy_name = f"legacy-{uuid.uuid4().hex}.json"
    legacy_path = Path(data_paths.BASE_DIR) / legacy_name
    legacy_path.write_text('{"migrated": true}', encoding="utf-8")

    try:
        target = data_paths.resolve_data_file("migrated.json", legacy_filename=legacy_name)
        target_path = Path(target)

        assert target_path == data_dir / "migrated.json"
        assert target_path.exists()
        assert not legacy_path.exists()
    finally:
        if legacy_path.exists():
            os.remove(legacy_path)
