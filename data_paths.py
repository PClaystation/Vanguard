import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.getenv("VANGUARD_DATA_DIR", os.path.join(BASE_DIR, "data")))


def ensure_data_dir() -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    return DATA_DIR


def resolve_data_file(filename: str, legacy_filename: str | None = None) -> str:
    data_dir = ensure_data_dir()
    data_path = os.path.join(data_dir, filename)
    legacy_path = os.path.join(BASE_DIR, legacy_filename or filename)
    if os.path.exists(legacy_path) and not os.path.exists(data_path):
        try:
            os.replace(legacy_path, data_path)
        except OSError:
            pass
    return data_path
