import json
from pathlib import Path

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


DD_VERSION_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
DD_CHAMPIONS_URL = (
    "https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/champion.json"
)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _get(url: str) -> httpx.Response:
    return httpx.get(url, timeout=15)


def fetch_latest_champions() -> dict:
    """
    Fetch latest champion data from Data Dragon and build id->name mapping.
    Returns a dict: {"version": str, "data": {id_int: {id, key, name, title}}}
    """
    versions_resp = _get(DD_VERSION_URL)
    versions_resp.raise_for_status()
    versions = versions_resp.json()
    latest = versions[0]

    champs_resp = _get(DD_CHAMPIONS_URL.format(version=latest))
    champs_resp.raise_for_status()
    champs_json = champs_resp.json()

    data = {}
    for champ in champs_json["data"].values():
        # champ["key"] is the numeric id as string
        try:
            cid = int(champ["key"])
        except (ValueError, KeyError):
            continue
        data[cid] = {
            "id": cid,
            "key": champ.get("id"),  # canonical string key, e.g., "Aatrox"
            "name": champ.get("name"),
            "title": champ.get("title"),
        }

    return {"version": latest, "data": data}


def ensure_champions_file(dest_path: Path) -> Path:
    """
    Ensure champions.json exists and is reasonably recent.
    If missing or empty, fetch and write it.
    """
    if not dest_path.exists():
        mapping = fetch_latest_champions()
        dest_path.write_text(json.dumps(mapping), encoding="utf-8")
        return dest_path

    # Basic sanity check (avoid heavy version logic here)
    try:
        existing = json.loads(dest_path.read_text(encoding="utf-8"))
        if not existing.get("data"):
            raise ValueError("no data")
    except Exception:
        mapping = fetch_latest_champions()
        dest_path.write_text(json.dumps(mapping), encoding="utf-8")
    return dest_path


def load_champion_map(base_dir: Path) -> dict:
    """
    Load champion mapping; fetch from Data Dragon if necessary.
    Returns the mapping dict {id_int: {id, key, name, title}}.
    """
    dest = base_dir / "champions.json"
    ensure_champions_file(dest)
    raw = json.loads(dest.read_text(encoding="utf-8"))
    return raw.get("data", {})