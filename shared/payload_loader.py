"""Load job payload from env var, file path, stdin, or CLI argument."""

from __future__ import annotations

import json
import os
import sys
from typing import Any


def load_payload_dict() -> dict[str, Any]:
    """
    Resolve the job payload from the first available source:

    1. ``DATA_VALIDATOR_PAYLOAD`` env var (JSON string)
    2. ``DATA_VALIDATOR_PAYLOAD_FILE`` env var (path to a ``.json`` file)
    3. First CLI argument if it ends with ``.json``
    4. stdin when piped (non-TTY)
    """
    raw = os.environ.get("DATA_VALIDATOR_PAYLOAD")
    if raw:
        return _parse_payload(raw, source="DATA_VALIDATOR_PAYLOAD")

    path = (os.environ.get("DATA_VALIDATOR_PAYLOAD_FILE") or "").strip()
    if path:
        return _load_json_file(path)

    if len(sys.argv) > 1 and sys.argv[1].endswith(".json"):
        return _load_json_file(sys.argv[1])

    if not sys.stdin.isatty():
        return _parse_payload(sys.stdin.read(), source="stdin")

    raise RuntimeError(
        "No job payload found. Set DATA_VALIDATOR_PAYLOAD, "
        "DATA_VALIDATOR_PAYLOAD_FILE, pass a .json file path, or pipe JSON on stdin."
    )


def _load_json_file(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return _parse_payload(f.read(), source=path)


def _parse_payload(raw: str, *, source: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in {source}: {e}") from e
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Payload in {source} must be a JSON object")
    return parsed
