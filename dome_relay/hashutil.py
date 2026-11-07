"""Identity hash shared with the relay.

The relay hashes the envelope with ``attempt`` removed, after each timestamp
is rewritten as UTC ``YYYY-MM-DDTHH:MM:SSZ``. Key order on the wire does not
matter. A change of attempt (a retry) is the same job. A change of anything
else is a different job.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone


def format_utc(value: datetime) -> str:
    """UTC RFC3339, to the second, ending in Z."""
    value = value.astimezone(timezone.utc).replace(microsecond=0)
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: str) -> datetime:
    """Read a Z or +00:00 timestamp. The caller rejects other offsets."""
    text = value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("naive timestamp")
    return parsed.astimezone(timezone.utc)


def canonical_json(value) -> str:
    """Sorted keys, no extra spaces, non-ASCII left as characters."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(value: dict) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def identity_hash(envelope: dict) -> str:
    """SHA-256 of the envelope with ``attempt`` removed."""
    body = {key: val for key, val in envelope.items() if key != "attempt"}
    return content_hash(body)


def new_job_id() -> str:
    return str(uuid.uuid4())


def is_lowercase_uuid(value: str) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return str(parsed) == value
