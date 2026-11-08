"""Helpers for the Dome Relay Hermes plugin.

The loader entrypoint is the ``__init__.py`` next to ``plugin.yaml``. This
package builds envelopes and talks to the relay. It does not wake Grok.
"""

from __future__ import annotations

from .hashutil import identity_hash


__version__ = "0.1.0"


def build_envelope(job_id: str, sender: str, target: str, brief: str, priority: str, deadline: str, callback: str, artifacts: list, created_at: str, attempt: int = 1) -> dict:
    """Build the JSON object the relay stores. ``attempt`` is not part of the hash."""
    return {
        "job_id": job_id,
        "from": sender,
        "target": target,
        "brief": brief,
        "priority": priority,
        "deadline": deadline,
        "callback_url": callback,
        "artifacts": artifacts,
        "created_at": created_at,
        "attempt": attempt,
    }


def envelope_hash(envelope: dict) -> str:
    return identity_hash(envelope)
