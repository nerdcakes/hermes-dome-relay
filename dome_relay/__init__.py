"""dome_relay: Team Astrum's Hermes plugin for agent-to-agent escalation.

Stub only. The real hooks get wired in once the Hermes plugin API is confirmed.
"""

__version__ = "0.1.0"


def build_brief(job_id: str, sender: str, brief: str, priority: str = "normal",
                callback_url: str | None = None) -> dict:
    """Package a task as the JSON brief the relay sends."""
    payload = {"job_id": job_id, "from": sender, "brief": brief, "priority": priority}
    if callback_url:
        payload["callback_url"] = callback_url
    return payload


def send_brief(payload: dict) -> dict:
    """POST a brief to the relay. Not implemented yet."""
    raise NotImplementedError("send_brief is coming next")
