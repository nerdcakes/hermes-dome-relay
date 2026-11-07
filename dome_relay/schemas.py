"""Tool schemas shown to the model.

Each schema carries the tool name, a description, and a JSON Schema parameters
object. The descriptions say what a job is not allowed to do, so the model
does not treat an escalation as permission to post, send, spend, or read keys.
"""

from __future__ import annotations

from .service import NO_AUTHORITY


_ARTIFACT = {
    "type": "object",
    "additionalProperties": False,
    "required": ["label", "url"],
    "properties": {
        "label": {"type": "string", "maxLength": 128},
        "url": {
            "type": "string",
            "maxLength": 2048,
            "description": "Public https URL with no query string and no credentials. The plugin does not fetch it.",
        },
    },
}


ESCALATE = {
    "name": "dome_relay_escalate",
    "description": (
        "Save an escalation job for one target and return its id immediately. "
        "The worker later registers the job with the configured relay. The relay, not this plugin, "
        "wakes the executive. Do not invent a callback URL, a sender, or a hop. "
        + NO_AUTHORITY
    ),
    "parameters": {
        "type": "object",
        "additionalProperties": False,
        "required": ["target", "brief"],
        "properties": {
            "target": {
                "type": "string",
                "description": "Who should do the work. 1 to 64 characters: a-z, 0-9, underscore, or hyphen.",
            },
            "brief": {
                "type": "string",
                "description": "The task text. 1 to 16000 characters. Not a command to post, pay, or read secrets.",
            },
            "priority": {
                "type": "string",
                "enum": ["normal", "high", "urgent"],
                "description": "How soon the executive should look at it. Defaults to normal.",
            },
            "deadline": {
                "type": "string",
                "description": "Optional UTC timestamp (Z). Must be within 30 minutes. Defaults to 20 minutes from now.",
            },
            "artifacts": {
                "type": "array",
                "maxItems": 10,
                "items": _ARTIFACT,
                "description": "Optional reference links. They are not downloaded.",
            },
            "job_id": {
                "type": "string",
                "description": "Optional lowercase UUID. Omit it to let the plugin choose one.",
            },
            "execution_location": {
                "type": "string",
                "enum": ["cloud", "local"],
                "description": "cloud sends when the worker is up. local waits until this machine's heartbeat is fresh.",
            },
        },
    },
}


STATUS = {
    "name": "dome_relay_status",
    "description": (
        "Read one job and its latest result for this conversation. "
        "This is how a finished job comes back to the chat that started it. "
        "Results from another conversation are not returned."
    ),
    "parameters": {
        "type": "object",
        "additionalProperties": False,
        "required": ["job_id"],
        "properties": {
            "job_id": {"type": "string", "description": "The lowercase UUID returned by dome_relay_escalate."},
        },
    },
}


CANCEL = {
    "name": "dome_relay_cancel",
    "description": (
        "Stop retries and stop putting this job's result into later turns. "
        "If the relay already accepted the job, the plugin also asks the relay to cancel it. "
        "That cannot undo work the executive already started."
    ),
    "parameters": {
        "type": "object",
        "additionalProperties": False,
        "required": ["job_id"],
        "properties": {
            "job_id": {"type": "string", "description": "The lowercase UUID returned by dome_relay_escalate."},
        },
    },
}
