"""Non-secret settings and the one plugin secret.

Settings come from ``ctx.get_config``, which Hermes reads under
``plugins.entries.dome-relay.settings``. The only secret is ``DOME_RELAY_KEY``,
read through ``agent.secret_scope.get_secret``. This module does not read the
process environment or credential files.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import PluginError


PLUGIN_ID = "dome-relay"
SECRET_NAME = "DOME_RELAY_KEY"

# Locked with the relay. Config cannot raise these.
MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (5.0, 15.0)
JITTER_MAX_SECONDS = 1.0
JOB_LIFETIME_SECONDS = 30 * 60
ACTIVE_POLL_SECONDS = 15.0
TARGET_CONCURRENCY = 1

# The spec left the local-readiness window open. Sixty seconds is long enough
# that one missed 15s poll does not flap local jobs, and short enough that a
# stopped worker is noticed before the 30 minute job lifetime.
DEFAULT_HEARTBEAT_SECONDS = 60.0
DEFAULT_IDLE_BACKOFF_SECONDS = 60.0
DEFAULT_UNREACHABLE_BACKOFF_SECONDS = 60.0
UNREACHABLE_BACKOFF_CAP_SECONDS = 300.0
LEASE_SECONDS = 45.0


@dataclass(frozen=True)
class Settings:
    """What one profile is allowed to do. ``__repr__`` hides the relay key."""

    relay_url: str
    callback_host: str
    sender: str
    hop: int
    inject_context: bool
    inject_message: bool
    allow_loopback: bool
    poll_interval_seconds: float
    idle_backoff_seconds: float
    unreachable_backoff_seconds: float
    local_heartbeat_seconds: float
    relay_key: str

    def __repr__(self) -> str:
        return "Settings(relay key redacted)"

    @property
    def key_is_set(self) -> bool:
        return bool(self.relay_key)


def _as_bool(value, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off", ""}:
            return False
    return default


def _as_float(value, default: float, minimum: float) -> float:
    if isinstance(value, bool) or value is None:
        return default
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            return default
    else:
        return default
    if number < minimum:
        return minimum
    return number


def _as_hop(value) -> int:
    if isinstance(value, bool):
        raise PluginError("invalid_config", "hop must be 0 or 1.")
    if isinstance(value, int) and value in (0, 1):
        return value
    if isinstance(value, str) and value.strip() in {"0", "1"}:
        return int(value.strip())
    raise PluginError("invalid_config", "hop must be 0 or 1.")


def _as_sender(value) -> str:
    if value is None:
        return "hermes"
    if not isinstance(value, str):
        raise PluginError("invalid_config", "sender must be 1 to 128 characters.")
    text = value.strip()
    if not text:
        return "hermes"
    if len(text) > 128:
        raise PluginError("invalid_config", "sender must be 1 to 128 characters.")
    return text


def load_settings(get_config, secret: str | None) -> Settings:
    """Build settings from a ``ctx.get_config`` callable and an already-read key."""

    def cfg(key: str, default):
        try:
            value = get_config(key, default)
        except Exception as exc:
            raise PluginError("invalid_config", "Plugin settings could not be read.") from exc
        return default if value is None else value

    relay_url = cfg("relay_url", "")
    callback_host = cfg("callback_host", "")
    if not isinstance(relay_url, str):
        relay_url = ""
    if not isinstance(callback_host, str):
        callback_host = ""
    key = secret if isinstance(secret, str) else ""
    return Settings(
        relay_url=relay_url.strip(),
        callback_host=callback_host.strip().lower(),
        sender=_as_sender(cfg("sender", "hermes")),
        hop=_as_hop(cfg("hop", 1)),
        inject_context=_as_bool(cfg("inject_context", False), False),
        inject_message=_as_bool(cfg("inject_message", False), False),
        allow_loopback=_as_bool(cfg("allow_loopback", False), False),
        poll_interval_seconds=_as_float(
            cfg("poll_interval_seconds", ACTIVE_POLL_SECONDS), ACTIVE_POLL_SECONDS, ACTIVE_POLL_SECONDS
        ),
        idle_backoff_seconds=_as_float(
            cfg("idle_backoff_seconds", DEFAULT_IDLE_BACKOFF_SECONDS),
            DEFAULT_IDLE_BACKOFF_SECONDS,
            ACTIVE_POLL_SECONDS,
        ),
        unreachable_backoff_seconds=_as_float(
            cfg("unreachable_backoff_seconds", DEFAULT_UNREACHABLE_BACKOFF_SECONDS),
            DEFAULT_UNREACHABLE_BACKOFF_SECONDS,
            ACTIVE_POLL_SECONDS,
        ),
        local_heartbeat_seconds=_as_float(
            cfg("local_heartbeat_seconds", DEFAULT_HEARTBEAT_SECONDS),
            DEFAULT_HEARTBEAT_SECONDS,
            ACTIVE_POLL_SECONDS,
        ),
        relay_key=key,
    )


def read_relay_key() -> str | None:
    """Read ``DOME_RELAY_KEY`` through Hermes secret scope.

    Import failures and a missing scope become "no key", which the tools report
    without echoing a value. This function does not touch the process environment.
    """
    try:
        from agent.secret_scope import get_secret
    except ImportError:
        return None
    try:
        value = get_secret(SECRET_NAME)
    except Exception:
        return None
    if not isinstance(value, str) or not value:
        return None
    return value


def load_settings_from_context(ctx) -> Settings:
    return load_settings(ctx.get_config, read_relay_key())
