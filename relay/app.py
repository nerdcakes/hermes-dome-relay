"""Dome Relay: the always-on callback service.

Plain-language tour
1. The plugin POSTs a job to /jobs, with the relay key.
2. We save the job first (so a crash cannot forget it), then POST the task
   text to the Grok Bot routine webhook. A 200 from Grok means a run started.
   It does not mean the work is finished.
3. The executive POSTs /jobs/{job_id}/claim with the callback key. Only the
   first claim wins. A second claim, a canceled job, or an expired job is refused.
4. The executive POSTs /jobs/{job_id}/complete with the result.
5. The plugin GETs /results?since=... with the relay key and keeps its own cursor.
   Fetching does not delete anything.

Two different keys (both Fly secrets):
- DOME_RELAY_KEY is the plugin. It can register, cancel, and read results.
  It cannot claim or complete.
- DOME_RELAY_CALLBACK_KEY is the executive. It can claim and complete.
  It cannot register or read the result list.

The wake URL and the Grok key are also Fly secrets. They are never written to
logs, to the audit table, or to a response.

Locked rules (do not "improve" these without a spec change):
- Wake retries: 3 tries total, then give up. Waits are 5s and 15s plus up to 1s
  of jitter. A Retry-After header replaces that wait. Only transient failures
  are retried. 401 and 403 are never retried. A timeout after the request may
  have been sent is "uncertain" and is not retried either.
- One accepted (or held) job at a time per target.
- One hop. A result cannot start another job. Completing a job never wakes Grok.
- A job dies 30 minutes after we received it, or sooner if its deadline is sooner.
  The relay clock decides. The caller's completed_at cannot extend the life.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import random
import re
import socket
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from storage import JOB_LIFETIME, Store, format_utc, parse_utc


logger = logging.getLogger("dome_relay.service")

# How many times we will call the webhook for one job, including the first try.
MAX_WAKE_ATTEMPTS = 3
# After try 1 fails in a retryable way, wait 5s. After try 2, wait 15s.
RETRY_DELAYS_SECONDS = (5.0, 15.0)
# "Plus jitter" so a herd of retries does not land on the same instant.
JITTER_MAX_SECONDS = 1.0
# Grok's documented rule: only HTTP 200 means a run started.
WAKE_ACCEPTED_STATUS = 200
# These HTTP statuses are temporary. Anything else (except 200) is final.
TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}
# Authentication failures are a bad configuration, not a reason to keep calling.
NEVER_RETRY_HTTP = {401, 403}

MAX_BODY_BYTES = 64 * 1024
RESULT_LIMIT_DEFAULT = 100
RESULT_LIMIT_MAX = 100

ENV_RELAY_KEY = "DOME_RELAY_KEY"
ENV_CALLBACK_KEY = "DOME_RELAY_CALLBACK_KEY"
ENV_GROK_KEY = "DOME_RELAY_GROK_KEY"
ENV_GROK_URL = "DOME_RELAY_GROK_WEBHOOK_URL"
ENV_ORIGIN = "DOME_RELAY_PUBLIC_ORIGIN"
ENV_DB = "DOME_RELAY_DB"
ENV_LOOPBACK = "DOME_RELAY_ALLOW_LOOPBACK"

MIN_KEY_LENGTH = 16
TARGET_RE = re.compile(r"^[a-z0-9_-]{1,64}$")
HEX_HASH_RE = re.compile(r"^[0-9a-f]{64}$")

# Registration JSON may contain only these keys. The envelope is the task.
REGISTRATION_FIELDS = {"envelope", "envelope_hash", "hop", "parent_job_id"}
ENVELOPE_FIELDS = {
    "job_id",
    "from",
    "target",
    "brief",
    "priority",
    "deadline",
    "callback_url",
    "artifacts",
    "created_at",
    "attempt",
}
RESULT_FIELDS = {"job_id", "status", "result", "completed_at"}
PRIORITIES = {"normal", "high", "urgent"}
RESULT_STATUSES = {"done", "held", "failed"}

# Query-string names that usually carry a signature or a token. We also reject
# any query string at all on artifact links, because a signed URL hides the
# signature there. The relay never fetches artifact links.
TOKEN_QUERY_NAMES = {
    "token",
    "access_token",
    "id_token",
    "key",
    "secret",
    "signature",
    "sig",
    "x-amz-signature",
    "x-amz-security-token",
    "x-amz-credential",
    "awsaccesskeyid",
    "credential",
}


class AppError(Exception):
    """A refusal we can show the caller. The text must not contain secrets or task text."""

    def __init__(self, status: int, error: str, detail: str):
        super().__init__(error)
        self.status = status
        self.error = error
        self.detail = detail


class ConfigError(Exception):
    """Startup configuration is unusable. The message must not contain the secret URL or keys."""


def default_resolver(host: str, port: int):
    """DNS lookup used before we talk to the webhook host."""
    return socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)


@dataclass(frozen=True)
class Settings:
    """Process configuration. __repr__ hides secrets so a crash dump cannot print them."""

    relay_key: str
    callback_key: str
    grok_key: str
    grok_webhook_url: str
    public_origin: str
    db_path: str
    allow_loopback: bool
    clock: Callable[[], datetime]
    sleeper: Callable[[float], None]
    rng: random.Random
    transport: httpx.BaseTransport | None = None
    resolver: Callable = default_resolver

    def __repr__(self) -> str:
        return "Settings(secrets redacted)"


def _exit(message: str) -> None:
    # SystemExit is how a missing secret stops the process before it serves traffic.
    # The message names the variable, never the value.
    raise SystemExit(message)


def _require_key(name: str, value: str) -> str:
    if not value or len(value) < MIN_KEY_LENGTH or any(char.isspace() for char in value):
        _exit(
            f"{name} must be a random string of at least {MIN_KEY_LENGTH} "
            "characters, with no spaces. The value is not printed."
        )
    return value


def _keys_are_same(left: str, right: str) -> bool:
    return keys_match(left, right)


def settings_from_env() -> Settings:
    """Read Fly secrets (or local env vars) into Settings. Does not log the values."""
    allow_loopback = os.environ.get(ENV_LOOPBACK, "").strip() == "1"
    # On Fly this variable is always set. Loopback would point the wake at the
    # machine itself, so we refuse to boot rather than allow that in production.
    if allow_loopback and os.environ.get("FLY_APP_NAME"):
        _exit(f"{ENV_LOOPBACK} must not be set on Fly.")
    relay_key = _require_key(ENV_RELAY_KEY, os.environ.get(ENV_RELAY_KEY, ""))
    callback_key = _require_key(ENV_CALLBACK_KEY, os.environ.get(ENV_CALLBACK_KEY, ""))
    grok_key = _require_key(ENV_GROK_KEY, os.environ.get(ENV_GROK_KEY, ""))
    same_key = (
        _keys_are_same(relay_key, callback_key)
        or _keys_are_same(relay_key, grok_key)
        or _keys_are_same(callback_key, grok_key)
    )
    if same_key:
        _exit(
            f"{ENV_RELAY_KEY}, {ENV_CALLBACK_KEY}, and {ENV_GROK_KEY} must be three different values."
        )
    webhook = os.environ.get(ENV_GROK_URL, "").strip()
    origin = os.environ.get(ENV_ORIGIN, "").strip()
    if not webhook or not origin:
        _exit(
            f"{ENV_GROK_URL} and {ENV_ORIGIN} are required. "
            "Set them as Fly secrets. The values are not printed."
        )
    db_path = os.environ.get(ENV_DB, "").strip() or "/data/dome-relay.db"
    return Settings(
        relay_key=relay_key,
        callback_key=callback_key,
        grok_key=grok_key,
        grok_webhook_url=webhook,
        public_origin=origin,
        db_path=db_path,
        allow_loopback=allow_loopback,
        clock=lambda: datetime.now(timezone.utc),
        sleeper=time.sleep,
        rng=random.Random(),
    )


def keys_match(provided: str, expected: str) -> bool:
    """Compare two secrets without leaking which character differed, or how long they are.

    We hash both first so the comparison is always the same length. A plain ==
    can return faster on a mismatch and that timing can be measured.
    """
    if not isinstance(provided, str) or not isinstance(expected, str):
        return False
    provided_hash = hashlib.sha256(provided.encode("utf-8")).digest()
    expected_hash = hashlib.sha256(expected.encode("utf-8")).digest()
    return hmac.compare_digest(provided_hash, expected_hash)


def canonical_json(value) -> str:
    """Stable JSON: sorted keys, no extra spaces, UTF-8 characters left as themselves.

    The plugin must hash the envelope with this same encoding. Key order on the
    wire does not matter, because we sort before hashing.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(value: dict) -> str:
    """SHA-256 of one JSON object, used to spot an identical result."""
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def identity_hash(envelope: dict) -> str:
    """SHA-256 of the envelope with `attempt` removed.

    SPEC says the identity hash ignores attempt, so a retry of the same task
    (attempt 2 or 3) is the same job, not a new one. The algorithm itself is
    not named in the spec; this is the choice documented in the README.
    Timestamps are normalized to UTC `Z` form before this runs.
    """
    body = {key: val for key, val in envelope.items() if key != "attempt"}
    return content_hash(body)


def _address_is_blocked(ip: ipaddress.IPv4Address | ipaddress.IPv6Address, allow_loopback: bool) -> bool:
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        return _address_is_blocked(mapped, allow_loopback)
    if allow_loopback and ip.is_loopback:
        return False
    # is_global is false for private, loopback, link-local, reserved, and metadata ranges.
    return not ip.is_global


def _ips_for_host(host: str, port: int, resolver: Callable, allow_loopback: bool) -> None:
    """Raise ConfigError if this host is not a public address we are willing to call.

    The check runs at startup and again immediately before every wake, so a DNS
    name that later starts pointing at a private address is refused. We do not
    follow redirects, which is the other half of that guard.
    """
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        ips = [literal]
    else:
        if host.lower() == "localhost" or host.lower().endswith(".localhost"):
            if not allow_loopback:
                raise ConfigError("webhook host is loopback")
        try:
            infos = resolver(host, port or 443)
        except socket.gaierror as exc:
            raise ConfigError("webhook host did not resolve") from exc
        ips = []
        for info in infos:
            ips.append(ipaddress.ip_address(info[4][0]))
        if not ips:
            raise ConfigError("webhook host did not resolve")
    for ip in ips:
        if _address_is_blocked(ip, allow_loopback):
            raise ConfigError("destination is not a public address")


def _split_url(url: str) -> object:
    if not isinstance(url, str) or not url or any(char.isspace() for char in url):
        raise ConfigError("url is missing or contains spaces")
    return urlsplit(url)


def _checked_netloc(parts, allow_loopback: bool, resolver: Callable) -> str:
    if parts.username or parts.password:
        raise ConfigError("url must not contain credentials")
    if parts.query or parts.fragment:
        raise ConfigError("url must not contain a query or fragment")
    scheme = parts.scheme.lower()
    host = parts.hostname
    if not host:
        raise ConfigError("url is missing a host")
    host = host.lower()
    if scheme != "https":
        if not (allow_loopback and scheme == "http"):
            raise ConfigError("url must be https")
    port = parts.port
    _ips_for_host(host, port or (443 if scheme == "https" else 80), resolver, allow_loopback)
    # Confirm loopback exception really is loopback when the scheme is plain http.
    if scheme == "http":
        _require_loopback_host(host, port or 80, resolver)
    display_host = f"[{host}]" if ":" in host else host
    if port:
        return f"{display_host}:{port}"
    return display_host


def _require_loopback_host(host: str, port: int, resolver: Callable) -> None:
    """Plain http is allowed only when every address is loopback (tests)."""
    try:
        ips = [ipaddress.ip_address(host)]
    except ValueError:
        infos = resolver(host, port)
        ips = [ipaddress.ip_address(info[4][0]) for info in infos]
    if not ips or not all(ip.is_loopback for ip in ips):
        raise ConfigError("plain http is only allowed for loopback in tests")


def prepare_settings(settings: Settings) -> Settings:
    """Check the webhook URL and the public origin. Normalize them without logging them."""
    webhook_parts = _split_url(settings.grok_webhook_url)
    origin_parts = _split_url(settings.public_origin)
    if origin_parts.path not in ("", "/"):
        raise ConfigError("public origin must not include a path")
    webhook_netloc = _checked_netloc(
        webhook_parts, settings.allow_loopback, settings.resolver
    )
    origin_netloc = _checked_netloc(
        origin_parts, settings.allow_loopback, settings.resolver
    )
    webhook = urlunsplit(
        (webhook_parts.scheme.lower(), webhook_netloc, webhook_parts.path or "/", "", "")
    )
    origin = urlunsplit((origin_parts.scheme.lower(), origin_netloc, "", "", ""))
    return replace(settings, grok_webhook_url=webhook, public_origin=origin)


def assert_webhook_still_safe(settings: Settings) -> None:
    """Re-check DNS right before a wake. Raises ConfigError, which the caller turns into a refusal."""
    parts = urlsplit(settings.grok_webhook_url)
    _checked_netloc(parts, settings.allow_loopback, settings.resolver)


def parse_bearer(header: str | None) -> str | None:
    """Pull the token out of `Authorization: Bearer <token>`. Scheme case does not matter."""
    if not header or not isinstance(header, str):
        return None
    parts = header.strip().split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    if not token or any(char.isspace() for char in token):
        return None
    return token


def identify_caller(header: str | None, settings: Settings) -> str | None:
    """Return 'relay' or 'callback', or None if the header is missing or wrong.

    Both keys are always compared, so the time taken does not reveal which one matched.
    """
    token = parse_bearer(header)
    if token is None:
        return None
    relay_ok = keys_match(token, settings.relay_key)
    callback_ok = keys_match(token, settings.callback_key)
    if relay_ok:
        return "relay"
    if callback_ok:
        return "callback"
    return None


def require_role(request: Request, role: str) -> None:
    settings: Settings = request.app.state.settings
    store: Store = request.app.state.store
    who = identify_caller(request.headers.get("authorization"), settings)
    if who is None:
        store.audit("auth_failure", None, {"reason": "rejected"}, settings.clock())
        raise AppError(401, "unauthorized", "Authorization was missing or not accepted.")
    if who != role:
        store.audit("auth_failure", None, {"reason": "wrong_role"}, settings.clock())
        raise AppError(403, "forbidden", "This key cannot call that endpoint.")


def parse_wire_time(value, field_name: str) -> datetime:
    """Accept only UTC RFC3339. Offsets other than Z or +00:00 are rejected."""
    if not isinstance(value, str) or not value:
        raise AppError(400, "invalid_timestamp", f"{field_name} must be a UTC timestamp.")
    text = value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise AppError(400, "invalid_timestamp", f"{field_name} must be a UTC timestamp.")
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise AppError(400, "invalid_timestamp", f"{field_name} must be UTC (Z or +00:00).")
    return parsed.astimezone(timezone.utc)


def parse_job_id(value, field_name: str = "job_id") -> str:
    if not isinstance(value, str):
        raise AppError(400, "invalid_job", f"{field_name} must be a lowercase UUID.")
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        raise AppError(400, "invalid_job", f"{field_name} must be a lowercase UUID.")
    if str(parsed) != value:
        raise AppError(400, "invalid_job", f"{field_name} must be a lowercase UUID.")
    return value


def _reject_query_url(url: str, field_name: str) -> str:
    """Artifacts are references only. Reject tokens, credentials, and private hosts.

    We do not resolve DNS for these, and we never fetch them. Resolving a
    caller-supplied name would itself be a trick (it can point somewhere private
    only at lookup time). IP literals and localhost are still rejected.
    """
    if not isinstance(url, str) or not url or len(url) > 2048:
        raise AppError(400, "invalid_url", f"{field_name} must be an https URL.")
    if any(char.isspace() for char in url):
        raise AppError(400, "invalid_url", f"{field_name} must be an https URL.")
    parts = urlsplit(url)
    query_names = {key.lower() for key, _value in parse_qsl(parts.query)}
    # Any query string is refused. Signed links hide the signature there, and
    # TOKEN_QUERY_NAMES is the list we are specifically guarding against.
    if parts.username or parts.password or parts.fragment or parts.query or (query_names & TOKEN_QUERY_NAMES):
        raise AppError(400, "invalid_url", f"{field_name} must not carry credentials or a query.")
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    if not host:
        raise AppError(400, "invalid_url", f"{field_name} must be an https URL.")
    if scheme != "https":
        raise AppError(400, "invalid_url", f"{field_name} must be https.")
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        raise AppError(400, "invalid_url", f"{field_name} must be a public https URL.")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None and not ip.is_global:
        raise AppError(400, "invalid_url", f"{field_name} must be a public https URL.")
    return url


def _walk_strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _walk_strings(key)
            yield from _walk_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_strings(item)


def reject_if_secret_present(value, settings: Settings) -> None:
    """Refuse the body if a configured key or the webhook URL was pasted into it."""
    secrets = (settings.relay_key, settings.callback_key, settings.grok_key, settings.grok_webhook_url)
    for text in _walk_strings(value):
        for secret in secrets:
            if secret and secret in text:
                raise AppError(
                    400,
                    "secret_in_body",
                    "The body contains a configured secret. Secrets stay in headers, not in JSON.",
                )


def _require_object(value, name: str) -> dict:
    if not isinstance(value, dict):
        raise AppError(400, "invalid_json", f"{name} must be a JSON object.")
    return value


def _reject_unknown(value: dict, allowed: set[str], name: str) -> None:
    extra = set(value) - allowed
    if extra:
        raise AppError(400, "invalid_json", f"{name} has a field that is not allowed.")


def validate_envelope(envelope: dict) -> dict:
    """Check the task envelope. Error text names the field, never the task text."""
    _require_object(envelope, "envelope")
    _reject_unknown(envelope, ENVELOPE_FIELDS, "envelope")
    missing = ENVELOPE_FIELDS - set(envelope)
    if missing:
        raise AppError(400, "invalid_envelope", "envelope is missing a required field.")
    job_id = parse_job_id(envelope["job_id"])
    sender = envelope["from"]
    if not isinstance(sender, str) or not 1 <= len(sender) <= 128:
        raise AppError(400, "invalid_envelope", "from must be 1 to 128 characters.")
    target = envelope["target"]
    if not isinstance(target, str) or TARGET_RE.fullmatch(target) is None:
        raise AppError(400, "invalid_envelope", "target must match the allowed pattern.")
    brief = envelope["brief"]
    if not isinstance(brief, str) or not 1 <= len(brief) <= 16000:
        raise AppError(400, "invalid_envelope", "brief must be 1 to 16000 characters.")
    if envelope["priority"] not in PRIORITIES:
        raise AppError(400, "invalid_envelope", "priority must be normal, high, or urgent.")
    deadline = parse_wire_time(envelope["deadline"], "deadline")
    created_at = parse_wire_time(envelope["created_at"], "created_at")
    callback = envelope["callback_url"]
    if not isinstance(callback, str) or len(callback) > 2048:
        raise AppError(400, "invalid_envelope", "callback_url is not valid.")
    artifacts = envelope["artifacts"]
    if not isinstance(artifacts, list) or len(artifacts) > 10:
        raise AppError(400, "invalid_envelope", "artifacts must be a list of at most 10 items.")
    cleaned_artifacts = []
    for item in artifacts:
        if not isinstance(item, dict) or set(item) != {"label", "url"}:
            raise AppError(400, "invalid_envelope", "each artifact needs label and url only.")
        label = item["label"]
        if not isinstance(label, str) or len(label) > 128:
            raise AppError(400, "invalid_envelope", "artifact label is too long.")
        cleaned_artifacts.append({"label": label, "url": _reject_query_url(item["url"], "artifact url")})
    attempt = envelope["attempt"]
    if isinstance(attempt, bool) or not isinstance(attempt, int) or not 1 <= attempt <= 3:
        raise AppError(400, "invalid_envelope", "attempt must be an integer from 1 to 3.")
    # Return a fresh object so later steps cannot be surprised by extra aliases.
    return {
        "job_id": job_id,
        "from": sender,
        "target": target,
        "brief": brief,
        "priority": envelope["priority"],
        "deadline": format_utc(deadline),
        "callback_url": callback,
        "artifacts": cleaned_artifacts,
        "created_at": format_utc(created_at),
        "attempt": attempt,
    }


def expected_callback(origin: str, job_id: str) -> str:
    """The only callback URL we will accept for this job.

    SPEC also mentions a bare /complete path. This service uses the path from
    the task brief: /jobs/{job_id}/complete. The URL contains no key.
    """
    return f"{origin}/jobs/{job_id}/complete"


def check_callback(envelope: dict, settings: Settings) -> None:
    expected = expected_callback(settings.public_origin, envelope["job_id"])
    provided = envelope["callback_url"]
    # compare_digest wants equal lengths. A mismatch is a normal 400, not a secret compare,
    # but we still avoid echoing the provided URL back to the caller.
    if not isinstance(provided, str) or len(provided) != len(expected) or not hmac.compare_digest(provided, expected):
        raise AppError(
            400,
            "callback_mismatch",
            "callback_url must be this relay's /jobs/{job_id}/complete URL.",
        )


def check_deadline_window(envelope: dict, now: datetime) -> datetime:
    """Reject deadlines in the past or more than 30 minutes after we received the job."""
    deadline = parse_utc(envelope["deadline"])
    created_at = parse_utc(envelope["created_at"])
    if deadline <= created_at:
        raise AppError(400, "invalid_deadline", "deadline must be after created_at.")
    if deadline <= now:
        raise AppError(400, "invalid_deadline", "deadline must be in the future on the relay clock.")
    if deadline > now + JOB_LIFETIME:
        raise AppError(
            400,
            "invalid_deadline",
            "deadline must be within 30 minutes of when the relay received the job.",
        )
    return deadline


def validate_registration(body: dict, settings: Settings, now: datetime) -> dict:
    """Turn a /jobs body into the fields we store, or raise AppError."""
    _require_object(body, "body")
    _reject_unknown(body, REGISTRATION_FIELDS, "body")
    if "envelope" not in body or "envelope_hash" not in body:
        raise AppError(400, "invalid_json", "body needs envelope and envelope_hash.")
    reject_if_secret_present(body, settings)
    envelope = validate_envelope(body["envelope"])
    # Hash the normalized envelope (timestamps in our Z form) so both sides share one encoding.
    # The plugin should hash the same canonical JSON, with attempt removed.
    computed = identity_hash(envelope)
    provided_hash = body["envelope_hash"]
    if not isinstance(provided_hash, str) or HEX_HASH_RE.fullmatch(provided_hash.lower()) is None:
        raise AppError(400, "bad_hash", "envelope_hash must be a 64-character sha256 hex string.")
    if not hmac.compare_digest(provided_hash.lower(), computed):
        raise AppError(
            400,
            "bad_hash",
            "envelope_hash does not match the envelope. Hash canonical JSON with attempt removed.",
        )
    check_callback(envelope, settings)
    hop = body.get("hop", 0)
    if isinstance(hop, bool) or not isinstance(hop, int) or hop < 0:
        raise AppError(400, "invalid_hop", "hop must be an integer.")
    # One external hop. hop 0 (not yet counted) and hop 1 (this is the hop) are allowed.
    # hop 2 or more would be a second dispatch. A parent id means this job was created
    # from a result, which is also a second hop. Either one is refused and not woken.
    parent = body.get("parent_job_id", None)
    if parent is not None:
        raise AppError(
            409,
            "hop_limit",
            "Results cannot be dispatched again. This relay allows one hop.",
        )
    if hop > 1:
        raise AppError(
            409,
            "hop_limit",
            "Results cannot be dispatched again. This relay allows one hop.",
        )
    deadline = check_deadline_window(envelope, now)
    expires_at = min(deadline, now + JOB_LIFETIME)
    return {
        "job_id": envelope["job_id"],
        "target": envelope["target"],
        "deadline": envelope["deadline"],
        "envelope_hash": computed,
        "hop": hop,
        "envelope": envelope,
        "expires_at": expires_at,
    }


def validate_result(body: dict, job_id: str) -> dict:
    _require_object(body, "body")
    _reject_unknown(body, RESULT_FIELDS, "body")
    missing = RESULT_FIELDS - set(body)
    if missing:
        raise AppError(400, "invalid_result", "result is missing a required field.")
    body_job = parse_job_id(body["job_id"])
    if body_job != job_id:
        raise AppError(400, "job_id_mismatch", "The job_id in the body and the URL do not match.")
    if body["status"] not in RESULT_STATUSES:
        raise AppError(400, "invalid_result", "status must be done, held, or failed.")
    result_text = body["result"]
    if not isinstance(result_text, str) or len(result_text) > 32000:
        raise AppError(400, "invalid_result", "result must be a string of at most 32000 characters.")
    completed_at = parse_wire_time(body["completed_at"], "completed_at")
    normalized = {
        "job_id": body_job,
        "status": body["status"],
        "result": result_text,
        "completed_at": format_utc(completed_at),
    }
    return normalized


def decode_cursor(value: str) -> int:
    """Cursors are opaque tokens of the form c<number>. Clients pass next_cursor back unchanged."""
    if value == "":
        return 0
    if not isinstance(value, str) or len(value) < 2 or not value.startswith("c") or not value[1:].isdigit():
        raise AppError(400, "bad_cursor", "since must be a cursor this relay returned.")
    return int(value[1:])


def encode_cursor(value: int) -> str:
    return f"c{value}"


def job_view(job: dict, deduped: bool) -> dict:
    """What /jobs returns. No task text, no keys, no webhook URL."""
    return {
        "job_id": job["job_id"],
        "state": job["state"],
        "wake_status": job["wake_status"],
        "expires_at": job["expires_at"],
        "received_at": job["received_at"],
        "deduped": deduped,
    }


def make_client(settings: Settings) -> httpx.Client:
    """HTTP client for the wake call.

    follow_redirects is off so a 3xx cannot send the Grok key to another host.
    trust_env is off so HTTP_PROXY on the machine cannot siphon that key either.
    """
    timeout = httpx.Timeout(10.0, connect=5.0)
    kwargs = {"timeout": timeout, "follow_redirects": False, "trust_env": False}
    if settings.transport is not None:
        kwargs["transport"] = settings.transport
    return httpx.Client(**kwargs)


def parse_retry_after(value: str | None, now: datetime) -> float | None:
    """Read a Retry-After header as a number of seconds, or None if it is absent or junk."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            when = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        seconds = (when - now).total_seconds()
    if seconds < 0:
        return 0.0
    return seconds


def classify_exception(exc: Exception) -> str:
    """Split transport errors into 'safe to retry' and 'do not retry'.

    A connect failure means the request never left. A read or write timeout
    means it might have left, so the routine might have started. That is uncertain,
    and we must not wake again.
    """
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError, httpx.PoolTimeout)):
        return "transient"
    if isinstance(exc, (httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError)):
        return "uncertain"
    if isinstance(exc, httpx.TimeoutException):
        return "uncertain"
    if isinstance(exc, httpx.NetworkError):
        return "transient"
    return "uncertain"


def classify_response(response: httpx.Response, now: datetime) -> tuple[str, int, float | None]:
    code = response.status_code
    if 300 <= code < 400:
        # Do not read Location and do not follow it.
        return "rejected", code, None
    if code == WAKE_ACCEPTED_STATUS:
        return "accepted", code, None
    retry_after = parse_retry_after(response.headers.get("retry-after"), now)
    if code in NEVER_RETRY_HTTP:
        return "rejected", code, None
    if code in TRANSIENT_HTTP:
        return "transient", code, retry_after
    return "rejected", code, None


def _sleep_or_stop(settings: Settings, attempt: int, retry_after: float | None, expires_at: datetime) -> bool:
    """Sleep before the next try. Return False if the wait would pass the deadline.

    attempt is the try that just failed (1 or 2). There is no sleep after the last try.
    """
    if retry_after is not None:
        base = retry_after
    else:
        base = RETRY_DELAYS_SECONDS[attempt - 1]
    delay = base + settings.rng.uniform(0.0, JITTER_MAX_SECONDS)
    now = settings.clock()
    if now + timedelta(seconds=delay) >= expires_at:
        logger.info("event=retry_skipped_deadline attempt=%s", attempt)
        return False
    settings.sleeper(delay)
    return True


def run_wake(settings: Settings, store: Store, job_id: str, envelope: dict, expires_at: datetime) -> str:
    """POST the envelope to the routine webhook. Return accepted, rejected, or uncertain.

    The envelope is the body. The key is only the Authorization header.
    Completing a job must never call this. A second hop must never call this.
    """
    outcome = "rejected"
    try:
        with make_client(settings) as client:
            for attempt in range(1, MAX_WAKE_ATTEMPTS + 1):
                current = store.get(job_id)
                if current is not None and current["state"] == "canceled":
                    outcome = "rejected"
                    logger.info("event=wake_skipped job_id=%s reason=canceled", job_id)
                    break
                try:
                    assert_webhook_still_safe(settings)
                except ConfigError:
                    store.add_attempt(job_id, attempt, "blocked", None, settings.clock())
                    outcome = "rejected"
                    logger.warning("event=wake_blocked job_id=%s attempt=%s", job_id, attempt)
                    break
                try:
                    response = client.post(
                        settings.grok_webhook_url,
                        json=envelope,
                        headers={"Authorization": f"Bearer {settings.grok_key}"},
                    )
                except Exception as exc:
                    kind = classify_exception(exc)
                    # Log the exception type only. str(exc) from httpx includes the URL.
                    logger.warning(
                        "event=wake_transport job_id=%s attempt=%s error_type=%s kind=%s",
                        job_id,
                        attempt,
                        type(exc).__name__,
                        kind,
                    )
                    store.add_attempt(job_id, attempt, kind, None, settings.clock())
                    if kind == "uncertain":
                        outcome = "uncertain"
                        break
                    if attempt == MAX_WAKE_ATTEMPTS or not _sleep_or_stop(
                        settings, attempt, None, expires_at
                    ):
                        outcome = "rejected"
                        break
                    continue
                kind, code, retry_after = classify_response(response, settings.clock())
                store.add_attempt(job_id, attempt, kind, code, settings.clock())
                logger.info(
                    "event=wake_response job_id=%s attempt=%s outcome=%s status=%s",
                    job_id,
                    attempt,
                    kind,
                    code,
                )
                if kind == "accepted":
                    outcome = "accepted"
                    break
                if kind == "rejected":
                    outcome = "rejected"
                    break
                if attempt == MAX_WAKE_ATTEMPTS or not _sleep_or_stop(
                    settings, attempt, retry_after, expires_at
                ):
                    outcome = "rejected"
                    break
    finally:
        # Always leave 'dispatching', even if we crash out, so a restart does not
        # sit in that state any longer than this call. finish_wake also wipes the text.
        store.finish_wake(job_id, outcome, settings.clock())
    return outcome


async def read_json_object(request: Request) -> dict:
    """Read a small JSON object. Secrets in the query string are refused and not logged."""
    if request.url.query:
        logger.warning("event=rejected_query path=%s", request.url.path)
        raise AppError(400, "query_not_allowed", "Send data in JSON, not in the URL.")
    content_type = request.headers.get("content-type", "")
    if not content_type.lower().startswith("application/json"):
        raise AppError(415, "unsupported_media_type", "Content-Type must be application/json.")
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_BODY_BYTES:
            raise AppError(413, "body_too_large", "The body is larger than 64KB.")
        chunks.append(chunk)
    raw = b"".join(chunks)
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise AppError(400, "invalid_json", "The body is not valid JSON.")
    if not isinstance(data, dict):
        raise AppError(400, "invalid_json", "The body must be a JSON object.")
    return data


def _error_payload(error: str, detail: str) -> dict:
    return {"error": error, "detail": detail}


CLAIM_ERRORS = {
    "not_found": (404, "not_found", "No job with that id."),
    "canceled": (409, "canceled", "This job was canceled."),
    "expired": (409, "expired", "The job is past its deadline on the relay clock."),
    "already_claimed": (409, "already_claimed", "This job was already claimed. It will not be claimed again."),
    "terminal": (409, "terminal", "This job already has a terminal result."),
    "not_claimable": (409, "not_claimable", "This job cannot be claimed."),
    "slot_busy": (
        409,
        "slot_busy",
        "This target already has an accepted job. Wait until it finishes, is canceled, or expires.",
    ),
}


def create_app(settings: Settings) -> FastAPI:
    """Build the ASGI app. Importing this module does not read secrets or open a database."""
    logger.setLevel(logging.INFO)
    settings = prepare_settings(settings)
    store = Store(settings.db_path)
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # The database is opened above, before the app starts serving.
        # Closing it here covers a normal shutdown. A crash is recovered on the next boot.
        try:
            yield
        finally:
            store.close()

    # docs are off so the public host does not offer an interactive "try it" page.
    app = FastAPI(
        title="Dome Relay",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.store = store

    @app.exception_handler(AppError)
    async def handle_app_error(_request: Request, exc: AppError):
        return JSONResponse(_error_payload(exc.error, exc.detail), status_code=exc.status)

    @app.exception_handler(Exception)
    async def handle_unexpected(_request: Request, exc: Exception):
        # Type name only. The exception text can contain a URL.
        logger.error("event=unhandled error_type=%s", type(exc).__name__)
        return JSONResponse(
            _error_payload("internal", "The relay hit an unexpected error."),
            status_code=500,
        )

    @app.get("/healthz")
    def healthz():
        """Fly calls this with no key. We also expire overdue jobs here so a slot frees itself."""
        try:
            store.expire_overdue(settings.clock())
            store.ping()
        except Exception:
            logger.error("event=healthz_failed")
            return JSONResponse(_error_payload("unhealthy", "database check failed"), status_code=500)
        return {"ok": True}

    @app.post("/jobs")
    async def register_job(request: Request):
        require_role(request, "relay")
        body = await read_json_object(request)
        now = settings.clock()
        registration = validate_registration(body, settings, now)
        kind, job = store.insert_job(
            job_id=registration["job_id"],
            target=registration["target"],
            deadline=registration["deadline"],
            envelope_hash=registration["envelope_hash"],
            hop=registration["hop"],
            envelope_json=canonical_json(registration["envelope"]),
            received_at=now,
            expires_at=registration["expires_at"],
        )
        if kind == "conflict":
            raise AppError(
                409,
                "conflict",
                "This job_id was already registered with different contents.",
            )
        # Duplicate of a job we already tried to wake: return it, do not wake again.
        # Duplicate of a job that is still 'pending' (we crashed before the send):
        # begin_wake wins exactly once and that caller sends.
        started = store.begin_wake(registration["job_id"], settings.clock())
        if started is None:
            current = store.get(registration["job_id"])
            status_code = 200 if kind == "duplicate" else 201
            return JSONResponse(job_view(current, deduped=kind == "duplicate"), status_code=status_code)
        envelope = json.loads(started["envelope_json"])
        # The wake sleeps and talks to the network. Run it off the event loop.
        await asyncio.to_thread(
            run_wake,
            settings,
            store,
            registration["job_id"],
            envelope,
            parse_utc(started["expires_at"]),
        )
        current = store.get(registration["job_id"])
        status_code = 200 if kind == "duplicate" else 201
        return JSONResponse(job_view(current, deduped=kind == "duplicate"), status_code=status_code)

    @app.post("/jobs/{job_id}/claim")
    def claim_job(job_id: str, request: Request):
        require_role(request, "callback")
        if request.url.query:
            raise AppError(400, "query_not_allowed", "Send data in JSON, not in the URL.")
        if request.headers.get("content-length") not in (None, "0"):
            raise AppError(400, "body_not_allowed", "Claim does not take a body.")
        job_id = parse_job_id(job_id)
        result = store.claim(job_id, settings.clock())
        if "error" in result:
            status, error, detail = CLAIM_ERRORS[result["error"]]
            raise AppError(status, error, detail)
        job = result["job"]
        return {
            "job_id": job["job_id"],
            "status": "claimed",
            "state": job["state"],
            "expires_at": job["expires_at"],
        }

    @app.post("/jobs/{job_id}/complete")
    async def complete_job(job_id: str, request: Request):
        require_role(request, "callback")
        job_id = parse_job_id(job_id)
        body = await read_json_object(request)
        reject_if_secret_present(body, settings)
        # A result is not a new job. We never wake from this endpoint, even if the
        # body tries to smuggle extra fields (those are rejected as unknown).
        result_body = validate_result(body, job_id)
        stored = store.complete(
            job_id=job_id,
            status=result_body["status"],
            result_text=result_body["result"],
            completed_at=result_body["completed_at"],
            body_hash=content_hash(result_body),
            now=settings.clock(),
        )
        if "error" in stored:
            if stored["error"] == "duplicate_result":
                raise AppError(
                    409,
                    "duplicate_result",
                    "This exact result was already stored. The first one stands.",
                )
            if stored["error"] == "conflict":
                raise AppError(
                    409,
                    "conflict",
                    "A different terminal result was already stored. The first one stands.",
                )
            if stored["error"] == "not_found":
                raise AppError(404, "not_found", "No job with that id.")
            if stored["error"] == "expired":
                raise AppError(
                    409,
                    "expired",
                    "The job is past its deadline on the relay clock. The late result was quarantined.",
                )
            if stored["error"] == "canceled":
                raise AppError(409, "canceled", "This job was canceled.")
            if stored["error"] == "not_claimed":
                raise AppError(409, "not_claimed", "Claim the job before sending a result.")
            raise AppError(409, stored["error"], "The result was not stored.")
        return JSONResponse(
            {
                "job_id": job_id,
                "status": result_body["status"],
                "state": stored["state"],
                "cursor": encode_cursor(stored["cursor"]),
            },
            status_code=201,
        )

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str, request: Request):
        """Plugin-only. Stops a later claim or result. Does not stop a running routine."""
        require_role(request, "relay")
        if request.url.query:
            raise AppError(400, "query_not_allowed", "Send data in JSON, not in the URL.")
        if request.headers.get("content-length") not in (None, "0"):
            raise AppError(400, "body_not_allowed", "Cancel does not take a body.")
        job_id = parse_job_id(job_id)
        result = store.cancel(job_id, settings.clock())
        if result.get("error") == "not_found":
            raise AppError(404, "not_found", "No job with that id.")
        if result.get("error") == "terminal":
            raise AppError(409, "terminal", "This job already has a terminal result.")
        if result.get("error") == "expired":
            raise AppError(409, "expired", "The job is past its deadline on the relay clock.")
        job = result["job"]
        return {
            "job_id": job["job_id"],
            "state": "canceled",
            "already_canceled": bool(result.get("already")),
        }

    @app.get("/results")
    def list_results(request: Request):
        """Non-destructive read. We do not delete rows and we do not store the caller's cursor."""
        require_role(request, "relay")
        allowed = {"since", "limit"}
        if not set(request.query_params.keys()) <= allowed:
            raise AppError(400, "bad_query", "Only since and limit are allowed.")
        since_raw = request.query_params.get("since", "c0")
        limit_raw = request.query_params.get("limit", str(RESULT_LIMIT_DEFAULT))
        since = decode_cursor(since_raw)
        if not isinstance(limit_raw, str) or not limit_raw.isdigit():
            raise AppError(400, "bad_query", "limit must be a whole number from 1 to 100.")
        limit = int(limit_raw)
        if limit < 1 or limit > RESULT_LIMIT_MAX:
            raise AppError(400, "bad_query", "limit must be a whole number from 1 to 100.")
        rows = store.list_results(since, limit)
        entries = [
            {
                "cursor": encode_cursor(int(row["cursor"])),
                "result": {
                    "job_id": row["job_id"],
                    "status": row["status"],
                    "result": row["result_text"],
                    "completed_at": row["completed_at"],
                },
            }
            for row in rows
        ]
        if entries:
            next_cursor = entries[-1]["cursor"]
        else:
            next_cursor = encode_cursor(since)
        return {"results": entries, "next_cursor": next_cursor}

    return app


def app_factory() -> FastAPI:
    """Uvicorn entrypoint: `uvicorn app:app_factory --factory`.

    Missing or unsafe configuration stops the process here, before it serves.
    """
    try:
        return create_app(settings_from_env())
    except ConfigError as exc:
        raise SystemExit(f"Relay configuration is not usable ({exc}). The URL is not printed.") from None


if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    uvicorn.run(app_factory(), host="0.0.0.0", port=8080)
