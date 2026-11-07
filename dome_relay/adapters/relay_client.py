"""HTTP client for the relay.

Registration, cancel, and result reads use ``Authorization: Bearer`` with the
plugin key. The body never contains the key. Redirects are off and proxy
environment variables are ignored, so the key cannot be steered to another
host. 401 and 403 are not retried. A read or write timeout is uncertain and
is not retried. Connect failures and HTTP 408, 429, 500, 502, 503, and 504
are retried up to three times, waiting 5s then 15s plus up to 1s of jitter.
A ``Retry-After`` header replaces that wait and is skipped when the wait
would pass the job deadline.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Callable

import httpx

from ..config import JITTER_MAX_SECONDS, MAX_ATTEMPTS, RETRY_DELAYS_SECONDS, Settings
from ..errors import PluginError
from ..safety import assert_origin_safe, default_resolver


TRANSIENT_HTTP = {408, 429, 500, 502, 503, 504}
NEVER_RETRY_HTTP = {401, 403}


@dataclass
class RegisterOutcome:
    kind: str
    http_status: int | None = None
    wake_status: str | None = None
    error: str | None = None
    deduped: bool = False
    attempts: int = 0


@dataclass
class CancelOutcome:
    kind: str
    http_status: int | None = None
    error: str | None = None


@dataclass
class PullOutcome:
    kind: str
    entries: list = field(default_factory=list)
    next_cursor: str = "c0"
    http_status: int | None = None
    error: str | None = None


class RelayClient:
    """One profile's client. Do not reuse it for another profile's key."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] | None = None,
        rng: random.Random | None = None,
        resolver=None,
    ):
        self.settings = settings
        self._transport = transport
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleeper = sleeper or _default_sleep
        self._rng = rng or random.Random()
        self._resolver = resolver or default_resolver
        self.auth_blocked = False

    def validate_config(self) -> None:
        if not self.settings.key_is_set:
            raise PluginError("missing_key", "DOME_RELAY_KEY is not set in the Hermes secret store.")
        if not self.settings.relay_url:
            raise PluginError("bad_relay_url", "relay_url is not set.")
        from ..safety import normalize_origin

        origin = normalize_origin(
            self.settings.relay_url,
            allow_loopback=self.settings.allow_loopback,
            callback_host=self.settings.callback_host,
        )
        assert_origin_safe(origin, allow_loopback=self.settings.allow_loopback, resolver=self._resolver)

    def dispatch(self, envelope, envelope_hash, hop, deadline, *, attempts_used: int, on_phase) -> RegisterOutcome:
        if hop not in (0, 1):
            return RegisterOutcome(kind="rejected", error="hop_limit", attempts=0)
        body = {"envelope": envelope, "envelope_hash": envelope_hash, "hop": hop}

        def interpret(response: httpx.Response) -> RegisterOutcome:
            return _interpret_register(response)

        return self._run(
            "POST",
            "/jobs",
            json_body=body,
            interpret=interpret,
            deadline=deadline,
            attempts_used=attempts_used,
            on_phase=on_phase,
        )

    def reconcile(self, job_id: str) -> str:
        """An interrupted registration stays uncertain. This does not POST again."""
        return "uncertain"

    def cancel(self, job_id: str, deadline: datetime | None, *, max_attempts: int = MAX_ATTEMPTS) -> CancelOutcome:
        def interpret(response: httpx.Response) -> CancelOutcome:
            return _interpret_cancel(response)

        return self._run(
            "POST",
            f"/jobs/{job_id}/cancel",
            json_body=None,
            interpret=interpret,
            deadline=deadline,
            attempts_used=0,
            on_phase=None,
            max_attempts=max_attempts,
        )

    def pull_results(self, since: str, deadline: datetime | None, *, max_attempts: int = MAX_ATTEMPTS) -> PullOutcome:
        # since is a cursor the relay minted (c + digits). Anything else is refused locally.
        if since != "c0" and (not since.startswith("c") or not since[1:].isdigit()):
            return PullOutcome(kind="rejected", error="bad_cursor", next_cursor=since)

        def interpret(response: httpx.Response) -> PullOutcome:
            return _interpret_pull(response, since)

        return self._run(
            "GET",
            "/results",
            json_body=None,
            params={"since": since, "limit": "100"},
            interpret=interpret,
            deadline=deadline,
            attempts_used=0,
            on_phase=None,
            max_attempts=max_attempts,
        )

    def _run(
        self,
        method: str,
        url: str,
        *,
        json_body,
        interpret,
        deadline: datetime | None,
        attempts_used: int,
        on_phase,
        params: dict | None = None,
        max_attempts: int = MAX_ATTEMPTS,
    ):
        if self.auth_blocked:
            return interpret_auth_blocked(interpret)
        if not self.settings.key_is_set:
            return interpret_missing_key(interpret)
        try:
            from ..safety import normalize_origin

            origin = normalize_origin(
                self.settings.relay_url,
                allow_loopback=self.settings.allow_loopback,
                callback_host=self.settings.callback_host,
            )
            assert_origin_safe(origin, allow_loopback=self.settings.allow_loopback, resolver=self._resolver)
        except PluginError as exc:
            if exc.code == "relay_unreachable":
                return interpret_unreachable(interpret)
            return interpret_blocked(interpret)
        if not url.startswith("/"):
            return interpret_blocked(interpret)
        url = origin + url

        headers = {"Authorization": f"Bearer {self.settings.relay_key}"}
        timeout = httpx.Timeout(10.0, connect=5.0)
        kwargs = {
            "timeout": timeout,
            "follow_redirects": False,
            "trust_env": False,
        }
        if self._transport is not None:
            kwargs["transport"] = self._transport
        limit = MAX_ATTEMPTS if max_attempts > MAX_ATTEMPTS or max_attempts < 1 else max_attempts
        start = attempts_used + 1
        if start > limit:
            return interpret_exhausted(interpret, attempts_used)
        with httpx.Client(**kwargs) as client:
            for attempt in range(start, limit + 1):
                if on_phase is not None:
                    on_phase("dispatching", attempt, None, None)
                try:
                    assert_origin_safe(
                        origin, allow_loopback=self.settings.allow_loopback, resolver=self._resolver
                    )
                except PluginError as exc:
                    if exc.code == "relay_unreachable":
                        return interpret_unreachable(interpret)
                    return interpret_blocked(interpret)
                try:
                    request_kwargs = {"headers": headers}
                    if params:
                        request_kwargs["params"] = params
                    if method == "POST" and json_body is not None:
                        response = client.post(url, json=json_body, **request_kwargs)
                    elif method == "POST":
                        response = client.post(url, content=b"", **request_kwargs)
                    else:
                        response = client.get(url, **request_kwargs)
                except Exception as exc:
                    kind = classify_exception(exc)
                    if on_phase is not None:
                        on_phase(kind, attempt, kind, None)
                    if kind == "uncertain":
                        return interpret_uncertain(interpret, attempt)
                    if attempt == limit or not self._sleep(attempt, None, deadline):
                        return interpret_exhausted(interpret, attempt)
                    if on_phase is not None:
                        on_phase("retry_wait", attempt, None, None)
                    continue
                outcome = interpret(response)
                outcome.attempts = attempt
                kind = _outcome_kind(outcome)
                http_status = outcome.http_status
                if on_phase is not None:
                    on_phase(kind, attempt, kind, http_status)
                if kind == "auth":
                    self.auth_blocked = True
                    return outcome
                if kind != "transient":
                    return outcome
                retry_after = _retry_after(response, self._clock())
                if attempt == limit or not self._sleep(attempt, retry_after, deadline):
                    return interpret_exhausted(interpret, attempt)
                if on_phase is not None:
                    on_phase("retry_wait", attempt, None, None)
        return interpret_exhausted(interpret, limit)

    def _sleep(self, attempt: int, retry_after: float | None, deadline: datetime | None) -> bool:
        if retry_after is not None:
            base = retry_after
        else:
            base = RETRY_DELAYS_SECONDS[attempt - 1]
        delay = base + self._rng.uniform(0.0, JITTER_MAX_SECONDS)
        now = self._clock()
        if deadline is not None and now + timedelta(seconds=delay) >= deadline:
            return False
        self._sleeper(delay)
        return True


def _default_sleep(seconds: float) -> None:
    import time

    time.sleep(seconds)


def classify_exception(exc: Exception) -> str:
    """Connect failures never left the machine. A read or write timeout might have."""
    if isinstance(exc, (httpx.ConnectTimeout, httpx.ConnectError, httpx.PoolTimeout)):
        return "transient"
    if isinstance(exc, (httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError)):
        return "uncertain"
    if isinstance(exc, httpx.TimeoutException):
        return "uncertain"
    if isinstance(exc, httpx.NetworkError):
        return "transient"
    return "uncertain"


def _retry_after(response: httpx.Response, now: datetime) -> float | None:
    value = response.headers.get("retry-after")
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


def _json_object(response: httpx.Response) -> dict:
    try:
        data = response.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _interpret_register(response: httpx.Response) -> RegisterOutcome:
    code = response.status_code
    if 300 <= code < 400:
        return RegisterOutcome(kind="rejected", http_status=code, error="redirect")
    if code in NEVER_RETRY_HTTP:
        return RegisterOutcome(kind="auth", http_status=code, error="unauthorized" if code == 401 else "forbidden")
    if code in TRANSIENT_HTTP:
        return RegisterOutcome(kind="transient", http_status=code)
    body = _json_object(response)
    if code in (200, 201):
        wake = body.get("wake_status") if isinstance(body.get("wake_status"), str) else ""
        if wake == "accepted":
            kind = "accepted"
        elif wake == "uncertain":
            kind = "uncertain"
        elif wake == "rejected":
            kind = "rejected"
        elif wake == "pending":
            kind = "registered"
        else:
            kind = "rejected"
        return RegisterOutcome(
            kind=kind,
            http_status=code,
            wake_status=wake,
            deduped=bool(body.get("deduped")),
        )
    error = body.get("error") if isinstance(body.get("error"), str) else "rejected"
    if code == 409 and error in {"conflict", "hop_limit"}:
        return RegisterOutcome(kind="rejected", http_status=code, error=error)
    if code == 400:
        return RegisterOutcome(kind="rejected", http_status=code, error=error)
    return RegisterOutcome(kind="rejected", http_status=code, error=error)


def _interpret_cancel(response: httpx.Response) -> CancelOutcome:
    code = response.status_code
    if 300 <= code < 400:
        return CancelOutcome(kind="rejected", http_status=code, error="redirect")
    if code in NEVER_RETRY_HTTP:
        return CancelOutcome(kind="auth", http_status=code, error="unauthorized" if code == 401 else "forbidden")
    if code in TRANSIENT_HTTP:
        return CancelOutcome(kind="transient", http_status=code)
    body = _json_object(response)
    if code == 200 and body.get("state") == "canceled":
        return CancelOutcome(kind="canceled", http_status=code)
    error = body.get("error") if isinstance(body.get("error"), str) else "rejected"
    if code == 404:
        return CancelOutcome(kind="not_found", http_status=code, error="not_found")
    return CancelOutcome(kind="rejected", http_status=code, error=error)


def _interpret_pull(response: httpx.Response, since: str) -> PullOutcome:
    code = response.status_code
    if 300 <= code < 400:
        return PullOutcome(kind="rejected", http_status=code, error="redirect", next_cursor=since)
    if code in NEVER_RETRY_HTTP:
        return PullOutcome(
            kind="auth",
            http_status=code,
            error="unauthorized" if code == 401 else "forbidden",
            next_cursor=since,
        )
    if code in TRANSIENT_HTTP:
        return PullOutcome(kind="transient", http_status=code, next_cursor=since)
    body = _json_object(response)
    if code != 200:
        error = body.get("error") if isinstance(body.get("error"), str) else "rejected"
        return PullOutcome(kind="rejected", http_status=code, error=error, next_cursor=since)
    entries = body.get("results")
    next_cursor = body.get("next_cursor") if isinstance(body.get("next_cursor"), str) else since
    if not isinstance(entries, list):
        return PullOutcome(kind="rejected", http_status=code, error="bad_results", next_cursor=since)
    cleaned = []
    for item in entries:
        if not isinstance(item, dict):
            return PullOutcome(kind="rejected", http_status=code, error="bad_results", next_cursor=since)
        cursor = item.get("cursor")
        result = item.get("result")
        if not isinstance(cursor, str) or not isinstance(result, dict):
            return PullOutcome(kind="rejected", http_status=code, error="bad_results", next_cursor=since)
        cleaned.append({"cursor": cursor, "result": result})
    return PullOutcome(kind="ok", entries=cleaned, next_cursor=next_cursor, http_status=code)


def _outcome_kind(outcome) -> str:
    return outcome.kind


def interpret_auth_blocked(interpret):
    # Build a typed outcome by interpreting a synthetic 401 without a body echo.
    response = httpx.Response(401, json={"error": "unauthorized"})
    outcome = interpret(response)
    outcome.attempts = 0
    return outcome


def interpret_missing_key(interpret):
    response = httpx.Response(401, json={"error": "missing_key"})
    outcome = interpret(response)
    outcome.kind = "missing_key"
    outcome.error = "missing_key"
    outcome.http_status = None
    outcome.attempts = 0
    return outcome


def interpret_blocked(interpret):
    response = httpx.Response(400, json={"error": "bad_relay_url"})
    outcome = interpret(response)
    outcome.kind = "blocked"
    outcome.error = "bad_relay_url"
    outcome.http_status = None
    outcome.attempts = 0
    return outcome


def interpret_unreachable(interpret):
    response = httpx.Response(503, json={"error": "relay_unreachable"})
    outcome = interpret(response)
    outcome.kind = "unreachable"
    outcome.error = "relay_unreachable"
    outcome.http_status = None
    outcome.attempts = 0
    return outcome


def interpret_uncertain(interpret, attempt: int):
    response = httpx.Response(504, json={"error": "uncertain"})
    outcome = interpret(response)
    outcome.kind = "uncertain"
    outcome.error = "uncertain"
    outcome.http_status = None
    outcome.attempts = attempt
    return outcome


def interpret_exhausted(interpret, attempt: int):
    response = httpx.Response(503, json={"error": "transient"})
    outcome = interpret(response)
    outcome.kind = "rejected"
    outcome.error = "retries_exhausted"
    outcome.http_status = None
    outcome.attempts = attempt
    return outcome

