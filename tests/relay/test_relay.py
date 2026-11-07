"""Tests for the relay.

These use a fake clock (so we do not wait 30 minutes), a fake webhook (so we
do not call Grok), and a temporary database (so nothing is left on disk).

The keys below are fakes for tests. They are not Fly secrets.
"""

from __future__ import annotations

import hashlib
import json
import logging
import socket
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient

from app import (
    ENV_CALLBACK_KEY,
    ENV_GROK_KEY,
    ENV_GROK_URL,
    ENV_LOOPBACK,
    ENV_ORIGIN,
    ENV_RELAY_KEY,
    ConfigError,
    Settings,
    canonical_json,
    content_hash,
    create_app,
    identity_hash,
    keys_match,
    make_client,
    settings_from_env,
)
from storage import Store, format_utc


# Fake keys. Long enough for the startup check, obviously not real.
RELAY_KEY = "test-relay-key-0123456789"
CALLBACK_KEY = "test-callback-key-0123456789"
GROK_KEY = "test-grok-key-0123456789"
ORIGIN = "http://127.0.0.1:8080"
WEBHOOK = "http://127.0.0.1:9/wake"
BRIEF = "BRIEF-MARKER-do-not-log"
RESULT = "RESULT-MARKER-do-not-log"


class Clock:
    """A clock the test can move forward. The relay asks it for 'now'."""

    def __init__(self):
        self.now = datetime(2026, 11, 7, 21, 0, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class FixedJitter:
    """Jitter is supposed to be random. Tests use a fixed extra 0.25s."""

    def uniform(self, _low: float, _high: float) -> float:
        return 0.25


class ScriptedWebhook(httpx.BaseTransport):
    """Stand-in for the Grok webhook.

    A list of replies is consumed one per call. None means "always answer 200",
    which is the stand-in for a healthy routine.
    """

    def __init__(self, script):
        self.script = None if script is None else list(script)
        self.calls: list[httpx.Request] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.script is None:
            return httpx.Response(200)
        if not self.script:
            raise AssertionError("the relay called the webhook more times than the test expected")
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def job_id(number: int) -> str:
    return f"00000000-0000-4000-8000-{number:012d}"


def bearer(key: str) -> dict:
    return {"Authorization": f"Bearer {key}"}


def build_registration(clock: Clock, number: int, **overrides) -> dict:
    """A valid /jobs body. The hash matches what the relay recomputes."""
    hop = overrides.pop("hop", 1)
    parent = overrides.pop("parent", None)
    created = format_utc(clock.now)
    deadline = overrides.pop("deadline", format_utc(clock.now + timedelta(minutes=20)))
    this_id = overrides.pop("job", job_id(number))
    envelope = {
        "job_id": this_id,
        "from": "celeste",
        "target": overrides.pop("target", "grok"),
        "brief": overrides.pop("brief", BRIEF),
        "priority": overrides.pop("priority", "normal"),
        "deadline": deadline,
        "callback_url": overrides.pop(
            "callback_url", f"{ORIGIN}/jobs/{this_id}/complete"
        ),
        "artifacts": overrides.pop("artifacts", []),
        "created_at": created,
        "attempt": overrides.pop("attempt", 1),
    }
    envelope.update(overrides)
    body = {
        "envelope": envelope,
        "envelope_hash": identity_hash(envelope),
        "hop": hop,
    }
    if parent is not None:
        body["parent_job_id"] = parent
    return body


def open_relay(tmp_path, script=None, clock=None, **settings_overrides):
    """Start the app against a temp database and a scripted webhook."""
    clock = clock or Clock()
    sleeps: list[float] = []
    transport = ScriptedWebhook(script)
    settings = Settings(
        relay_key=RELAY_KEY,
        callback_key=CALLBACK_KEY,
        grok_key=GROK_KEY,
        grok_webhook_url=settings_overrides.pop("grok_webhook_url", WEBHOOK),
        public_origin=settings_overrides.pop("public_origin", ORIGIN),
        db_path=str(tmp_path / "relay.db"),
        allow_loopback=settings_overrides.pop("allow_loopback", True),
        clock=clock,
        sleeper=sleeps.append,
        rng=FixedJitter(),  # type: ignore[arg-type]
        transport=transport,
        resolver=settings_overrides.pop("resolver", socket.getaddrinfo),
    )
    app = create_app(settings)
    client = TestClient(app)
    client.__enter__()
    return SimpleNamespace(
        client=client,
        app=app,
        clock=clock,
        transport=transport,
        sleeps=sleeps,
        store=app.state.store,
    )


def close_relay(world) -> None:
    world.client.__exit__(None, None, None)


@pytest.fixture
def world(tmp_path):
    opened = open_relay(tmp_path)
    try:
        yield opened
    finally:
        close_relay(opened)


def post_job(world, number: int, **overrides):
    body = build_registration(world.clock, number, **overrides)
    return world.client.post("/jobs", json=body, headers=bearer(RELAY_KEY))


def claim(world, number: int):
    return world.client.post(
        f"/jobs/{job_id(number)}/claim", headers=bearer(CALLBACK_KEY)
    )


def complete(world, number: int, **overrides):
    body = {
        "job_id": job_id(number),
        "status": overrides.get("status", "done"),
        "result": overrides.get("result", RESULT),
        "completed_at": overrides.get("completed_at", "2026-11-07T21:05:00Z"),
    }
    return world.client.post(
        f"/jobs/{job_id(number)}/complete", json=body, headers=bearer(CALLBACK_KEY)
    )


# --- hash and auth helpers -------------------------------------------------


def test_identity_hash_drops_attempt_and_ignores_key_order():
    """The hash is part of the contract with the plugin. Lock the bytes."""
    envelope = {"b": 1, "attempt": 3, "a": 2}
    assert identity_hash(envelope) == hashlib.sha256(b'{"a":2,"b":1}').hexdigest()
    # Same identity if only the attempt number changes.
    assert identity_hash(envelope) == identity_hash({"a": 2, "b": 1, "attempt": 1})


def test_keys_match_is_exact_and_does_not_throw_on_length_mismatch():
    assert keys_match(RELAY_KEY, RELAY_KEY) is True
    assert keys_match("nope", RELAY_KEY) is False
    assert keys_match("short", RELAY_KEY) is False


def test_wake_client_does_not_follow_redirects_or_use_proxy_env():
    settings = Settings(
        relay_key=RELAY_KEY,
        callback_key=CALLBACK_KEY,
        grok_key=GROK_KEY,
        grok_webhook_url=WEBHOOK,
        public_origin=ORIGIN,
        db_path=":memory:",
        allow_loopback=True,
        clock=Clock(),
        sleeper=lambda _seconds: None,
        rng=FixedJitter(),  # type: ignore[arg-type]
        transport=ScriptedWebhook([]),
    )
    client = make_client(settings)
    try:
        assert client.follow_redirects is False
        assert client.trust_env is False
    finally:
        client.close()


# --- the five behaviors the task asked for ---------------------------------


def test_claim_once_second_claim_is_rejected(world):
    created = post_job(world, 1)
    assert created.status_code == 201
    first = claim(world, 1)
    second = claim(world, 1)
    assert first.status_code == 200
    assert first.json()["status"] == "claimed"
    assert second.status_code == 409
    assert second.json()["error"] == "already_claimed"
    # Cancel is local. A later claim stays refused, and the slot is free for someone else.
    canceled = world.client.post(f"/jobs/{job_id(1)}/cancel", headers=bearer(RELAY_KEY))
    assert canceled.status_code == 200
    assert claim(world, 1).json()["error"] == "canceled"


def test_claim_race_only_one_caller_wins(world):
    assert post_job(world, 1).status_code == 201
    barrier = threading.Barrier(2)
    outcomes = []

    def race():
        barrier.wait()
        outcomes.append(world.store.claim(job_id(1), world.clock.now))

    threads = [threading.Thread(target=race), threading.Thread(target=race)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    errors = [item.get("error") for item in outcomes]
    assert errors.count("already_claimed") == 1
    assert sum(1 for item in outcomes if "job" in item) == 1


def test_expiry_is_enforced_on_register_claim_and_complete(world):
    # A deadline further than 30 minutes out is refused, and Grok is not called.
    too_far = format_utc(world.clock.now + timedelta(minutes=31))
    refused = post_job(world, 1, deadline=too_far)
    assert refused.status_code == 400
    assert refused.json()["error"] == "invalid_deadline"
    assert world.transport.calls == []

    # Exactly 30 minutes is allowed. It dies on the relay clock at that instant.
    on_the_dot = format_utc(world.clock.now + timedelta(minutes=30))
    assert post_job(world, 2, deadline=on_the_dot).status_code == 201
    assert claim(world, 2).status_code == 200
    world.clock.advance(minutes=30)
    assert claim(world, 2).json()["error"] == "expired"
    late = complete(world, 2)
    assert late.status_code == 409
    assert late.json()["error"] == "expired"
    # The late body was quarantined: it is not in the result list.
    listed = world.client.get("/results", headers=bearer(RELAY_KEY))
    assert listed.json()["results"] == []

    # A shorter client deadline also wins. Server receive time is the clock.
    world.clock.advance(minutes=-30)  # back to 21:00; job 2 stays expired
    soon = format_utc(world.clock.now + timedelta(minutes=10))
    assert post_job(world, 3, deadline=soon).status_code == 201
    world.clock.advance(minutes=10)
    assert claim(world, 3).json()["error"] == "expired"
    assert complete(world, 3).json()["error"] == "expired"


def test_duplicate_registration_does_not_wake_twice_and_different_body_conflicts(world):
    first = post_job(world, 1)
    assert first.status_code == 201
    assert first.json()["deduped"] is False
    assert first.json()["wake_status"] == "accepted"
    again = post_job(world, 1)
    assert again.status_code == 200
    assert again.json()["deduped"] is True
    assert len(world.transport.calls) == 1

    # attempt is not part of the identity. A retry of the same task is the same job.
    retried = post_job(world, 1, attempt=2)
    assert retried.status_code == 200
    assert retried.json()["deduped"] is True
    assert len(world.transport.calls) == 1

    changed = post_job(world, 1, brief="A different task")
    assert changed.status_code == 409
    assert changed.json()["error"] == "conflict"
    assert len(world.transport.calls) == 1


def test_auth_rejection(world):
    assert world.client.post("/jobs", json={}).status_code == 401
    bad = world.client.post("/jobs", json={}, headers=bearer("not-the-relay-key"))
    assert bad.status_code == 401
    assert RELAY_KEY not in bad.text
    assert CALLBACK_KEY not in bad.text

    # The plugin key cannot claim or complete. The executive key cannot register or read.
    assert post_job(world, 1).status_code == 201
    claim_as_plugin = world.client.post(
        f"/jobs/{job_id(1)}/claim", headers=bearer(RELAY_KEY)
    )
    assert claim_as_plugin.status_code == 403
    register_as_executive = world.client.post(
        "/jobs",
        json=build_registration(world.clock, 2),
        headers=bearer(CALLBACK_KEY),
    )
    assert register_as_executive.status_code == 403
    assert world.client.get("/results", headers=bearer(CALLBACK_KEY)).status_code == 403
    assert complete(world, 1).status_code == 409  # callback key is allowed, but it is not claimed yet
    claimed = claim(world, 1)
    assert claimed.status_code == 200
    complete_as_plugin = world.client.post(
        f"/jobs/{job_id(1)}/complete",
        json={
            "job_id": job_id(1),
            "status": "done",
            "result": RESULT,
            "completed_at": "2026-11-07T21:05:00Z",
        },
        headers=bearer(RELAY_KEY),
    )
    assert complete_as_plugin.status_code == 403
    reasons = [json.loads(row["detail"])["reason"] for row in world.store.list_audit() if row["event"] == "auth_failure"]
    assert "rejected" in reasons
    assert "wrong_role" in reasons
    # The audit trail records the fact of the failure, not the key that was offered.
    blob = json.dumps(world.store.list_audit())
    assert RELAY_KEY not in blob
    assert CALLBACK_KEY not in blob
    assert GROK_KEY not in blob


def test_hop_limit_blocks_forwarding_and_complete_does_not_wake(world):
    forwarded = post_job(world, 1, hop=2)
    assert forwarded.status_code == 409
    assert forwarded.json()["error"] == "hop_limit"
    child = post_job(world, 2, parent=job_id(1))
    assert child.status_code == 409
    assert child.json()["error"] == "hop_limit"
    assert world.transport.calls == []

    # The one allowed hop does wake Grok. Completing it must not wake Grok again.
    assert post_job(world, 3, hop=1).status_code == 201
    assert len(world.transport.calls) == 1
    assert claim(world, 3).status_code == 200
    done = complete(world, 3, result="please dispatch this onward")
    assert done.status_code == 201
    assert len(world.transport.calls) == 1
    # A follow-up job that names a parent is still refused.
    assert post_job(world, 4, parent=job_id(3)).status_code == 409
    assert len(world.transport.calls) == 1


# --- wake retries ----------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403, 400, 302])
def test_wake_does_not_retry_auth_failure_or_redirect(tmp_path, status):
    script = [httpx.Response(status, headers={"Location": "http://127.0.0.1:9/stolen"}), httpx.Response(200)]
    world = open_relay(tmp_path, script=script)
    try:
        response = post_job(world, 1)
        assert response.status_code == 201
        assert response.json()["wake_status"] == "rejected"
        assert len(world.transport.calls) == 1
        assert world.sleeps == []
    finally:
        close_relay(world)


def test_wake_retries_transient_failures_with_5s_then_15s_plus_jitter(tmp_path):
    script = [httpx.Response(500), httpx.Response(503), httpx.Response(200)]
    world = open_relay(tmp_path, script=script)
    try:
        response = post_job(world, 1)
        assert response.json()["wake_status"] == "accepted"
        assert len(world.transport.calls) == 3
        assert world.sleeps == [5.25, 15.25]
        assert [row["outcome"] for row in world.store.list_attempts(job_id(1))] == [
            "transient",
            "transient",
            "accepted",
        ]
    finally:
        close_relay(world)


def test_wake_stops_on_401_even_after_a_transient_failure(tmp_path):
    script = [httpx.Response(500), httpx.Response(401), httpx.Response(200)]
    world = open_relay(tmp_path, script=script)
    try:
        response = post_job(world, 1)
        assert response.json()["wake_status"] == "rejected"
        assert len(world.transport.calls) == 2
        assert world.sleeps == [5.25]
    finally:
        close_relay(world)


def test_wake_honors_retry_after_and_stops_if_it_would_pass_the_deadline(tmp_path):
    honored = open_relay(
        tmp_path,
        script=[httpx.Response(429, headers={"Retry-After": "2"}), httpx.Response(200)],
    )
    try:
        assert post_job(honored, 1).json()["wake_status"] == "accepted"
        assert honored.sleeps == [2.25]
    finally:
        close_relay(honored)

    # 100000 seconds is far past the 20 minute deadline, so we do not sleep or retry.
    stopped = open_relay(
        tmp_path / "later",
        script=[httpx.Response(503, headers={"Retry-After": "100000"}), httpx.Response(200)],
    )
    try:
        assert post_job(stopped, 1).json()["wake_status"] == "rejected"
        assert stopped.sleeps == []
        assert len(stopped.transport.calls) == 1
    finally:
        close_relay(stopped)


def test_read_timeout_is_uncertain_and_is_not_sent_again(tmp_path):
    world = open_relay(tmp_path, script=[httpx.ReadTimeout("timed out"), httpx.Response(200)])
    try:
        first = post_job(world, 1)
        assert first.json()["wake_status"] == "uncertain"
        assert first.json()["state"] == "uncertain"
        again = post_job(world, 1)
        assert again.status_code == 200
        assert again.json()["deduped"] is True
        assert len(world.transport.calls) == 1
    finally:
        close_relay(world)


def test_connect_failure_is_retried_because_the_request_never_left(tmp_path):
    world = open_relay(
        tmp_path, script=[httpx.ConnectError("connection refused"), httpx.Response(200)]
    )
    try:
        assert post_job(world, 1).json()["wake_status"] == "accepted"
        assert len(world.transport.calls) == 2
        assert world.sleeps == [5.25]
    finally:
        close_relay(world)


def test_wake_posts_the_envelope_with_a_bearer_key(world):
    assert post_job(world, 1).status_code == 201
    sent = world.transport.calls[0]
    assert sent.headers["authorization"] == f"Bearer {GROK_KEY}"
    body = json.loads(sent.content)
    assert body["job_id"] == job_id(1)
    assert body["brief"] == BRIEF
    # The key is a header, not part of the JSON the routine is asked to read.
    assert GROK_KEY not in sent.content.decode()


def test_pending_job_is_sent_once_if_the_first_process_died_before_the_call(world):
    """A row left 'pending' has not been sent. The next identical register sends it once."""
    body = build_registration(world.clock, 7)
    envelope = body["envelope"]
    world.store.insert_job(
        job_id=envelope["job_id"],
        target=envelope["target"],
        deadline=envelope["deadline"],
        envelope_hash=body["envelope_hash"],
        hop=body["hop"],
        envelope_json=canonical_json(envelope),
        received_at=world.clock.now,
        expires_at=world.clock.now + timedelta(minutes=20),
    )
    response = world.client.post("/jobs", json=body, headers=bearer(RELAY_KEY))
    assert response.status_code == 200
    assert response.json()["deduped"] is True
    assert response.json()["wake_status"] == "accepted"
    assert len(world.transport.calls) == 1


def test_interrupted_wake_is_uncertain_after_restart(tmp_path):
    path = tmp_path / "relay.db"
    clock = Clock()
    store = Store(str(path))
    store.insert_job(
        job_id=job_id(1),
        target="grok",
        deadline=format_utc(clock.now + timedelta(minutes=20)),
        envelope_hash="a" * 64,
        hop=1,
        envelope_json='{"brief":"BRIEF-MARKER-do-not-log"}',
        received_at=clock.now,
        expires_at=clock.now + timedelta(minutes=20),
    )
    assert store.begin_wake(job_id(1), clock.now) is not None
    store.close()
    reopened = Store(str(path))
    try:
        job = reopened.get(job_id(1))
        assert job["state"] == "uncertain"
        assert job["wake_status"] == "uncertain"
        assert job["envelope_json"] == ""
    finally:
        reopened.close()


# --- slot, results, secrets ------------------------------------------------


def test_one_accepted_job_per_target_and_held_keeps_the_slot(world):
    assert post_job(world, 1, target="grok").status_code == 201
    assert post_job(world, 2, target="grok").status_code == 201
    assert post_job(world, 3, target="other").status_code == 201
    assert claim(world, 1).status_code == 200
    # A different target can still be claimed.
    assert claim(world, 3).status_code == 200
    busy = claim(world, 2)
    assert busy.status_code == 409
    assert busy.json()["error"] == "slot_busy"
    held = complete(world, 1, status="held", result="blocked on a question")
    assert held.status_code == 201
    assert claim(world, 2).json()["error"] == "slot_busy"
    finished = complete(world, 1, status="done", result="answered")
    assert finished.status_code == 201
    assert claim(world, 2).status_code == 200


def test_identical_result_is_duplicate_and_a_different_terminal_result_conflicts(world):
    assert post_job(world, 1).status_code == 201
    assert claim(world, 1).status_code == 200
    assert complete(world, 1).status_code == 201
    duplicate = complete(world, 1)
    assert duplicate.status_code == 409
    assert duplicate.json()["error"] == "duplicate_result"
    conflict = complete(world, 1, result="a different ending")
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "conflict"
    listed = world.client.get("/results", headers=bearer(RELAY_KEY))
    assert len(listed.json()["results"]) == 1
    # Fetching again does not delete the row or move a server-side cursor.
    again = world.client.get("/results", headers=bearer(RELAY_KEY))
    assert again.json() == listed.json()


def test_held_updates_can_change_then_a_terminal_result_wins(world):
    assert post_job(world, 1).status_code == 201
    assert claim(world, 1).status_code == 200
    first = complete(world, 1, status="held", result="still blocked", completed_at="2026-11-07T21:04:00Z")
    same = complete(world, 1, status="held", result="still blocked", completed_at="2026-11-07T21:04:00Z")
    second = complete(world, 1, status="held", result="blocked on something else", completed_at="2026-11-07T21:06:00Z")
    done = complete(world, 1, status="done", result="unblocked")
    assert first.status_code == 201
    assert same.json()["error"] == "duplicate_result"
    assert second.status_code == 201
    assert done.status_code == 201
    cursors = [item["cursor"] for item in world.client.get("/results", headers=bearer(RELAY_KEY)).json()["results"]]
    assert cursors == ["c1", "c2", "c3"]
    # since= is exclusive and does not destroy the earlier rows.
    page = world.client.get("/results?since=c1&limit=1", headers=bearer(RELAY_KEY))
    body = page.json()
    assert [item["cursor"] for item in body["results"]] == ["c2"]
    assert body["next_cursor"] == "c2"
    full = world.client.get("/results", headers=bearer(RELAY_KEY))
    assert len(full.json()["results"]) == 3


def test_unclaimed_job_cannot_be_completed(world):
    assert post_job(world, 1).status_code == 201
    refused = complete(world, 1)
    assert refused.status_code == 409
    assert refused.json()["error"] == "not_claimed"


def test_secrets_and_task_text_are_not_logged_or_audited(world, caplog):
    caplog.set_level(logging.INFO, logger="dome_relay.service")
    assert post_job(world, 1).status_code == 201
    assert claim(world, 1).status_code == 200
    assert complete(world, 1).status_code == 201
    text = caplog.text
    for secret in (RELAY_KEY, CALLBACK_KEY, GROK_KEY, WEBHOOK, BRIEF, RESULT):
        assert secret not in text
    blob = json.dumps(world.store.list_audit())
    for secret in (RELAY_KEY, CALLBACK_KEY, GROK_KEY, WEBHOOK, BRIEF, RESULT):
        assert secret not in blob
    # The task text is wiped once the wake finishes. The result is kept, on purpose.
    stored = world.store.get(job_id(1))
    assert stored["envelope_json"] == ""
    assert BRIEF not in json.dumps(stored)


def test_body_that_contains_a_secret_is_refused_and_not_stored(world):
    response = post_job(world, 1, brief=f"please use {GROK_KEY}")
    assert response.status_code == 400
    assert response.json()["error"] == "secret_in_body"
    assert GROK_KEY not in response.text
    assert world.transport.calls == []
    assert world.store.get(job_id(1)) is None


def test_query_string_is_refused_without_echoing_it(world, caplog):
    caplog.set_level(logging.WARNING, logger="dome_relay.service")
    response = world.client.post(
        "/jobs?token=super-secret-token",
        json=build_registration(world.clock, 1),
        headers=bearer(RELAY_KEY),
    )
    assert response.status_code == 400
    assert "super-secret-token" not in response.text
    assert "super-secret-token" not in caplog.text


def test_callback_must_match_this_relay_and_artifacts_cannot_carry_tokens(world):
    bad_callback = post_job(world, 1, callback_url="https://evil.example/jobs/x/complete")
    assert bad_callback.status_code == 400
    assert bad_callback.json()["error"] == "callback_mismatch"
    signed = post_job(
        world,
        2,
        artifacts=[{"label": "notes", "url": "https://files.example/notes?token=abc"}],
    )
    assert signed.status_code == 400
    assert world.transport.calls == []


def test_healthz_needs_no_key_and_expires_overdue_jobs(world):
    assert world.client.get("/healthz").json() == {"ok": True}
    assert world.client.get("/healthz", headers=bearer("wrong")).status_code == 200
    assert post_job(world, 1).status_code == 201
    assert claim(world, 1).status_code == 200
    world.clock.advance(minutes=31)
    assert world.client.get("/healthz").status_code == 200
    assert world.store.get(job_id(1))["state"] == "expired"
    # Expiry releases the single slot for that target.
    assert post_job(world, 2).status_code == 201
    assert claim(world, 2).status_code == 200


def test_private_webhook_is_refused_at_startup(tmp_path):
    with pytest.raises(ConfigError):
        open_relay(
            tmp_path,
            grok_webhook_url="https://10.1.1.1/hook",
            public_origin="https://8.8.8.8",
            allow_loopback=False,
        )


def test_dns_that_flips_to_a_private_address_does_not_send(tmp_path):
    """Startup sees a public address. The send-time lookup sees a private one. We do not call it."""
    lookups = {"n": 0}

    def resolver(host, port):
        lookups["n"] += 1
        ip = "8.8.8.8" if lookups["n"] == 1 else "10.1.2.3"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, port))]

    world = open_relay(
        tmp_path,
        grok_webhook_url="https://wake.invalid/hook",
        public_origin="https://8.8.8.8",
        allow_loopback=False,
        resolver=resolver,
    )
    try:
        # The callback URL has to match the public origin we configured above.
        created = world.clock
        body = build_registration(created, 1)
        body["envelope"]["callback_url"] = f"https://8.8.8.8/jobs/{job_id(1)}/complete"
        body["envelope_hash"] = identity_hash(body["envelope"])
        response = world.client.post("/jobs", json=body, headers=bearer(RELAY_KEY))
        assert response.status_code == 201
        assert response.json()["wake_status"] == "rejected"
        assert world.transport.calls == []
    finally:
        close_relay(world)


def test_settings_from_env_names_missing_variables_without_values(monkeypatch):
    for name in (ENV_RELAY_KEY, ENV_CALLBACK_KEY, ENV_GROK_KEY, ENV_GROK_URL, ENV_ORIGIN, ENV_LOOPBACK, "FLY_APP_NAME"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit) as caught:
        settings_from_env()
    message = str(caught.value)
    assert ENV_RELAY_KEY in message
    assert RELAY_KEY not in message


def test_settings_from_env_refuses_loopback_on_fly(monkeypatch):
    monkeypatch.setenv("FLY_APP_NAME", "dome-relay")
    monkeypatch.setenv(ENV_LOOPBACK, "1")
    monkeypatch.setenv(ENV_RELAY_KEY, RELAY_KEY)
    monkeypatch.setenv(ENV_CALLBACK_KEY, CALLBACK_KEY)
    monkeypatch.setenv(ENV_GROK_KEY, GROK_KEY)
    monkeypatch.setenv(ENV_GROK_URL, "https://8.8.8.8/hook")
    monkeypatch.setenv(ENV_ORIGIN, "https://8.8.8.8")
    with pytest.raises(SystemExit) as caught:
        settings_from_env()
    assert "not printed" in str(caught.value) or ENV_LOOPBACK in str(caught.value)
    assert GROK_KEY not in str(caught.value)


def test_result_hash_treats_key_order_as_the_same_result():
    left = {"job_id": job_id(1), "status": "done", "result": "ok", "completed_at": "2026-11-07T21:05:00Z"}
    right = {"completed_at": left["completed_at"], "result": "ok", "status": "done", "job_id": left["job_id"]}
    assert content_hash(left) == content_hash(right)
