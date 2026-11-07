"""Plugin tests against a fake relay. Nothing here calls Grok."""

from __future__ import annotations

import hashlib
import json
import logging
import socket
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from dome_relay.adapters.relay_client import RelayClient
from dome_relay.hashutil import format_utc, identity_hash
from dome_relay.service import cancel, dispatch_queued, escalate, pull_into, status, tick
from tests.plugin.conftest import ORIGIN, RELAY_KEY, FixedJitter, ScriptedTransport, job_args, make_settings


ROOT = Path(__file__).resolve().parents[2]
PROFILE = "default"
CONVERSATION = "conv-1"


def client_for(settings, script, clock, sleeps=None):
    transport = ScriptedTransport(script)
    sleeper = (sleeps.append if sleeps is not None else (lambda _seconds: None))
    relay = RelayClient(
        settings,
        transport=transport,
        clock=clock,
        sleeper=sleeper,
        rng=FixedJitter(),
    )
    return relay, transport


def accepted(job_id: str, wake: str = "accepted", code: int = 201):
    return httpx.Response(
        code,
        json={
            "job_id": job_id,
            "state": "registered",
            "wake_status": wake,
            "deduped": code == 200,
            "expires_at": "2026-11-07T21:20:00Z",
            "received_at": "2026-11-07T21:00:00Z",
        },
    )


def save(store, clock, settings=None, **overrides):
    settings = settings or make_settings()
    payload = escalate(
        store,
        settings,
        job_args(**overrides),
        profile=PROFILE,
        conversation_id=CONVERSATION,
        session_id="sess-1",
        now=clock.now,
    )
    return payload


def test_identity_hash_matches_the_relay_bytes():
    envelope = {"b": 1, "attempt": 3, "a": 2}
    assert identity_hash(envelope) == hashlib.sha256(b'{"a":2,"b":1}').hexdigest()
    assert identity_hash(envelope) == identity_hash({"a": 2, "b": 1, "attempt": 1})


def test_escalate_rejects_bad_input_and_persists_a_valid_job(store, clock):
    settings = make_settings()
    with pytest.raises(Exception) as missing:
        escalate(store, settings, {"brief": "hello"}, profile=PROFILE, conversation_id=CONVERSATION, session_id="s", now=clock.now)
    assert missing.value.code == "invalid_target"
    with pytest.raises(Exception) as deadline:
        escalate(
            store,
            settings,
            job_args(deadline=format_utc(clock.now + timedelta(minutes=31))),
            profile=PROFILE,
            conversation_id=CONVERSATION,
            session_id="s",
            now=clock.now,
        )
    assert deadline.value.code == "invalid_deadline"
    with pytest.raises(Exception) as artifact:
        escalate(
            store,
            settings,
            job_args(artifacts=[{"label": "notes", "url": "https://files.example/notes?token=abc"}]),
            profile=PROFILE,
            conversation_id=CONVERSATION,
            session_id="s",
            now=clock.now,
        )
    assert artifact.value.code == "invalid_artifact"
    saved = save(store, clock)
    assert saved["state"] == "queued"
    assert saved["deduped"] is False
    row = store.get(saved["job_id"])
    assert row["callback_url"] == f"{ORIGIN}/jobs/{saved['job_id']}/complete"
    assert row["hop"] == 1
    assert "parent_job_id" not in row["artifacts_json"]


def test_duplicate_id_returns_the_same_job_and_changed_content_conflicts(store, clock):
    settings = make_settings()
    job_id = "00000000-0000-4000-8000-000000000001"
    deadline = format_utc(clock.now + timedelta(minutes=20))
    first = save(store, clock, settings, job_id=job_id, deadline=deadline)
    again = save(store, clock, settings, job_id=job_id, deadline=deadline)
    assert again["deduped"] is True
    assert again["job_id"] == first["job_id"]
    with pytest.raises(Exception) as changed:
        save(store, clock, settings, job_id=job_id, deadline=deadline, brief="A different task")
    assert changed.value.code == "conflict"


def test_worker_registers_with_bearer_callback_and_hash(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    sleeps: list[float] = []
    relay, transport = client_for(settings, [accepted(saved["job_id"])], clock, sleeps)
    assert dispatch_queued(store, relay, settings, clock.now) == "ok"
    sent = transport.calls[0]
    assert sent.headers["authorization"] == f"Bearer {RELAY_KEY}"
    assert sent.url.path == "/jobs"
    body = json.loads(sent.content)
    assert set(body) == {"envelope", "envelope_hash", "hop"}
    assert body["hop"] == 1
    assert "parent_job_id" not in body
    envelope = body["envelope"]
    assert envelope["callback_url"] == f"{ORIGIN}/jobs/{saved['job_id']}/complete"
    assert body["envelope_hash"] == identity_hash(envelope)
    assert identity_hash(envelope) == identity_hash({**envelope, "attempt": 3})
    assert store.get(saved["job_id"])["state"] == "accepted"
    assert store.get(saved["job_id"])["brief"] == ""
    assert sleeps == []


def test_retries_transient_failures_then_stops_on_401(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    sleeps: list[float] = []
    script = [httpx.Response(500), httpx.Response(503), accepted(saved["job_id"])]
    relay, transport = client_for(settings, script, clock, sleeps)
    assert dispatch_queued(store, relay, settings, clock.now) == "ok"
    assert [item for item in sleeps] == [5.25, 15.25]
    assert len(transport.calls) == 3
    assert [row["outcome"] for row in store.list_attempts(saved["job_id"])] == [
        "transient",
        "transient",
        "accepted",
    ]

    other = save(store, clock, settings, target="other")
    sleeps.clear()
    blocked, transport = client_for(
        settings,
        [httpx.Response(401), accepted(other["job_id"])],
        clock,
        sleeps,
    )
    assert dispatch_queued(store, blocked, settings, clock.now) == "auth"
    assert len(transport.calls) == 1
    assert sleeps == []
    assert blocked.auth_blocked is True
    assert store.get(other["job_id"])["state"] == "queued"


def test_retry_after_and_deadline_cutoff(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings, deadline=format_utc(clock.now + timedelta(minutes=20)))
    sleeps: list[float] = []
    relay, transport = client_for(
        settings,
        [httpx.Response(429, headers={"Retry-After": "2"}), accepted(saved["job_id"])],
        clock,
        sleeps,
    )
    dispatch_queued(store, relay, settings, clock.now)
    assert sleeps == [2.25]
    assert len(transport.calls) == 2

    late = save(store, clock, settings, target="late", deadline=format_utc(clock.now + timedelta(minutes=20)))
    stopped_sleeps: list[float] = []
    stopped, transport = client_for(
        settings,
        [httpx.Response(503, headers={"Retry-After": "100000"}), accepted(late["job_id"])],
        clock,
        stopped_sleeps,
    )
    dispatch_queued(store, stopped, settings, clock.now)
    assert stopped_sleeps == []
    assert len(transport.calls) == 1
    assert store.get(late["job_id"])["state"] == "rejected"


def test_read_timeout_is_uncertain_and_is_not_sent_again(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    relay, transport = client_for(settings, [httpx.ReadTimeout("timed out"), accepted(saved["job_id"])], clock)
    dispatch_queued(store, relay, settings, clock.now)
    assert store.get(saved["job_id"])["state"] == "uncertain"
    dispatch_queued(store, relay, settings, clock.now)
    assert len(transport.calls) == 1


def test_connect_failure_is_retried(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    sleeps: list[float] = []
    relay, transport = client_for(
        settings, [httpx.ConnectError("connection refused"), accepted(saved["job_id"])], clock, sleeps
    )
    dispatch_queued(store, relay, settings, clock.now)
    assert len(transport.calls) == 2
    assert sleeps == [5.25]
    assert store.get(saved["job_id"])["state"] == "accepted"


def test_one_outstanding_job_per_target_and_hop_guard(store, clock):
    settings = make_settings()
    first = save(store, clock, settings, target="research")
    second = save(store, clock, settings, target="research")
    other = save(store, clock, settings, target="other")
    relay, transport = client_for(
        settings,
        [accepted(first["job_id"]), accepted(other["job_id"]), accepted(second["job_id"])],
        clock,
    )
    dispatch_queued(store, relay, settings, clock.now)
    sent_ids = [json.loads(call.content)["envelope"]["job_id"] for call in transport.calls]
    research_sent = [job_id for job_id in sent_ids if job_id in {first["job_id"], second["job_id"]}]
    assert research_sent == [first["job_id"]] or research_sent == [second["job_id"]]
    assert other["job_id"] in sent_ids
    waiting = second["job_id"] if research_sent == [first["job_id"]] else first["job_id"]
    assert store.get(waiting)["state"] == "queued"

    hopped = save(store, clock, settings, target="hop")
    store._conn.execute("UPDATE jobs SET hop = 2, state = 'queued' WHERE job_id = ?", (hopped["job_id"],))
    store._conn.commit()
    row = store.get(hopped["job_id"])
    assert row["hop"] == 2
    relay, transport = client_for(settings, [accepted(hopped["job_id"])], clock)
    dispatch_queued(store, relay, settings, clock.now)
    assert transport.calls == []
    assert store.get(hopped["job_id"])["state"] == "rejected"


def test_expiry_is_checked_before_send(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings, deadline=format_utc(clock.now + timedelta(minutes=10)))
    clock.advance(minutes=10)
    relay, transport = client_for(settings, [accepted(saved["job_id"])], clock)
    dispatch_queued(store, relay, settings, clock.now)
    assert transport.calls == []
    assert store.get(saved["job_id"])["state"] == "expired"


def test_local_job_waits_for_a_fresh_heartbeat(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings, execution_location="local")
    assert saved["state"] == "held_local"
    relay, transport = client_for(settings, [accepted(saved["job_id"])], clock)
    dispatch_queued(store, relay, settings, clock.now)
    assert transport.calls == []
    store.touch_heartbeat(clock.now, "worker")
    store.promote_held_local(clock.now, settings.local_heartbeat_seconds)
    dispatch_queued(store, relay, settings, clock.now)
    assert len(transport.calls) == 1
    clock.advance(seconds=61)
    later = save(store, clock, settings, execution_location="local", target="local-2")
    assert later["state"] == "held_local"


def test_restart_marks_dispatching_uncertain_and_keeps_retry_wait(store, clock, tmp_path):
    settings = make_settings()
    saved = save(store, clock, settings)
    assert store.claim_dispatch(saved["job_id"], clock.now) is not None
    store.recover_inflight(clock.now)
    assert store.get(saved["job_id"])["state"] == "uncertain"
    assert store.get(saved["job_id"])["brief"] == ""

    waiting = save(store, clock, settings, target="wait")
    claimed = store.claim_dispatch(waiting["job_id"], clock.now)
    store.note_phase(claimed["job_id"], "retry_wait", 1, "transient", None, clock.now)
    store.recover_inflight(clock.now)
    restored = store.get(waiting["job_id"])
    assert restored["state"] == "queued"
    assert restored["attempts_used"] == 1
    assert restored["brief"] != ""


def test_results_cursor_commits_with_the_batch_and_a_crash_rolls_back(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    relay, _transport = client_for(settings, [accepted(saved["job_id"])], clock)
    dispatch_queued(store, relay, settings, clock.now)
    page = {
        "results": [
            {
                "cursor": "c1",
                "result": {
                    "job_id": saved["job_id"],
                    "status": "done",
                    "result": "RESULT-MARKER-do-not-log",
                    "completed_at": "2026-11-07T21:05:00Z",
                },
            }
        ],
        "next_cursor": "c1",
    }
    puller, _transport = client_for(settings, [httpx.Response(200, json=page)], clock)
    assert pull_into(store, puller, clock.now, max_attempts=1) == "ok"
    assert store.results_cursor() == "c1"
    assert store.latest_result(saved["job_id"])["result"] == "RESULT-MARKER-do-not-log"
    assert store.get(saved["job_id"])["state"] == "done"

    store._failpoint = "before_commit"
    again = dict(page)
    again["results"] = [
        {
            "cursor": "c2",
            "result": {
                "job_id": saved["job_id"],
                "status": "done",
                "result": "second",
                "completed_at": "2026-11-07T21:06:00Z",
            },
        }
    ]
    again["next_cursor"] = "c2"
    failing, _transport = client_for(settings, [httpx.Response(200, json=again)], clock)
    with pytest.raises(RuntimeError):
        pull_into(store, failing, clock.now, max_attempts=1)
    store._failpoint = None
    assert store.results_cursor() == "c1"
    assert store.latest_result(saved["job_id"])["result"] == "RESULT-MARKER-do-not-log"


def test_status_is_limited_to_the_origin_conversation(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    own = status(store, settings, saved["job_id"], profile=PROFILE, conversation_id=CONVERSATION, now=clock.now)
    assert own["state"] == "queued"
    with pytest.raises(Exception) as other:
        status(store, settings, saved["job_id"], profile=PROFILE, conversation_id="conv-2", now=clock.now)
    assert other.value.code == "not_found"
    with pytest.raises(Exception) as other_profile:
        status(store, settings, saved["job_id"], profile="other", conversation_id=CONVERSATION, now=clock.now)
    assert other_profile.value.code == "not_found"


def test_cancel_stops_dispatch_and_posts_when_the_relay_has_the_job(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    relay, _transport = client_for(settings, [accepted(saved["job_id"])], clock)
    dispatch_queued(store, relay, settings, clock.now)
    cancel_client, transport = client_for(settings, [httpx.Response(200, json={"job_id": saved["job_id"], "state": "canceled"})], clock)
    result = cancel(
        store,
        settings,
        saved["job_id"],
        profile=PROFILE,
        conversation_id=CONVERSATION,
        now=clock.now,
        client=cancel_client,
    )
    assert result["state"] == "canceled"
    assert result["cancel_sent"] is True
    assert transport.calls[0].url.path == f"/jobs/{saved['job_id']}/cancel"
    assert transport.calls[0].content == b""
    follow, transport = client_for(settings, [accepted(saved["job_id"])], clock)
    dispatch_queued(store, follow, settings, clock.now)
    assert transport.calls == []


def test_forbidden_is_not_retried(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    sleeps: list[float] = []
    relay, transport = client_for(settings, [httpx.Response(403), accepted(saved["job_id"])], clock, sleeps)
    assert dispatch_queued(store, relay, settings, clock.now) == "auth"
    assert len(transport.calls) == 1
    assert sleeps == []


def test_redirect_is_not_followed(store, clock):
    settings = make_settings()
    saved = save(store, clock, settings)
    relay, transport = client_for(
        settings,
        [httpx.Response(302, headers={"Location": "http://127.0.0.1:9/stolen"}), accepted(saved["job_id"])],
        clock,
    )
    dispatch_queued(store, relay, settings, clock.now)
    assert len(transport.calls) == 1
    assert store.get(saved["job_id"])["state"] == "rejected"


def test_private_relay_url_is_refused_before_a_call():
    from dome_relay.safety import normalize_origin

    with pytest.raises(Exception) as caught:
        normalize_origin("https://10.1.1.1", allow_loopback=False, callback_host="")
    assert caught.value.code == "bad_relay_url"
    with pytest.raises(Exception):
        normalize_origin("http://127.0.0.1:8080", allow_loopback=False, callback_host="")
    assert normalize_origin("http://127.0.0.1:8080", allow_loopback=True, callback_host="") == ORIGIN


def test_secrets_are_not_in_the_audit_log(store, clock, caplog):
    caplog.set_level(logging.INFO, logger="dome_relay.plugin")
    settings = make_settings()
    saved = save(store, clock, settings, brief="BRIEF-MARKER-do-not-log")
    relay, _transport = client_for(settings, [accepted(saved["job_id"])], clock)
    dispatch_queued(store, relay, settings, clock.now)
    page = {
        "results": [
            {
                "cursor": "c1",
                "result": {
                    "job_id": saved["job_id"],
                    "status": "held",
                    "result": "RESULT-MARKER-do-not-log",
                    "completed_at": "2026-11-07T21:04:00Z",
                },
            }
        ],
        "next_cursor": "c1",
    }
    puller, _transport = client_for(settings, [httpx.Response(200, json=page)], clock)
    pull_into(store, puller, clock.now, max_attempts=1)
    blob = json.dumps(store.list_audit()) + caplog.text
    for secret in (RELAY_KEY, "BRIEF-MARKER-do-not-log", "RESULT-MARKER-do-not-log", ORIGIN):
        assert secret not in blob


def test_tick_backs_off_when_idle_and_polls_while_active(store, clock):
    settings = make_settings()
    empty = httpx.Response(200, json={"results": [], "next_cursor": "c0"})
    relay, _transport = client_for(settings, [empty], clock)
    assert tick(store, relay, settings, clock.now, owner="worker-a") == "idle"
    save(store, clock, settings)
    job_id = store.list_queued()[0]["job_id"]
    relay, _transport = client_for(
        settings,
        [accepted(job_id), httpx.Response(200, json={"results": [], "next_cursor": "c0"})],
        clock,
    )
    assert tick(store, relay, settings, clock.now, owner="worker-a") == "active"


def test_plugin_source_does_not_read_env_or_grok_secrets():
    banned = ("os.environ", "DOME_RELAY_GROK", "auth.json", "load_dotenv", "dotenv")
    files = [ROOT / "__init__.py", *Path(ROOT / "dome_relay").rglob("*.py")]
    text = "\n".join(path.read_text(encoding="utf-8") for path in files)
    for token in banned:
        assert token not in text


def test_open_store_uses_plugin_db_outside_the_install_tree(monkeypatch, tmp_path):
    calls = {}

    def fake_plugin_db(name, filename="data.db"):
        calls["name"] = name
        calls["filename"] = filename
        path = tmp_path / "plugin-data" / name / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return __import__("sqlite3").connect(path)

    package = __import__("types").ModuleType("plugins")
    package.__path__ = []
    monkeypatch.setitem(__import__("sys").modules, "plugins", package)
    storage_mod = __import__("types").ModuleType("plugins.plugin_storage")
    storage_mod.plugin_db = fake_plugin_db
    monkeypatch.setitem(__import__("sys").modules, "plugins.plugin_storage", storage_mod)
    from dome_relay.storage import open_store

    opened = open_store()
    try:
        assert calls["name"] == "dome-relay"
        assert "dome_relay" not in str(tmp_path / "plugin-data")
        assert not str(tmp_path / "plugin-data").startswith(str(ROOT / "dome_relay"))
    finally:
        opened.close()


def test_worker_lease_is_exclusive(store, clock):
    assert store.acquire_lease("a", clock.now, 45) is True
    assert store.acquire_lease("b", clock.now, 45) is False
    clock.advance(seconds=46)
    assert store.acquire_lease("b", clock.now, 45) is True
