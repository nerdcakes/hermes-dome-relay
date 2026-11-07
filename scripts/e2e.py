"""One local round trip with a fake executive.

Default mode never leaves the machine and never reads a real key. It prints
a short job id and a status word. Live mode is refused unless you pass
``--i-understand-this-wakes-a-real-executive``. Live mode does not take a key
on the command line.

    python scripts/e2e.py
"""

from __future__ import annotations

import socket
import sqlite3
import sys
import tempfile
from datetime import timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "relay"))

from app import Settings as RelaySettings  # noqa: E402
from app import create_app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
import httpx  # noqa: E402

from dome_relay.adapters.relay_client import RelayClient  # noqa: E402
from dome_relay.config import Settings  # noqa: E402
from dome_relay.errors import PluginError  # noqa: E402
from dome_relay.service import dispatch_queued, escalate, pull_into, status  # noqa: E402
from dome_relay.storage import Store  # noqa: E402
from storage import format_utc  # noqa: E402


RELAY_KEY = "test-relay-key-0123456789"
CALLBACK_KEY = "test-callback-key-0123456789"
GROK_KEY = "test-grok-key-0123456789"
ORIGIN = "http://127.0.0.1:8080"
BRIEF = "Summarize the public notes."
RESULT = "A short harmless summary."


class Clock:
    def __init__(self):
        from datetime import datetime, timezone

        self.now = datetime(2026, 11, 7, 21, 0, tzinfo=timezone.utc)

    def __call__(self):
        return self.now


class FixedJitter:
    def uniform(self, _low, _high):
        return 0.25


def plugin_settings() -> Settings:
    return Settings(
        relay_url=ORIGIN,
        callback_host="",
        sender="celeste",
        hop=1,
        inject_context=False,
        inject_message=False,
        allow_loopback=True,
        poll_interval_seconds=15,
        idle_backoff_seconds=60,
        unreachable_backoff_seconds=60,
        local_heartbeat_seconds=60,
        relay_key=RELAY_KEY,
    )


class ScriptedWebhook(httpx.BaseTransport):
    def __init__(self):
        self.calls = 0

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        return httpx.Response(200)


class PluginTransport(httpx.BaseTransport):
    def __init__(self, client: TestClient):
        self.client = client

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.url.query:
            path = f"{path}?{request.url.query.decode()}"
        response = self.client.request(
            request.method,
            path,
            content=request.content,
            headers={key: value for key, value in request.headers.items() if key.lower() != "host"},
        )
        return httpx.Response(response.status_code, content=response.content, headers=dict(response.headers))


def _say(job_id: str, state: str) -> None:
    print(f"job {job_id[:8]}… {state}")


def run_fake() -> int:
    clock = Clock()
    webhook = ScriptedWebhook()
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        relay_settings = RelaySettings(
            relay_key=RELAY_KEY,
            callback_key=CALLBACK_KEY,
            grok_key=GROK_KEY,
            grok_webhook_url="http://127.0.0.1:9/wake",
            public_origin=ORIGIN,
            db_path=str(root / "relay.db"),
            allow_loopback=True,
            clock=clock,
            sleeper=lambda _seconds: None,
            rng=FixedJitter(),
            transport=webhook,
            resolver=socket.getaddrinfo,
        )
        app = create_app(relay_settings)
        api = TestClient(app)
        api.__enter__()
        try:
            store = Store(sqlite3.connect(root / "plugin.db"))
            settings = plugin_settings()
            saved = escalate(
                store,
                settings,
                {
                    "target": "research",
                    "brief": BRIEF,
                    "priority": "normal",
                    "deadline": format_utc(clock.now + timedelta(minutes=20)),
                },
                profile="default",
                conversation_id="conv-1",
                session_id="sess-1",
                now=clock.now,
            )
            _say(saved["job_id"], saved["state"])
            relay = RelayClient(
                settings,
                transport=PluginTransport(api),
                clock=clock,
                sleeper=lambda _seconds: None,
                rng=FixedJitter(),
            )
            assert dispatch_queued(store, relay, settings, clock.now) == "ok"
            assert webhook.calls == 1
            _say(saved["job_id"], store.get(saved["job_id"])["state"])
            claim = api.post(f"/jobs/{saved['job_id']}/claim", headers={"Authorization": f"Bearer {CALLBACK_KEY}"})
            assert claim.status_code == 200
            done = api.post(
                f"/jobs/{saved['job_id']}/complete",
                json={
                    "job_id": saved["job_id"],
                    "status": "done",
                    "result": RESULT,
                    "completed_at": "2026-11-07T21:05:00Z",
                },
                headers={"Authorization": f"Bearer {CALLBACK_KEY}"},
            )
            assert done.status_code == 201
            duplicate = api.post(
                f"/jobs/{saved['job_id']}/complete",
                json={
                    "job_id": saved["job_id"],
                    "status": "done",
                    "result": RESULT,
                    "completed_at": "2026-11-07T21:05:00Z",
                },
                headers={"Authorization": f"Bearer {CALLBACK_KEY}"},
            )
            assert duplicate.status_code == 409
            assert duplicate.json()["error"] == "duplicate_result"
            assert pull_into(store, relay, clock.now, max_attempts=1) == "ok"
            viewed = status(
                store, settings, saved["job_id"], profile="default", conversation_id="conv-1", now=clock.now
            )
            assert viewed["result"]["status"] == "done"
            assert viewed["result"]["result"] == RESULT
            _say(saved["job_id"], viewed["state"])
            # A second registration of the same id does not wake again.
            again = escalate(
                store,
                settings,
                {
                    "target": "research",
                    "brief": BRIEF,
                    "priority": "normal",
                    "deadline": format_utc(clock.now + timedelta(minutes=20)),
                    "job_id": saved["job_id"],
                },
                profile="default",
                conversation_id="conv-1",
                session_id="sess-1",
                now=clock.now,
            )
            assert again["deduped"] is True
            assert webhook.calls == 1
            try:
                from dome_relay.service import cancel

                cancel(
                    store,
                    settings,
                    saved["job_id"],
                    profile="default",
                    conversation_id="conv-1",
                    now=clock.now,
                    client=relay,
                )
            except PluginError as exc:
                assert exc.code == "terminal"
            else:
                raise AssertionError("a finished job must stay finished")
            assert store.get(saved["job_id"])["state"] == "done"
            store.close()
        finally:
            api.__exit__(None, None, None)
    print("fake executive round trip ok")
    return 0


def run_live() -> int:
    print(
        "Live mode wakes a real executive and spends usage. "
        "This script will not read a key from the command line. "
        "Run it inside Hermes after DOME_RELAY_KEY is in the secret store, "
        "or keep using the default fake executive."
    )
    try:
        from agent.secret_scope import get_secret
        from dome_relay.config import load_settings
    except ImportError:
        print("Hermes is not installed here, so live mode cannot read the secret store.")
        return 2
    key = get_secret("DOME_RELAY_KEY")
    if not isinstance(key, str) or not key:
        print("DOME_RELAY_KEY is not set in the Hermes secret store.")
        return 2
    print("A relay URL from plugin settings is required. Refusing to guess one.")
    try:
        load_settings(lambda _key, default=None: default, key)
    except Exception:
        print("Plugin settings could not be read.")
        return 2
    print("Live dispatch is not run by this copy. Use the worker against your own relay.")
    return 2


def main(argv: list[str]) -> int:
    if "--live" in argv:
        if "--i-understand-this-wakes-a-real-executive" not in argv:
            print("Refusing live mode. Re-run with --i-understand-this-wakes-a-real-executive.")
            return 2
        return run_live()
    try:
        return run_fake()
    except Exception:
        import traceback

        traceback.print_exc()
        print("fake executive round trip failed")
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
