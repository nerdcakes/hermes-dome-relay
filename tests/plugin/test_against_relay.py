"""One registration against the real relay, with a fake Grok webhook.

Proves the plugin's hash and callback URL are the ones the relay accepts.
"""

from __future__ import annotations

import json
import socket
from datetime import timedelta

import httpx
from fastapi.testclient import TestClient

from app import Settings as RelaySettings
from app import create_app
from dome_relay.service import dispatch_queued, escalate, status
from storage import format_utc
from tests.plugin.conftest import RELAY_KEY, Clock, FixedJitter, job_args, make_settings


CALLBACK_KEY = "test-callback-key-0123456789"
GROK_KEY = "test-grok-key-0123456789"
ORIGIN = "http://127.0.0.1:8080"


class ScriptedWebhook(httpx.BaseTransport):
    def __init__(self):
        self.calls = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        return httpx.Response(200)


class PluginTransport(httpx.BaseTransport):
    """Send the plugin's httpx calls into the relay's in-process TestClient."""

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


def test_plugin_registration_is_accepted_by_the_relay(tmp_path):
    clock = Clock()
    webhook = ScriptedWebhook()
    relay_settings = RelaySettings(
        relay_key=RELAY_KEY,
        callback_key=CALLBACK_KEY,
        grok_key=GROK_KEY,
        grok_webhook_url="http://127.0.0.1:9/wake",
        public_origin=ORIGIN,
        db_path=str(tmp_path / "relay.db"),
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
        import sqlite3

        from dome_relay.adapters.relay_client import RelayClient
        from dome_relay.storage import Store

        store = Store(sqlite3.connect(tmp_path / "plugin.db"))
        try:
            settings = make_settings()
            saved = escalate(
                store,
                settings,
                job_args(deadline=format_utc(clock.now + timedelta(minutes=20))),
                profile="default",
                conversation_id="conv-1",
                session_id="sess-1",
                now=clock.now,
            )
            relay = RelayClient(
                settings,
                transport=PluginTransport(api),
                clock=clock,
                sleeper=lambda _seconds: None,
                rng=FixedJitter(),
            )
            assert dispatch_queued(store, relay, settings, clock.now) == "ok"
            assert store.get(saved["job_id"])["state"] == "accepted"
            assert len(webhook.calls) == 1
            sent = json.loads(webhook.calls[0].content)
            assert sent["job_id"] == saved["job_id"]
            assert GROK_KEY not in webhook.calls[0].content.decode()
            viewed = status(
                store, settings, saved["job_id"], profile="default", conversation_id="conv-1", now=clock.now
            )
            assert viewed["wake_status"] == "accepted"
            assert viewed["result"] is None
        finally:
            store.close()
    finally:
        api.__exit__(None, None, None)
