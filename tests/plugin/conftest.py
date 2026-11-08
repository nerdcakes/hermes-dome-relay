"""Shared fakes for plugin tests. Keys here are obvious test strings, not real secrets."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from dome_relay.config import Settings
from dome_relay.storage import Store


RELAY_KEY = "test-relay-key-0123456789"
ORIGIN = "http://127.0.0.1:8080"


class Clock:
    def __init__(self):
        self.now = datetime(2026, 11, 7, 21, 0, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class FixedJitter:
    def uniform(self, _low: float, _high: float) -> float:
        return 0.25


class ScriptedTransport(httpx.BaseTransport):
    def __init__(self, script):
        self.script = list(script)
        self.calls: list[httpx.Request] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if not self.script:
            raise AssertionError("the plugin called the relay more times than the test expected")
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def make_settings(**overrides) -> Settings:
    data = dict(
        relay_url=ORIGIN,
        callback_host="",
        sender="celeste",
        hop=1,
        inject_context=False,
        inject_message=False,
        allow_loopback=True,
        poll_interval_seconds=15.0,
        idle_backoff_seconds=60.0,
        unreachable_backoff_seconds=60.0,
        local_heartbeat_seconds=60.0,
        relay_key=RELAY_KEY,
    )
    data.update(overrides)
    return Settings(**data)


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def store(tmp_path):
    conn = sqlite3.connect(tmp_path / "plugin.db")
    opened = Store(conn)
    try:
        yield opened
    finally:
        opened.close()


def job_args(**overrides) -> dict:
    data = {
        "target": "research",
        "brief": "Summarize the public notes. Do not post or spend.",
        "priority": "normal",
        "execution_location": "cloud",
    }
    data.update(overrides)
    return data
