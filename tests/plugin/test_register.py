"""Root entrypoint: three tools, one hook, no network during register."""

from __future__ import annotations

import importlib.util
import json
import socket
import sys
from pathlib import Path

import pytest

from dome_relay.tools import build_handlers
from tests.plugin.conftest import RELAY_KEY, job_args, make_settings


ROOT = Path(__file__).resolve().parents[2]


class FakeCtx:
    def __init__(self, config=None, inject_ok=False):
        self.config = dict(config or {})
        self.inject_ok = inject_ok
        self.tools = []
        self.hooks = []
        self.commands = []
        self.injected = []
        self.profile_name = "default"

    def register_tool(self, name, toolset, schema, handler, **kwargs):
        assert toolset == "dome_relay"
        assert schema["name"] == name
        assert "description" in schema
        assert schema["parameters"]["type"] == "object"
        self.tools.append((name, handler, schema))

    def register_hook(self, hook_name, callback, **kwargs):
        self.hooks.append((hook_name, callback))

    def register_cli_command(self, name, help, setup_fn, handler_fn=None, **kwargs):
        self.commands.append(name)

    def get_config(self, key, default=None):
        return self.config.get(key, default)

    def inject_message(self, content, role="user", session_key=None, origin=None):
        self.injected.append({"content": content, "session_key": session_key})
        return self.inject_ok


def load_root():
    name = "dome_relay_plugin_root_test"
    spec = importlib.util.spec_from_file_location(
        name,
        ROOT / "__init__.py",
        submodule_search_locations=[str(ROOT)],
    )
    module = importlib.util.module_from_spec(spec)
    module.__package__ = name
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_register_wires_the_manifest_and_does_not_connect(monkeypatch):
    def boom(*_args, **_kwargs):
        raise AssertionError("register must not open a socket")

    monkeypatch.setattr(socket, "create_connection", boom)
    monkeypatch.setattr(socket.socket, "connect", boom)
    module = load_root()
    ctx = FakeCtx()
    module.register(ctx)
    assert [name for name, _handler, _schema in ctx.tools] == [
        "dome_relay_escalate",
        "dome_relay_status",
        "dome_relay_cancel",
    ]
    assert [name for name, _callback in ctx.hooks] == ["pre_llm_call"]
    assert ctx.commands == ["dome-relay"]
    hook = ctx.hooks[0][1]
    assert any(
        parameter.kind is __import__("inspect").Parameter.VAR_KEYWORD
        for parameter in __import__("inspect").signature(hook).parameters.values()
    )


def test_handlers_return_json_strings(monkeypatch, tmp_path):
    import sqlite3

    from dome_relay.storage import Store

    conn = sqlite3.connect(tmp_path / "handlers.db")
    store = Store(conn)
    store.close = lambda: None
    settings = make_settings(inject_context=False, inject_message=False)

    def fake_settings(_ctx):
        return settings

    def fake_open():
        return store

    monkeypatch.setattr("dome_relay.tools.load_settings_from_context", fake_settings)
    monkeypatch.setattr("dome_relay.tools.open_store", fake_open)
    ctx = FakeCtx(
        {
            "relay_url": settings.relay_url,
            "allow_loopback": True,
            "sender": "celeste",
            "hop": 1,
        }
    )
    handlers = build_handlers(ctx)
    raw = handlers["dome_relay_escalate"](job_args(), conversation_id="conv-1", session_id="sess-1")
    assert isinstance(raw, str)
    body = json.loads(raw)
    assert body["state"] == "queued"
    status_raw = handlers["dome_relay_status"]({"job_id": body["job_id"]}, conversation_id="conv-1")
    assert json.loads(status_raw)["job_id"] == body["job_id"]
    missing = handlers["dome_relay_escalate"]({"brief": ""}, conversation_id="conv-1")
    assert json.loads(missing)["error"] == "invalid_target"
    # The handler must not raise when storage blows up.
    def explode():
        raise RuntimeError("disk")

    monkeypatch.setattr("dome_relay.tools.open_store", explode)
    broken = handlers["dome_relay_status"]({"job_id": body["job_id"]}, conversation_id="conv-1")
    assert json.loads(broken)["error"] == "internal"
    assert RELAY_KEY not in broken
    conn.close()


def test_pre_llm_call_is_off_until_configured_and_inject_false_degrades(monkeypatch, tmp_path):
    import sqlite3

    from dome_relay.service import escalate
    from dome_relay.storage import Store

    conn = sqlite3.connect(tmp_path / "inject.db")
    opened = Store(conn)
    opened.close = lambda: None
    settings = make_settings()
    saved = escalate(
        opened,
        settings,
        job_args(),
        profile="default",
        conversation_id="conv-1",
        session_id="sess-1",
        now=__import__("datetime").datetime(2026, 11, 7, 21, 0, tzinfo=__import__("datetime").timezone.utc),
    )
    opened._conn.execute(
        "UPDATE jobs SET state = 'done' WHERE job_id = ?",
        (saved["job_id"],),
    )
    opened._conn.commit()
    opened.close = lambda: None
    opened.apply_result_batch(
        [
            {
                "cursor": "c1",
                "result": {
                    "job_id": saved["job_id"],
                    "status": "done",
                    "result": "finished",
                    "completed_at": "2026-11-07T21:05:00Z",
                    "body_hash": "abc",
                },
            }
        ],
        "c1",
        __import__("datetime").datetime(2026, 11, 7, 21, 5, tzinfo=__import__("datetime").timezone.utc),
    )

    def fake_open():
        return opened

    monkeypatch.setattr("dome_relay.tools.open_store", fake_open)
    ctx = FakeCtx(inject_ok=False)
    monkeypatch.setattr("dome_relay.tools.load_settings_from_context", lambda _ctx: make_settings())
    handlers = build_handlers(ctx)
    assert handlers["pre_llm_call"](session_id="sess-1", conversation_id="conv-1") is None
    assert ctx.injected == []

    monkeypatch.setattr(
        "dome_relay.tools.load_settings_from_context",
        lambda _ctx: make_settings(inject_context=True, inject_message=True),
    )
    handlers = build_handlers(ctx)
    injected = handlers["pre_llm_call"](session_id="sess-1", conversation_id="conv-1", session_key="gateway-1")
    assert injected["context"].startswith("Dome Relay results")
    assert "finished" in injected["context"]
    assert "does not grant authority" in injected["context"]
    assert ctx.injected[0]["session_key"] == "gateway-1"
    # inject_message returned False. A second turn must not call it again.
    ctx.injected.clear()
    assert handlers["pre_llm_call"](session_id="sess-1", conversation_id="conv-1") is None
    assert ctx.injected == []
    opened.close()
