"""Tool handlers. Each one returns a JSON string and does not raise."""

from __future__ import annotations

import json
import logging

from .config import load_settings_from_context
from .errors import PluginError
from .schemas import CANCEL, ESCALATE, STATUS
from .service import cancel as cancel_job
from .service import escalate as escalate_job
from .service import inject_ready_results, status as status_job
from .storage import open_store


logger = logging.getLogger("dome_relay.plugin")


def _dumps(payload: dict) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _profile(ctx) -> str:
    try:
        name = ctx.profile_name
    except Exception:
        name = "default"
    if not isinstance(name, str) or not name.strip():
        return "default"
    return name.strip()[:128]


def _text(kwargs: dict, *names: str) -> str:
    for name in names:
        value = kwargs.get(name)
        if isinstance(value, str):
            return value
    return ""


def _client_for(settings):
    from .adapters.relay_client import RelayClient

    return RelayClient(settings)


def build_handlers(ctx):
    """Close over the plugin context. Settings and the database are read per call."""

    def dome_relay_escalate(args: dict, **kwargs) -> str:
        try:
            settings = load_settings_from_context(ctx)
            store = open_store()
            try:
                payload = escalate_job(
                    store,
                    settings,
                    args if isinstance(args, dict) else {},
                    profile=_profile(ctx),
                    conversation_id=_text(kwargs, "conversation_id"),
                    session_id=_text(kwargs, "session_id"),
                )
            finally:
                store.close()
            return _dumps(payload)
        except PluginError as exc:
            return _dumps({"error": exc.code, "detail": exc.detail})
        except Exception:
            logger.warning("event=tool_error tool=dome_relay_escalate")
            return _dumps({"error": "internal", "detail": "The plugin hit an unexpected error."})

    def dome_relay_status(args: dict, **kwargs) -> str:
        try:
            if not isinstance(args, dict) or set(args) - {"job_id"}:
                raise PluginError("invalid_args", "status takes job_id only.")
            settings = load_settings_from_context(ctx)
            store = open_store()
            try:
                client = _client_for(settings)
                payload = status_job(
                    store,
                    settings,
                    args.get("job_id"),
                    profile=_profile(ctx),
                    conversation_id=_text(kwargs, "conversation_id"),
                    session_id=_text(kwargs, "session_id"),
                    client=client,
                )
            finally:
                store.close()
            return _dumps(payload)
        except PluginError as exc:
            return _dumps({"error": exc.code, "detail": exc.detail})
        except Exception:
            logger.warning("event=tool_error tool=dome_relay_status")
            return _dumps({"error": "internal", "detail": "The plugin hit an unexpected error."})

    def dome_relay_cancel(args: dict, **kwargs) -> str:
        try:
            if not isinstance(args, dict) or set(args) - {"job_id"}:
                raise PluginError("invalid_args", "cancel takes job_id only.")
            settings = load_settings_from_context(ctx)
            store = open_store()
            try:
                client = _client_for(settings)
                payload = cancel_job(
                    store,
                    settings,
                    args.get("job_id"),
                    profile=_profile(ctx),
                    conversation_id=_text(kwargs, "conversation_id"),
                    session_id=_text(kwargs, "session_id"),
                    client=client,
                )
            finally:
                store.close()
            return _dumps(payload)
        except PluginError as exc:
            return _dumps({"error": exc.code, "detail": exc.detail})
        except Exception:
            logger.warning("event=tool_error tool=dome_relay_cancel")
            return _dumps({"error": "internal", "detail": "The plugin hit an unexpected error."})

    def pre_llm_call(session_id=None, user_message=None, **kwargs):
        try:
            settings = load_settings_from_context(ctx)
            if not settings.inject_context and not settings.inject_message:
                return None
            origin_session = session_id if isinstance(session_id, str) else ""
            conversation_id = _text(kwargs, "conversation_id") or origin_session
            session_key = _text(kwargs, "session_key") or origin_session
            store = open_store()
            try:
                return inject_ready_results(
                    store,
                    settings,
                    ctx,
                    profile=_profile(ctx),
                    conversation_id=conversation_id,
                    session_id=origin_session,
                    session_key=session_key or None,
                )
            finally:
                store.close()
        except Exception:
            logger.info("event=pre_llm_call_skipped")
            return None

    return {
        "dome_relay_escalate": dome_relay_escalate,
        "dome_relay_status": dome_relay_status,
        "dome_relay_cancel": dome_relay_cancel,
        "pre_llm_call": pre_llm_call,
    }


def register_tools(ctx) -> None:
    """Register the three tools and the optional context hook. No network, no worker."""
    handlers = build_handlers(ctx)
    ctx.register_tool(
        name="dome_relay_escalate",
        toolset="dome_relay",
        schema=ESCALATE,
        handler=handlers["dome_relay_escalate"],
    )
    ctx.register_tool(
        name="dome_relay_status",
        toolset="dome_relay",
        schema=STATUS,
        handler=handlers["dome_relay_status"],
    )
    ctx.register_tool(
        name="dome_relay_cancel",
        toolset="dome_relay",
        schema=CANCEL,
        handler=handlers["dome_relay_cancel"],
    )
    ctx.register_hook("pre_llm_call", handlers["pre_llm_call"])
    from .worker import handle_cli, setup_cli

    ctx.register_cli_command(
        name="dome-relay",
        help="Poll the Dome Relay for this profile",
        setup_fn=setup_cli,
        handler_fn=lambda args: handle_cli(ctx, args),
        description="Register jobs and pull results. Does not wake Grok.",
    )
