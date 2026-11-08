"""The poll loop.

Start it on purpose with ``hermes dome-relay worker``. Registration does not
start it. While a job is still open it polls every 15 seconds. When nothing
is open, or the relay cannot be reached, it waits longer. One process holds
the lease. Ctrl-C releases the lease and returns.

Local-only jobs stay held until this process has refreshed the heartbeat
inside the configured window (60 seconds unless settings say otherwise).
"""

from __future__ import annotations

import logging
import signal
import uuid
from datetime import datetime, timezone

from .adapters.relay_client import RelayClient
from .config import LEASE_SECONDS, UNREACHABLE_BACKOFF_CAP_SECONDS, load_settings_from_context
from .service import tick
from .storage import open_store


logger = logging.getLogger("dome_relay.plugin")


def setup_cli(subparser) -> None:
    subs = subparser.add_subparsers(dest="dome_relay_command")
    worker = subs.add_parser("worker", help="Poll the relay until stopped")
    worker.add_argument("--once", action="store_true", help="Run one poll and exit")
    subs.add_parser("ready", help="Mark this machine ready for local-only jobs")


def handle_cli(ctx, args) -> None:
    command = getattr(args, "dome_relay_command", None)
    if command == "worker":
        raise SystemExit(serve(ctx, once=bool(getattr(args, "once", False))))
    if command == "ready":
        raise SystemExit(mark_ready(ctx))
    print("Usage: hermes dome-relay <worker|ready>")
    raise SystemExit(2)


def mark_ready(ctx) -> int:
    settings = load_settings_from_context(ctx)
    store = open_store()
    try:
        store.touch_heartbeat(datetime.now(timezone.utc), "ready")
    finally:
        store.close()
    print("Local heartbeat refreshed. Local-only jobs can leave the queue on the next worker poll.")
    return 0


def serve(ctx, *, once: bool = False, client: RelayClient | None = None, sleeper=None, clock=None) -> int:
    """Run until stopped. Returns 0, or 1 when another worker holds the lease."""
    settings = load_settings_from_context(ctx)
    clock = clock or (lambda: datetime.now(timezone.utc))
    sleeper = sleeper or _interruptible_sleep
    owner = uuid.uuid4().hex
    store = open_store()
    client = client or RelayClient(settings, clock=clock)
    stop = _Stop()
    previous = {}
    try:
        if not once:
            previous = _install_stop_signals(stop)
        now = clock()
        if not store.acquire_lease(owner, now, LEASE_SECONDS):
            print("Another dome-relay worker is already running for this profile.")
            return 1
        store.recover_inflight(now)
        unreachable_wait = settings.unreachable_backoff_seconds
        while not stop.stopped:
            now = clock()
            if not store.acquire_lease(owner, now, LEASE_SECONDS):
                print("Lost the worker lease.")
                return 1
            kind = tick(store, client, settings, now, owner=owner)
            if once or stop.stopped:
                break
            if kind == "active":
                delay = settings.poll_interval_seconds
                unreachable_wait = settings.unreachable_backoff_seconds
            elif kind == "unreachable":
                delay = unreachable_wait
                unreachable_wait = min(unreachable_wait * 2, UNREACHABLE_BACKOFF_CAP_SECONDS)
            else:
                delay = settings.idle_backoff_seconds
                unreachable_wait = settings.unreachable_backoff_seconds
            sleeper(delay, stop)
        return 0
    finally:
        try:
            store.release_lease(owner, clock())
        except Exception:
            logger.info("event=lease_release_failed")
        store.close()
        if previous:
            _restore_signals(previous)


class _Stop:
    def __init__(self):
        self.stopped = False

    def stop(self, *_args) -> None:
        self.stopped = True


def _install_stop_signals(stop: _Stop) -> dict:
    previous = {}
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[name] = signal.getsignal(name)
            signal.signal(name, stop.stop)
        except Exception:
            continue
    return previous


def _restore_signals(previous: dict) -> None:
    for name, handler in previous.items():
        try:
            signal.signal(name, handler)
        except Exception:
            continue


def _interruptible_sleep(seconds: float, stop: _Stop) -> None:
    import time

    remaining = seconds
    while remaining > 0 and not stop.stopped:
        step = min(1.0, remaining)
        time.sleep(step)
        remaining -= step
