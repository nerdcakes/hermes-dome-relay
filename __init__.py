"""Hermes entrypoint for Dome Relay.

Hermes loads this file because it sits next to plugin.yaml. ``register`` only
records tools, one hook, and the worker command. It does not open a socket
and it does not start the worker.
"""

from __future__ import annotations


def register(ctx) -> None:
    from .dome_relay.tools import register_tools

    register_tools(ctx)
