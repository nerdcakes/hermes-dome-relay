"""Checks on the relay origin before any request leaves the machine.

The model never supplies this URL. It comes from plugin settings. Production
refuses loopback, private, and link-local addresses. Plain http is allowed
only when ``allow_loopback`` is on and every address is loopback, which is
the local-test exception. Redirects are not followed; the HTTP client sets
that separately.
"""

from __future__ import annotations

import ipaddress
import socket
from typing import Callable
from urllib.parse import urlsplit, urlunsplit

from .errors import PluginError


Resolver = Callable[[str, int], list]


def default_resolver(host: str, port: int):
    return socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)


def _blocked(ip: ipaddress.IPv4Address | ipaddress.IPv6Address, allow_loopback: bool) -> bool:
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        return _blocked(mapped, allow_loopback)
    if allow_loopback and ip.is_loopback:
        return False
    return not ip.is_global


def _addresses(host: str, port: int, resolver: Resolver) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        return [ipaddress.ip_address(host)]
    except ValueError:
        pass
    try:
        infos = resolver(host, port)
    except socket.gaierror as exc:
        raise PluginError("relay_unreachable", "The relay host did not resolve.") from exc
    found = []
    for info in infos:
        found.append(ipaddress.ip_address(info[4][0]))
    if not found:
        raise PluginError("relay_unreachable", "The relay host did not resolve.")
    return found


def normalize_origin(url: str, *, allow_loopback: bool, callback_host: str) -> str:
    """Return ``scheme://host[:port]`` or raise PluginError. Does not resolve DNS."""
    if not isinstance(url, str) or not url.strip() or any(char.isspace() for char in url):
        raise PluginError("bad_relay_url", "relay_url must be an https origin with no path or credentials.")
    parts = urlsplit(url.strip())
    if parts.username or parts.password:
        raise PluginError("bad_relay_url", "relay_url must not contain credentials.")
    if parts.query or parts.fragment:
        raise PluginError("bad_relay_url", "relay_url must not contain a query or fragment.")
    if parts.path not in ("", "/"):
        raise PluginError("bad_relay_url", "relay_url must be an origin, with no path.")
    host = (parts.hostname or "").lower()
    if not host:
        raise PluginError("bad_relay_url", "relay_url must be an https origin with no path or credentials.")
    scheme = parts.scheme.lower()
    if scheme != "https" and not (allow_loopback and scheme == "http"):
        raise PluginError("bad_relay_url", "relay_url must be https.")
    if callback_host and host != callback_host.strip().lower():
        raise PluginError("bad_relay_url", "relay_url host does not match callback_host.")
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None and _blocked(literal, allow_loopback):
        raise PluginError("bad_relay_url", "relay_url must be a public https origin.")
    if host == "localhost" or host.endswith(".localhost") or host.endswith(".local"):
        if not allow_loopback:
            raise PluginError("bad_relay_url", "relay_url must be a public https origin.")
    port = parts.port
    display_host = f"[{host}]" if ":" in host else host
    netloc = f"{display_host}:{port}" if port else display_host
    return urlunsplit((scheme, netloc, "", "", ""))


def assert_origin_safe(origin: str, *, allow_loopback: bool, resolver: Resolver | None = None) -> None:
    """Resolve the host and refuse anything that is not public (loopback tests excepted)."""
    resolver = resolver or default_resolver
    parts = urlsplit(origin)
    host = parts.hostname or ""
    port = parts.port or (443 if parts.scheme == "https" else 80)
    addresses = _addresses(host, port, resolver)
    for ip in addresses:
        if _blocked(ip, allow_loopback):
            raise PluginError("bad_relay_url", "relay_url must be a public https origin.")
    if parts.scheme == "http" and not all(ip.is_loopback for ip in addresses):
        raise PluginError("bad_relay_url", "Plain http is only allowed for loopback in tests.")


def callback_url(origin: str, job_id: str) -> str:
    """The only callback the relay accepts. No key is embedded in it."""
    return f"{origin}/jobs/{job_id}/complete"
