"""Errors the plugin can show a caller.

``detail`` is a fixed sentence we wrote. It must not contain a brief, a result,
a key, or a URL.
"""

from __future__ import annotations


class PluginError(Exception):
    def __init__(self, code: str, detail: str):
        super().__init__(code)
        self.code = code
        self.detail = detail
