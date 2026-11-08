"""The dispatch surface the worker calls.

``dispatch`` registers a job with the relay. It does not wake Grok. The relay
does that. ``reconcile`` is optional: an interrupted send stays uncertain and
is not registered again.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

from .relay_client import CancelOutcome, PullOutcome, RegisterOutcome


class RelayAdapter(Protocol):
    def validate_config(self) -> None:
        """Raise PluginError when the origin or the key cannot be used."""

    def dispatch(
        self,
        envelope: dict,
        envelope_hash: str,
        hop: int,
        deadline: datetime,
        *,
        attempts_used: int,
        on_phase,
    ) -> RegisterOutcome:
        """POST /jobs. ``accepted`` means the relay accepted the wake, not a result."""

    def reconcile(self, job_id: str) -> str:
        """Report local uncertainty. Must not send the registration again."""

    def cancel(self, job_id: str, deadline: datetime | None) -> CancelOutcome:
        """POST /jobs/{id}/cancel. Best effort after a wake has started."""

    def pull_results(self, since: str, deadline: datetime | None) -> PullOutcome:
        """GET /results?since=. Non-destructive on the relay."""
