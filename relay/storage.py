"""SQLite storage for the Dome Relay.

The database file lives on the Fly volume (see fly.toml), so jobs and results
survive a restart. This process is the only writer. Do not run a second
machine against the same file.

What we keep
- One row per job_id. The database rejects a second row with the same id.
- Result rows, in the order they were accepted. Reading them does not delete
  them and does not move a cursor. The plugin saves its own cursor.
- An audit trail of what happened. The trail stores event names and ids only.
  It must never store the task text, the result text, a key, or the webhook URL.
- A short record of each wake attempt (the POST to the Grok Bot routine).

The task text itself is stored only while a wake has not been sent yet. After
the wake finishes (started, refused, or uncertain), that text is wiped. Results
stay, because the original agent still needs to collect them.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path


# A job may not live longer than this, measured from the relay's own clock
# at the moment the job was received. A client-supplied time cannot extend it.
JOB_LIFETIME = timedelta(minutes=30)

# States that still occupy the single "accepted work" slot for a target.
# A held update is not finished, so it keeps the slot. done/failed/canceled/
# expired do not.
SLOT_STATES = ("accepted", "held")

# States we are allowed to expire. Terminal results and cancellations stay as
# they are so a late timer cannot rewrite history.
EXPIRABLE_STATES = ("registered", "dispatching", "uncertain", "accepted", "held")


def format_utc(value: datetime) -> str:
    """Write a timestamp as UTC RFC3339, ending in Z, to the second."""
    value = value.astimezone(timezone.utc).replace(microsecond=0)
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(value: str) -> datetime:
    """Read a timestamp this module stored (always Z, always aware)."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Store:
    """One SQLite connection, guarded by a lock so claims cannot interleave."""

    def __init__(self, path: str):
        self.path = path
        database = Path(path)
        # The parent folder is the Fly volume mount (/data). Create it only
        # for local runs; on Fly the mount already exists.
        if database.parent and str(database.parent) not in ("", "."):
            database.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level None means we start and commit transactions ourselves.
        # check_same_thread False is safe because every use takes self._lock.
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            # FULL waits for the disk before we say a claim was committed.
            # That is what makes "claim once" survive a crash.
            # fetchone() so the PRAGMA result is not left sitting on the connection.
            self._conn.execute("PRAGMA journal_mode=WAL").fetchone()
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._create_tables()
            # A wake that was in flight when the process died is uncertain:
            # the routine might have started. Do not treat it as unsent.
            self._recover_inflight()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _create_tables(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                target TEXT NOT NULL,
                deadline TEXT NOT NULL,
                envelope_hash TEXT NOT NULL,
                hop INTEGER NOT NULL,
                envelope_json TEXT NOT NULL,
                state TEXT NOT NULL,
                wake_status TEXT NOT NULL,
                received_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                claimed_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_target_state
                ON jobs (target, state);

            CREATE TABLE IF NOT EXISTS results (
                cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                status TEXT NOT NULL,
                result_text TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                received_at TEXT NOT NULL,
                body_hash TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_results_job
                ON results (job_id, body_hash);

            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                attempt_no INTEGER NOT NULL,
                outcome TEXT NOT NULL,
                http_status INTEGER,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                event TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )

    def _recover_inflight(self) -> None:
        """Mark an interrupted wake as uncertain and drop the task text.

        Called only while the lock is already held (from __init__).
        """
        now = format_utc(datetime.now(timezone.utc))
        rows = self._conn.execute(
            "SELECT job_id FROM jobs WHERE state = 'dispatching'"
        ).fetchall()
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = 'uncertain',
                       wake_status = 'uncertain',
                       envelope_json = '',
                       updated_at = ?
                 WHERE state = 'dispatching'
                """,
                (now,),
            )
            for row in rows:
                self._audit(
                    "wake_recovered_uncertain",
                    row["job_id"],
                    {},
                    now,
                )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    @contextmanager
    def _tx(self):
        """A transaction that blocks other writers in this process."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def _get(self, job_id: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
        ).fetchone()
        return dict(row) if row else None

    def _audit(self, event: str, job_id: str | None, detail: dict, now: str) -> None:
        # detail is a small dict of codes we chose. Never pass task text here.
        self._conn.execute(
            """
            INSERT INTO audit_events (job_id, event, detail, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (job_id, event, json.dumps(detail, sort_keys=True), now),
        )

    def _is_expired(self, job: dict, now: datetime) -> bool:
        """True when the server clock says the job is past its window.

        expires_at was chosen when we received the job. The +30 minute check
        is a second guard so a bad row cannot live longer than the locked window.
        """
        if now >= parse_utc(job["expires_at"]):
            return True
        if now >= parse_utc(job["received_at"]) + JOB_LIFETIME:
            return True
        return False

    def _mark_expired(self, job: dict, now: datetime) -> None:
        stamp = format_utc(now)
        self._conn.execute(
            """
            UPDATE jobs
               SET state = 'expired', envelope_json = '', updated_at = ?
             WHERE job_id = ?
            """,
            (stamp, job["job_id"]),
        )
        self._audit("expired", job["job_id"], {}, stamp)

    def _expire_overdue(self, now: datetime) -> None:
        rows = self._conn.execute(
            f"""
            SELECT * FROM jobs
             WHERE state IN ({",".join("?" for _ in EXPIRABLE_STATES)})
            """,
            EXPIRABLE_STATES,
        ).fetchall()
        for row in rows:
            job = dict(row)
            if self._is_expired(job, now):
                self._mark_expired(job, now)

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            return self._get(job_id)

    def ping(self) -> None:
        """Used by /healthz. Raises if the database file cannot be read."""
        with self._lock:
            self._conn.execute("SELECT 1").fetchone()

    def audit(self, event: str, job_id: str | None, detail: dict, now: datetime) -> None:
        with self._tx():
            self._audit(event, job_id, detail, format_utc(now))

    def list_audit(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, job_id, event, detail, created_at FROM audit_events ORDER BY id"
            ).fetchall()
        return [dict(row) for row in rows]

    def list_attempts(self, job_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT attempt_no, outcome, http_status, created_at
                  FROM attempts
                 WHERE job_id = ?
                 ORDER BY id
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def attempt_count(self, job_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM attempts WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return int(row["n"])

    def add_attempt(
        self,
        job_id: str,
        attempt_no: int,
        outcome: str,
        http_status: int | None,
        now: datetime,
    ) -> None:
        with self._tx():
            self._conn.execute(
                """
                INSERT INTO attempts (job_id, attempt_no, outcome, http_status, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, attempt_no, outcome, http_status, format_utc(now)),
            )

    def expire_overdue(self, now: datetime) -> None:
        """Used by the health check so a dead claim does not hold a slot forever."""
        with self._tx():
            self._expire_overdue(now)

    def insert_job(
        self,
        *,
        job_id: str,
        target: str,
        deadline: str,
        envelope_hash: str,
        hop: int,
        envelope_json: str,
        received_at: datetime,
        expires_at: datetime,
    ) -> tuple[str, dict]:
        """Insert a new job, or report that this id was already used.

        Returns ("created", row), ("duplicate", row), or ("conflict", row).
        Same id + same envelope hash + same hop is a duplicate (no second row).
        Same id with different contents is a conflict. The existing row is kept.
        """
        received = format_utc(received_at)
        expires = format_utc(expires_at)
        with self._tx():
            existing = self._get(job_id)
            if existing is not None:
                same = (
                    existing["envelope_hash"] == envelope_hash
                    and int(existing["hop"]) == int(hop)
                )
                if same:
                    self._audit("deduped", job_id, {}, received)
                    return "duplicate", existing
                self._audit("registration_conflict", job_id, {}, received)
                return "conflict", existing
            self._conn.execute(
                """
                INSERT INTO jobs (
                    job_id, target, deadline, envelope_hash, hop, envelope_json,
                    state, wake_status, received_at, expires_at, claimed_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'registered', 'pending', ?, ?, NULL, ?)
                """,
                (
                    job_id,
                    target,
                    deadline,
                    envelope_hash,
                    hop,
                    envelope_json,
                    received,
                    expires,
                    received,
                ),
            )
            self._audit("registered", job_id, {"target": target, "hop": hop}, received)
            return "created", self._get(job_id)

    def begin_wake(self, job_id: str, now: datetime) -> dict | None:
        """Take the one chance to send this job, if it has never been sent.

        Returns the row (including the task text) or None when someone else
        already sent it, or the wake already finished. Only a 'pending' job
        can start. Accepted, rejected, and uncertain wakes are not sent again.
        """
        stamp = format_utc(now)
        with self._tx():
            cursor = self._conn.execute(
                """
                UPDATE jobs
                   SET state = 'dispatching', updated_at = ?
                 WHERE job_id = ?
                   AND state = 'registered'
                   AND wake_status = 'pending'
                """,
                (stamp, job_id),
            )
            if cursor.rowcount != 1:
                return None
            return self._get(job_id)

    def finish_wake(self, job_id: str, wake_status: str, now: datetime) -> None:
        """Record how the webhook call ended and wipe the task text.

        If the executive already claimed the job, or the plugin canceled it,
        the state is left alone. We only move 'dispatching' forward.
        """
        stamp = format_utc(now)
        with self._tx():
            job = self._get(job_id)
            if job is None:
                return
            state = job["state"]
            if state == "dispatching":
                if self._is_expired(job, now):
                    state = "expired"
                elif wake_status == "uncertain":
                    state = "uncertain"
                else:
                    # 200 means the routine started, not that the work is done.
                    # The job stays 'registered' until the executive claims it.
                    # A refusal also returns to 'registered' so the row remains
                    # visible, but wake_status 'rejected' blocks another send.
                    state = "registered"
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = ?,
                       wake_status = ?,
                       envelope_json = '',
                       updated_at = ?
                 WHERE job_id = ?
                """,
                (state, wake_status, stamp, job_id),
            )
            self._audit("wake_finished", job_id, {"wake_status": wake_status}, stamp)

    def claim(self, job_id: str, now: datetime) -> dict:
        """Claim a job once, and only if the target has a free slot.

        The whole check-and-update is one transaction, so two claims cannot
        both win. A second claim, a canceled job, and an expired job lose.
        """
        with self._tx():
            self._expire_overdue(now)
            job = self._get(job_id)
            stamp = format_utc(now)
            if job is None:
                self._audit("claim_rejected", job_id, {"reason": "not_found"}, stamp)
                return {"error": "not_found"}
            if job["state"] == "canceled":
                self._audit("claim_rejected", job_id, {"reason": "canceled"}, stamp)
                return {"error": "canceled"}
            if job["state"] == "expired" or self._is_expired(job, now):
                if job["state"] != "expired":
                    self._mark_expired(job, now)
                self._audit("claim_rejected", job_id, {"reason": "expired"}, stamp)
                return {"error": "expired"}
            if job["state"] in ("accepted", "held"):
                self._audit(
                    "claim_rejected", job_id, {"reason": "already_claimed"}, stamp
                )
                return {"error": "already_claimed"}
            if job["state"] in ("done", "failed"):
                self._audit("claim_rejected", job_id, {"reason": "terminal"}, stamp)
                return {"error": "terminal"}
            if job["state"] not in ("registered", "uncertain", "dispatching"):
                self._audit(
                    "claim_rejected",
                    job_id,
                    {"reason": "not_claimable"},
                    stamp,
                )
                return {"error": "not_claimable"}
            # One outstanding accepted (or held) job per target. The slot is
            # reserved here and released on a terminal result, a cancel, or expiry.
            occupied = self._conn.execute(
                f"""
                SELECT job_id FROM jobs
                 WHERE target = ?
                   AND state IN ({",".join("?" for _ in SLOT_STATES)})
                """,
                (job["target"], *SLOT_STATES),
            ).fetchone()
            if occupied is not None:
                self._audit("claim_rejected", job_id, {"reason": "slot_busy"}, stamp)
                return {"error": "slot_busy"}
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = 'accepted', claimed_at = ?, updated_at = ?
                 WHERE job_id = ?
                """,
                (stamp, stamp, job_id),
            )
            self._audit("claimed", job_id, {"target": job["target"]}, stamp)
            return {"job": self._get(job_id)}

    def complete(
        self,
        *,
        job_id: str,
        status: str,
        result_text: str,
        completed_at: str,
        body_hash: str,
        now: datetime,
    ) -> dict:
        """Store a result if this job was claimed and is still inside the window.

        The first done/failed result wins. An identical repeat is duplicate_result.
        A different result after a terminal one is a conflict. Neither one changes
        the stored terminal result. A late (expired) body is dropped on the floor:
        we audit that it was quarantined and we do not bring the job back.
        """
        with self._tx():
            self._expire_overdue(now)
            job = self._get(job_id)
            stamp = format_utc(now)
            if job is None:
                self._audit(
                    "completion_rejected", job_id, {"reason": "not_found"}, stamp
                )
                return {"error": "not_found"}
            if job["state"] == "canceled":
                self._audit(
                    "completion_rejected", job_id, {"reason": "canceled"}, stamp
                )
                return {"error": "canceled"}
            # Expiry uses the relay clock, never the caller's completed_at.
            if job["state"] == "expired" or (
                job["state"] not in ("done", "failed") and self._is_expired(job, now)
            ):
                if job["state"] not in ("expired", "done", "failed"):
                    self._mark_expired(job, now)
                self._audit("late_callback_quarantined", job_id, {}, stamp)
                return {"error": "expired"}
            existing = self._conn.execute(
                """
                SELECT cursor FROM results
                 WHERE job_id = ? AND body_hash = ?
                """,
                (job_id, body_hash),
            ).fetchone()
            if existing is not None:
                self._audit(
                    "duplicate_result",
                    job_id,
                    {"cursor": int(existing["cursor"])},
                    stamp,
                )
                return {"error": "duplicate_result"}
            terminal = self._conn.execute(
                """
                SELECT cursor FROM results
                 WHERE job_id = ? AND status IN ('done', 'failed')
                 ORDER BY cursor DESC
                 LIMIT 1
                """,
                (job_id,),
            ).fetchone()
            if terminal is not None:
                self._audit("result_conflict", job_id, {}, stamp)
                return {"error": "conflict"}
            if job["state"] not in SLOT_STATES:
                self._audit(
                    "completion_rejected", job_id, {"reason": "not_claimed"}, stamp
                )
                return {"error": "not_claimed"}
            cursor = self._conn.execute(
                """
                INSERT INTO results (
                    job_id, status, result_text, completed_at, received_at, body_hash
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (job_id, status, result_text, completed_at, stamp, body_hash),
            )
            result_cursor = int(cursor.lastrowid)
            # held keeps the slot. done and failed release it.
            self._conn.execute(
                "UPDATE jobs SET state = ?, updated_at = ? WHERE job_id = ?",
                (status, stamp, job_id),
            )
            self._audit(
                "result_stored",
                job_id,
                {"status": status, "cursor": result_cursor},
                stamp,
            )
            return {"cursor": result_cursor, "state": status}

    def cancel(self, job_id: str, now: datetime) -> dict:
        """Stop a job locally. This cannot recall a routine that already started."""
        with self._tx():
            self._expire_overdue(now)
            job = self._get(job_id)
            stamp = format_utc(now)
            if job is None:
                return {"error": "not_found"}
            if job["state"] in ("done", "failed"):
                self._audit("cancel_rejected", job_id, {"reason": "terminal"}, stamp)
                return {"error": "terminal"}
            if job["state"] == "expired" or self._is_expired(job, now):
                if job["state"] != "expired":
                    self._mark_expired(job, now)
                return {"error": "expired"}
            if job["state"] == "canceled":
                return {"already": True, "job": job}
            previous = job["state"]
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = 'canceled', envelope_json = '', updated_at = ?
                 WHERE job_id = ?
                """,
                (stamp, job_id),
            )
            self._audit("canceled", job_id, {"previous": previous}, stamp)
            return {"already": False, "job": self._get(job_id)}

    def list_results(self, since_cursor: int, limit: int) -> list[dict]:
        """Return results newer than since_cursor. Does not delete or advance anything."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT cursor, job_id, status, result_text, completed_at
                  FROM results
                 WHERE cursor > ?
                 ORDER BY cursor ASC
                 LIMIT ?
                """,
                (since_cursor, limit),
            ).fetchall()
        return [dict(row) for row in rows]
