"""Profile-scoped SQLite queue.

Hermes opens the file through ``plugins.plugin_storage.plugin_db("dome-relay")``,
under the active profile's plugin-data directory. Nothing here is written into
the plugin install tree. Audit rows store event names and ids only: no brief,
no result text, no key, and no URL.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

from .hashutil import format_utc, parse_utc


PLUGIN_DB_NAME = "dome-relay"

# A job we already handed to the relay still occupies the one slot for its target.
# queued and held_local have not been sent, so they do not.
SLOT_STATES = ("registered", "dispatching", "retry_wait", "accepted", "held", "uncertain")
ACTIVE_STATES = SLOT_STATES + ("queued", "held_local")
TERMINAL_STATES = ("done", "failed", "canceled", "expired", "rejected")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Store:
    """One connection. A lock keeps a claim and a cursor update from interleaving."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._failpoint: str | None = None
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL").fetchone()
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._create_tables()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _create_tables(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                profile TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                target TEXT NOT NULL,
                sender TEXT NOT NULL,
                brief TEXT NOT NULL,
                priority TEXT NOT NULL,
                deadline TEXT NOT NULL,
                created_at TEXT NOT NULL,
                callback_url TEXT NOT NULL,
                origin TEXT NOT NULL,
                artifacts_json TEXT NOT NULL,
                execution_location TEXT NOT NULL,
                hop INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                envelope_hash TEXT NOT NULL,
                state TEXT NOT NULL,
                wake_status TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                attempts_used INTEGER NOT NULL,
                registered INTEGER NOT NULL,
                cancel_pending INTEGER NOT NULL,
                created_row_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_target_state ON jobs (target, state);
            CREATE INDEX IF NOT EXISTS idx_jobs_conversation ON jobs (profile, conversation_id);

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

            CREATE TABLE IF NOT EXISTS result_inbox (
                cursor TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                status TEXT NOT NULL,
                result_text TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                body_hash TEXT NOT NULL,
                received_at TEXT NOT NULL,
                announced INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS consumer_cursors (
                name TEXT PRIMARY KEY,
                cursor TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS delivery_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                result_cursor TEXT NOT NULL,
                conversation_id TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (result_cursor)
            );

            CREATE TABLE IF NOT EXISTS heartbeat (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                beat_at TEXT NOT NULL,
                owner TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS worker_lease (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                owner TEXT NOT NULL,
                until_at TEXT NOT NULL
            );
            """
        )

    def _tx(self):
        return _Transaction(self)

    def _audit(self, event: str, job_id: str | None, detail: dict, now: str) -> None:
        self._conn.execute(
            """
            INSERT INTO audit_events (job_id, event, detail, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (job_id, event, json.dumps(detail, sort_keys=True), now),
        )

    def _get(self, job_id: str) -> dict | None:
        row = self._conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return dict(row) if row else None

    def _is_expired(self, job: dict, now: datetime) -> bool:
        if now >= parse_utc(job["expires_at"]):
            return True
        if now >= parse_utc(job["created_at"]) + timedelta(minutes=30):
            return True
        return False

    def insert_job(self, row: dict, now: datetime) -> tuple[str, dict]:
        """Insert, or report duplicate / conflict. Same id + same hash is a duplicate."""
        stamp = format_utc(now)
        with self._tx() as tx:
            existing = self._get(row["job_id"])
            if existing is not None:
                if existing["envelope_hash"] == row["envelope_hash"] and int(existing["hop"]) == int(row["hop"]):
                    self._audit("deduped", row["job_id"], {}, stamp)
                    return "duplicate", existing
                self._audit("conflict", row["job_id"], {}, stamp)
                return "conflict", existing
            self._conn.execute(
                """
                INSERT INTO jobs (
                    job_id, profile, conversation_id, session_id, target, sender, brief,
                    priority, deadline, created_at, callback_url, origin, artifacts_json,
                    execution_location, hop, attempt, envelope_hash, state, wake_status,
                    expires_at, attempts_used, registered, cancel_pending, created_row_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, 'pending',
                    ?, 0, 0, 0, ?, ?
                )
                """,
                (
                    row["job_id"],
                    row["profile"],
                    row["conversation_id"],
                    row["session_id"],
                    row["target"],
                    row["sender"],
                    row["brief"],
                    row["priority"],
                    row["deadline"],
                    row["created_at"],
                    row["callback_url"],
                    row["origin"],
                    row["artifacts_json"],
                    row["execution_location"],
                    int(row["hop"]),
                    int(row["attempt"]),
                    row["envelope_hash"],
                    row["state"],
                    row["expires_at"],
                    stamp,
                    stamp,
                ),
            )
            self._audit(
                "queued",
                row["job_id"],
                {"target": row["target"], "hop": int(row["hop"]), "execution_location": row["execution_location"]},
                stamp,
            )
            return "created", self._get(row["job_id"])

    def get_for_origin(self, job_id: str, profile: str, conversation_id: str, session_id: str) -> dict | None:
        """Return the job only when this profile and this conversation own it.

        Hermes v0.23.0 tool calls carry ``session_id`` and not ``conversation_id``.
        A later build may pass both. Either id may match the ids stored at escalate
        time. An empty id never matches, so a call with no session cannot read a job.
        """
        with self._lock:
            job = self._get(job_id)
        if job is None or job["profile"] != profile:
            return None
        caller = {conversation_id, session_id} - {""}
        stored = {job["conversation_id"], job["session_id"]} - {""}
        if not caller or not stored or caller.isdisjoint(stored):
            return None
        return job

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            return self._get(job_id)

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
                  FROM attempts WHERE job_id = ? ORDER BY id
                """,
                (job_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def touch_heartbeat(self, now: datetime, owner: str) -> None:
        stamp = format_utc(now)
        with self._tx():
            self._conn.execute(
                """
                INSERT INTO heartbeat (id, beat_at, owner) VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET beat_at = excluded.beat_at, owner = excluded.owner
                """,
                (stamp, owner),
            )

    def heartbeat_age_seconds(self, now: datetime) -> float | None:
        with self._lock:
            row = self._conn.execute("SELECT beat_at FROM heartbeat WHERE id = 1").fetchone()
        if row is None:
            return None
        return (now - parse_utc(row["beat_at"])).total_seconds()

    def heartbeat_fresh(self, now: datetime, window_seconds: float) -> bool:
        age = self.heartbeat_age_seconds(now)
        return age is not None and age <= window_seconds

    def acquire_lease(self, owner: str, now: datetime, ttl_seconds: float) -> bool:
        until = format_utc(now + timedelta(seconds=ttl_seconds))
        stamp = format_utc(now)
        with self._tx():
            row = self._conn.execute("SELECT owner, until_at FROM worker_lease WHERE id = 1").fetchone()
            if row is not None and row["owner"] != owner and parse_utc(row["until_at"]) > now:
                return False
            self._conn.execute(
                """
                INSERT INTO worker_lease (id, owner, until_at) VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET owner = excluded.owner, until_at = excluded.until_at
                """,
                (owner, until),
            )
            self._audit("lease_acquired", None, {}, stamp)
            return True

    def release_lease(self, owner: str, now: datetime) -> None:
        stamp = format_utc(now)
        with self._tx():
            self._conn.execute(
                "UPDATE worker_lease SET until_at = ? WHERE id = 1 AND owner = ?",
                (stamp, owner),
            )

    def recover_inflight(self, now: datetime) -> list[str]:
        """A crash mid-request is uncertain and is not sent again.

        A crash during the wait after a connect failure (``retry_wait``) did not
        leave a request in flight, so that job goes back to the queue and keeps
        its attempt count.
        """
        stamp = format_utc(now)
        changed: list[str] = []
        with self._tx():
            inflight = self._conn.execute("SELECT job_id FROM jobs WHERE state = 'dispatching'").fetchall()
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = 'uncertain', wake_status = 'uncertain', brief = '', updated_at = ?
                 WHERE state = 'dispatching'
                """,
                (stamp,),
            )
            for row in inflight:
                self._audit("recovered_uncertain", row["job_id"], {}, stamp)
                changed.append(row["job_id"])
            waiting = self._conn.execute(
                "SELECT job_id, attempts_used FROM jobs WHERE state = 'retry_wait'"
            ).fetchall()
            for row in waiting:
                if int(row["attempts_used"]) >= 3:
                    self._conn.execute(
                        "UPDATE jobs SET state = 'rejected', brief = '', updated_at = ? WHERE job_id = ?",
                        (stamp, row["job_id"]),
                    )
                    self._audit("retries_exhausted", row["job_id"], {}, stamp)
                else:
                    self._conn.execute(
                        "UPDATE jobs SET state = 'queued', updated_at = ? WHERE job_id = ?",
                        (stamp, row["job_id"]),
                    )
                    self._audit("retry_resumed", row["job_id"], {"attempts_used": int(row["attempts_used"])}, stamp)
                changed.append(row["job_id"])
        return changed

    def expire_due(self, now: datetime) -> None:
        stamp = format_utc(now)
        with self._tx():
            rows = self._conn.execute(
                f"SELECT * FROM jobs WHERE state IN ({','.join('?' for _ in ACTIVE_STATES)})",
                ACTIVE_STATES,
            ).fetchall()
            for row in rows:
                job = dict(row)
                if self._is_expired(job, now):
                    self._conn.execute(
                        "UPDATE jobs SET state = 'expired', brief = '', updated_at = ? WHERE job_id = ?",
                        (stamp, job["job_id"]),
                    )
                    self._audit("expired", job["job_id"], {}, stamp)

    def promote_held_local(self, now: datetime, window_seconds: float) -> None:
        if not self.heartbeat_fresh(now, window_seconds):
            return
        stamp = format_utc(now)
        with self._tx():
            self._conn.execute(
                """
                UPDATE jobs SET state = 'queued', updated_at = ?
                 WHERE state = 'held_local'
                """,
                (stamp,),
            )

    def mark_held_local(self, job_id: str, now: datetime) -> None:
        stamp = format_utc(now)
        with self._tx():
            self._conn.execute(
                "UPDATE jobs SET state = 'held_local', updated_at = ? WHERE job_id = ? AND state = 'queued'",
                (stamp, job_id),
            )
            self._audit("held_local", job_id, {}, stamp)

    def target_busy(self, target: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                f"SELECT job_id FROM jobs WHERE target = ? AND state IN ({','.join('?' for _ in SLOT_STATES)})",
                (target, *SLOT_STATES),
            ).fetchone()
        return row is not None

    def list_queued(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE state = 'queued' ORDER BY created_row_at, job_id"
            ).fetchall()
        return [dict(row) for row in rows]

    def has_active(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                f"SELECT job_id FROM jobs WHERE state IN ({','.join('?' for _ in ACTIVE_STATES)}) LIMIT 1",
                ACTIVE_STATES,
            ).fetchone()
        return row is not None

    def claim_dispatch(self, job_id: str, now: datetime) -> dict | None:
        stamp = format_utc(now)
        with self._tx():
            cursor = self._conn.execute(
                """
                UPDATE jobs SET state = 'dispatching', updated_at = ?
                 WHERE job_id = ? AND state = 'queued'
                """,
                (stamp, job_id),
            )
            if cursor.rowcount != 1:
                return None
            self._audit("dispatching", job_id, {}, stamp)
            return self._get(job_id)

    def note_phase(self, job_id: str, phase: str, attempt: int, outcome: str | None, http_status: int | None, now: datetime) -> None:
        stamp = format_utc(now)
        with self._tx():
            if phase == "retry_wait":
                self._conn.execute(
                    "UPDATE jobs SET state = 'retry_wait', attempts_used = ?, updated_at = ? WHERE job_id = ?",
                    (attempt, stamp, job_id),
                )
            elif phase == "dispatching":
                self._conn.execute(
                    "UPDATE jobs SET state = 'dispatching', updated_at = ? WHERE job_id = ?",
                    (stamp, job_id),
                )
            if outcome:
                self._conn.execute(
                    """
                    INSERT INTO attempts (job_id, attempt_no, outcome, http_status, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (job_id, attempt, outcome, http_status, stamp),
                )
                self._conn.execute(
                    "UPDATE jobs SET attempts_used = ?, updated_at = ? WHERE job_id = ?",
                    (attempt, stamp, job_id),
                )
                self._audit("attempt", job_id, {"attempt": attempt, "outcome": outcome, "http_status": http_status}, stamp)

    def revert_to_queued(self, job_id: str, now: datetime, reason: str) -> None:
        stamp = format_utc(now)
        with self._tx():
            self._conn.execute(
                """
                UPDATE jobs SET state = 'queued', updated_at = ?
                 WHERE job_id = ? AND state IN ('dispatching', 'retry_wait')
                """,
                (stamp, job_id),
            )
            self._audit("destination_blocked", job_id, {"reason": reason}, stamp)

    def finish_registration(self, job_id: str, *, state: str, wake_status: str, registered: bool, now: datetime) -> None:
        stamp = format_utc(now)
        wipe = state in {"accepted", "rejected", "uncertain", "registered"}
        with self._tx():
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = ?, wake_status = ?, registered = ?,
                       brief = CASE WHEN ? THEN '' ELSE brief END,
                       updated_at = ?
                 WHERE job_id = ?
                """,
                (state, wake_status, 1 if registered else 0, 1 if wipe else 0, stamp, job_id),
            )
            self._audit("registration_finished", job_id, {"state": state, "wake_status": wake_status}, stamp)

    def mark_expired(self, job_id: str, now: datetime) -> None:
        stamp = format_utc(now)
        with self._tx():
            self._conn.execute(
                "UPDATE jobs SET state = 'expired', brief = '', updated_at = ? WHERE job_id = ?",
                (stamp, job_id),
            )
            self._audit("expired", job_id, {}, stamp)

    def mark_canceled(self, job_id: str, now: datetime) -> dict | None:
        stamp = format_utc(now)
        with self._tx():
            job = self._get(job_id)
            if job is None:
                return None
            if job["state"] in ("done", "failed"):
                self._audit("cancel_rejected", job_id, {"reason": "terminal"}, stamp)
                return job
            if job["state"] == "expired" or self._is_expired(job, now):
                self._conn.execute(
                    "UPDATE jobs SET state = 'expired', brief = '', updated_at = ? WHERE job_id = ?",
                    (stamp, job_id),
                )
                self._audit("expired", job_id, {}, stamp)
                return self._get(job_id)
            if job["state"] == "canceled":
                return job
            pending = 1 if int(job["registered"]) == 1 else 0
            self._conn.execute(
                """
                UPDATE jobs
                   SET state = 'canceled', brief = '', cancel_pending = ?, updated_at = ?
                 WHERE job_id = ?
                """,
                (pending, stamp, job_id),
            )
            self._audit("canceled", job_id, {"cancel_pending": pending}, stamp)
            return self._get(job_id)

    def list_cancel_pending(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE state = 'canceled' AND cancel_pending = 1"
            ).fetchall()
        return [dict(row) for row in rows]

    def clear_cancel_pending(self, job_id: str, now: datetime) -> None:
        stamp = format_utc(now)
        with self._tx():
            self._conn.execute(
                "UPDATE jobs SET cancel_pending = 0, updated_at = ? WHERE job_id = ?",
                (stamp, job_id),
            )
            self._audit("cancel_sent", job_id, {}, stamp)

    def results_cursor(self) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT cursor FROM consumer_cursors WHERE name = 'results'"
            ).fetchone()
        if row is None:
            return "c0"
        return str(row["cursor"])

    def apply_result_batch(self, entries: list[dict], next_cursor: str, now: datetime) -> int:
        """Save the inbox, the cursor, and delivery rows in one transaction.

        A crash before commit leaves the cursor where it was, so the next pull
        returns the same batch. Unknown job ids are not stored (their text is
        not this profile's) but the cursor still advances past them.
        """
        stamp = format_utc(now)
        stored = 0
        with self._tx():
            for entry in entries:
                cursor = entry["cursor"]
                result = entry["result"]
                existing = self._conn.execute(
                    "SELECT cursor FROM result_inbox WHERE cursor = ?", (cursor,)
                ).fetchone()
                if existing is not None:
                    continue
                job = self._get(result["job_id"])
                if job is None:
                    self._audit("result_ignored", result["job_id"], {"cursor": cursor}, stamp)
                    continue
                duplicate = self._conn.execute(
                    "SELECT cursor FROM result_inbox WHERE job_id = ? AND body_hash = ?",
                    (result["job_id"], result["body_hash"]),
                ).fetchone()
                if duplicate is not None:
                    self._audit("duplicate_result", result["job_id"], {"cursor": cursor}, stamp)
                    continue
                self._conn.execute(
                    """
                    INSERT INTO result_inbox (
                        cursor, job_id, status, result_text, completed_at, body_hash, received_at, announced
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        cursor,
                        result["job_id"],
                        result["status"],
                        result["result"],
                        result["completed_at"],
                        result["body_hash"],
                        stamp,
                    ),
                )
                stored += 1
                if job["state"] not in ("canceled", "expired", "done", "failed"):
                    self._conn.execute(
                        "UPDATE jobs SET state = ?, brief = '', updated_at = ? WHERE job_id = ?",
                        (result["status"], stamp, result["job_id"]),
                    )
                delivery_state = "delivery_pending"
                if job["state"] == "canceled":
                    delivery_state = "suppressed"
                self._conn.execute(
                    """
                    INSERT INTO delivery_outbox (job_id, result_cursor, conversation_id, state, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (result["job_id"], cursor, job["conversation_id"], delivery_state, stamp),
                )
                self._audit(
                    "result_stored",
                    result["job_id"],
                    {"status": result["status"], "cursor": cursor},
                    stamp,
                )
            self._conn.execute(
                """
                INSERT INTO consumer_cursors (name, cursor, updated_at) VALUES ('results', ?, ?)
                ON CONFLICT(name) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at
                """,
                (next_cursor, stamp),
            )
            if self._failpoint == "before_commit":
                raise RuntimeError("failpoint before_commit")
        return stored

    def latest_result(self, job_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT cursor, job_id, status, result_text, completed_at
                  FROM result_inbox WHERE job_id = ?
                 ORDER BY CAST(substr(cursor, 2) AS INTEGER) DESC
                 LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "cursor": row["cursor"],
            "status": row["status"],
            "result": row["result_text"],
            "completed_at": row["completed_at"],
        }

    def unannounced_results(self, profile: str, conversation_id: str, session_id: str = "") -> list[dict]:
        keys = [key for key in (conversation_id, session_id) if key]
        if not keys:
            return []
        marks = ",".join("?" for _ in keys)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT inbox.cursor, inbox.job_id, inbox.status, inbox.result_text, inbox.completed_at
                  FROM result_inbox AS inbox
                  JOIN jobs ON jobs.job_id = inbox.job_id
                 WHERE inbox.announced = 0
                   AND jobs.profile = ?
                   AND jobs.state != 'canceled'
                   AND (
                        jobs.conversation_id IN ({marks})
                        OR jobs.session_id IN ({marks})
                   )
                 ORDER BY CAST(substr(inbox.cursor, 2) AS INTEGER)
                """,
                (profile, *keys, *keys),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_announced(self, cursors: list[str], now: datetime) -> None:
        if not cursors:
            return
        stamp = format_utc(now)
        with self._tx():
            for cursor in cursors:
                self._conn.execute(
                    "UPDATE result_inbox SET announced = 1 WHERE cursor = ?",
                    (cursor,),
                )
                self._conn.execute(
                    """
                    UPDATE delivery_outbox SET state = 'delivered'
                     WHERE result_cursor = ? AND state = 'delivery_pending'
                    """,
                    (cursor,),
                )
            self._audit("results_announced", None, {"count": len(cursors)}, stamp)


class _Transaction:
    def __init__(self, store: Store):
        self._store = store

    def __enter__(self):
        self._store._lock.acquire()
        self._store._conn.execute("BEGIN IMMEDIATE")
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self._store._conn.execute("COMMIT")
            else:
                self._store._conn.execute("ROLLBACK")
        finally:
            self._store._lock.release()
        return False


def open_store() -> Store:
    """Open the profile database. Tests pass a connection to ``Store`` directly."""
    from plugins.plugin_storage import plugin_db

    return Store(plugin_db(PLUGIN_DB_NAME))


def utc_now() -> datetime:
    return _now()
