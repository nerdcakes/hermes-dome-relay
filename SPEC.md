# Dome Relay technical specification

**Status:** the relay in `relay/` and the Hermes plugin and local worker in `dome_relay/` are implemented. Target Hermes release: v0.23.0 (`requires_hermes: ">=0.23.0"`). Author: Juni, interim CTO. Relay rules below are the built service. One Hermes limit is called out in section 3A: v0.23.0 does not pass a compression-stable `conversation_id` to plugin tools.
**Target:** one real round trip before the November 11, 2026 noon HST contest deadline.

## 1. Goal and scope

Keep established assistants in their own workspaces. A local Hermes agent asks Celeste to escalate a job; the relay (not the plugin) wakes the selected Grok Bot executive; the executive uses its existing tools; an authenticated callback returns its result to the original conversation.

The MVP must also reject duplicates, hold local-only work while the worker is offline, and recover jobs and received results after process restart. Non-goals: Lyra's host adapter, email and Telegram (after the contest), arbitrary remote code execution, a new agent runtime, and moving identities into Hermes.

A real external executive and sanctioned callback mechanism must be proven. A mock executive is useful for tests but is not the live-demo acceptance criterion.

## 2. Current repository and exact Hermes integration contract

Read before changing the plugin: README.md, plugin.yaml, the root `__init__.py`, and `relay/` on main. The relay service, the three tools, and the local worker all run. The manifest must not declare the Grok webhook URL or the Grok key.

### Layout and entrypoint

Use a native directory plugin, not a portable plugin and not a gateway platform adapter:

```text
plugin.yaml
__init__.py                 # Hermes discovers register(ctx) HERE
SPEC.md
dome_relay/
  __init__.py               # existing helpers/package, not the loader entrypoint
  schemas.py
  tools.py
  storage.py
  worker.py
  adapters/base.py
  adapters/relay_client.py      # registers with the relay; never calls Grok
relay/
  app.py
  storage.py
  requirements.txt
schemas/
  envelope.schema.json
  result.schema.json
tests/
scripts/e2e.py
```

The manifest and loadable root `__init__.py` are adjacent. `register(ctx)` lives in that root file. The manifest uses `config_schema:` and does not set `entrypoint:`.

In root `register(ctx)`, register exactly `dome_relay_escalate`, `dome_relay_status`, `dome_relay_cancel` using `ctx.register_tool(name=..., toolset="dome_relay", schema=..., handler=...)`. Each schema has name, description and a JSON Schema parameters object. Handlers accept `(args: dict, **kwargs) -> str`, catch errors and return JSON strings, including errors. Do not shadow existing tools. `register()` must perform no network calls or launch workers; doctor imports it in isolation.

- escalate: target, brief, priority, deadline, artifacts, optional caller-supplied job_id and execution_location cloud/local. Persist before any send; return job_id and state immediately. If the caller omits `deadline`, the plugin sets it to 20 minutes from now. The plugin also rejects a deadline more than 30 minutes in the future. Derive sender and callback URL from trusted settings/context, never arbitrary model arguments.
- status: job_id; query the originating profile/conversation's job and latest result, never another user's job. This is the minimum durable return-to-origin mechanism.
- cancel: job_id; stop retries and future result injection. Cancellation after acceptance is local/best-effort; it cannot undo external work or reliably interrupt a running Grok Bot routine.

Manifest v2 uses top-level `provides_tools`, `provides_hooks`, `config_schema`, `requires_env`, `python_dependencies` and a LIST of privileged `capabilities`. Do not confuse this with a future catalog YAML's `capabilities` MAPPING, which summarizes actual tools/hooks/middleware/env names. Declare only implemented registrations. `provides_tools` lists the three tools. `provides_hooks` lists `pre_llm_call` and `post_compaction`.

Use `ctx.get_config("relay_url", default="")` for nonsecret plugin settings, resolved under `plugins.entries.dome-relay.settings`; `ctx.set_config` changes only that namespace. The only plugin secret is `DOME_RELAY_KEY`, read through `from agent.secret_scope import get_secret`, then `get_secret("DOME_RELAY_KEY")`. The plugin must never read `DOME_RELAY_GROK_KEY`, `DOME_RELAY_GROK_WEBHOOK_URL`, or `DOME_RELAY_CALLBACK_KEY`. Those live only as Fly secrets on the relay process. Never read `.env`, auth.json, browser profiles or a bare process-wide environment for Hermes secrets. Resolve the active profile's values before passing narrowly scoped credentials to a worker; do not reuse one global client across profiles.

Use `plugins.plugin_storage.plugin_db("dome-relay")` for profile-scoped durable SQLite storage and `plugin_data_dir` where files are necessary. Never put runtime databases in the plugin install tree. The worker is an explicitly started plugin-owned process/CLI command, with clean shutdown, bounded network timeouts and a single-worker lease. Keep root registration cheap. Final implementation must use public lifecycle APIs and must declare any added hooks/commands honestly.

Do not invent a minimum Hermes version: pin a released test version and derive the truthful SemVer floor from tests. Current guide has contradictory prose about api_version/global API numbering; omit optional api_version and rely on documented behaviors plus validation.

## 3. Components and flow

### A. Plugin and worker

SQLite tables: jobs, attempts, audit_events, result_inbox, consumer_cursors, delivery_outbox, and session_lineage. Jobs retain profile, original conversation_id, session-routing metadata, immutable envelope hash, local execution requirement, timestamps, lease and terminal state.

Hermes v0.23.0 tool calls receive the physical `session_id` and do not receive a compression-stable `conversation_id`. That argument was added to Hermes after the v0.23.0 tag (plugin tools and tool hooks only; this plugin does not patch Hermes). Rotating compression (`compression.in_place: false`) therefore changes the id a later tool call sees. The plugin records a lineage edge only when the public `post_compaction` hook reports a new `session_id` and a different `old_session_id`. Status, cancel, a repeated escalate, and optional result injection follow that chain inside the same profile. A `parent_session_id` on `pre_llm_call` is not ownership: in v0.23.0 it is the delegating agent, not the compression parent, and treating it as proof would admit a different session. In-place compression keeps the session id, so no edge is required. If `post_compaction` did not run in this profile, a later call that only has the new session id is `not_found`. The plugin does not read Hermes session tables. When a future Hermes passes `conversation_id`, that id is the stable key: two non-empty conversation ids must match, and a different conversation id does not inherit the session.

The worker runs network I/O without model-token polling. A configured interval checks pending work and pulls relay results; default 15 seconds while active, with backoff when idle or unreachable. This is process polling, not a recurring executive routine. Persist cursor and inbox entries atomically before applying results.

The supported return path is status retrieval in the origin conversation. `inject_context` and `inject_message` default to false. Optional `ctx.inject_message` is not a supported push path: a gateway routing key is not the transcript session id, and this plugin does not invent one. `pre_llm_call` context, when enabled, is ephemeral and applies to the current turn only. It is limited to the same origin check as status. A result that cannot be matched to this conversation stays available through status. Optional delivery is at-least-once when it runs, not exactly once across a crash at the host boundary.

Local expiry and a terminal relay result are different. `expired` means this profile stopped waiting and the job no longer reserves its target. A `done` or `failed` result the relay accepted before the deadline still becomes the job state when it is retrieved late. A `held` result stays stored for inspection, and after the deadline the operational state is `expired` so a later job for that target can be sent. A canceled job does not return a result. A stored result is not a new turn in the originating task and does not apply the result as code.

### B. Plugin dispatch

Interface (conceptual, not Hermes API): `validate_config`, `dispatch(envelope) -> accepted | rejected | uncertain`, and optional `reconcile(job_id)`. Do not assume all targets support cancel or idempotent execution.

The plugin dispatches by registering the job with the relay (`POST /jobs` on `relay_url`, `Authorization: Bearer <get_secret("DOME_RELAY_KEY")>`). Registration is the plugin's only network call for a new job. The relay performs the Grok wake. The plugin must never POST to the Grok routine, must never hold `DOME_RELAY_GROK_KEY` or `DOME_RELAY_GROK_WEBHOOK_URL`, and must never retry by calling Grok itself. A 201 from the relay means the job was stored and the relay attempted the wake. A 200 means the same job was already registered. Read `wake_status` on that response: `accepted` means the routine run started, and it is not a result. `rejected` means Grok did not start a run. `uncertain` means the relay cannot tell whether the request arrived. The plugin must not send its own wake in any of those cases. A transport timeout after the registration request was sent is uncertain. Repeating the same registration is safe: the relay will not wake again after an accepted, rejected, or uncertain wake. It will send once only if the first process died before the webhook call (`wake_status` still `pending`).

### C. Always-on callback relay

Implemented in `relay/` (FastAPI). Host recipe: one Fly machine, `auto_stop_machines = "off"` and `min_machines_running = 1`, SQLite on a Fly volume mounted at `/data`. An ephemeral disk cannot satisfy restart recovery. Two machines must not share the file. Free-tier deployment is an option, not a guarantee: verify persistence, quotas, sleep behavior, TLS and costs.

The relay, not the plugin, sends the Grok wake. On the first successful registration it POSTs the envelope JSON to `DOME_RELAY_GROK_WEBHOOK_URL` with `Authorization: Bearer <DOME_RELAY_GROK_KEY>`, a bounded timeout, redirects disabled, and environment proxy variables ignored. HTTP 200 from Grok means a run started. It does not mean the work finished. Any other HTTP response means a run did not start. A read or write timeout after the request may have been sent is `uncertain`, and that job is not woken again. Fixed claim and complete URLs are preconfigured in the routine, not obeyed from incoming task text. No credential is embedded in `callback_url`.

The relay process reads these from Fly secrets. `DOME_RELAY_GROK_KEY` and `DOME_RELAY_GROK_WEBHOOK_URL` exist only there. The plugin's only secret is `DOME_RELAY_KEY`. The executive holds `DOME_RELAY_CALLBACK_KEY` and does not hold the Grok key:

- `DOME_RELAY_KEY` — plugin. Register, cancel, and read results. Cannot claim or complete.
- `DOME_RELAY_CALLBACK_KEY` — executive. Claim and complete. Cannot register or list results.
- `DOME_RELAY_GROK_KEY` — wake bearer. Sent only to the configured webhook.
- `DOME_RELAY_GROK_WEBHOOK_URL` — routine POST URL. HTTPS and a public host. Not a plugin setting.
- `DOME_RELAY_PUBLIC_ORIGIN` — this relay's public origin, with no path and no key.

`DOME_RELAY_KEY`, `DOME_RELAY_CALLBACK_KEY`, and `DOME_RELAY_GROK_KEY` must be three different values. Compare bearer keys in constant time. Secrets are headers, never JSON or URL parameters.

Endpoints:

- `POST /jobs`: plugin registers the envelope, `envelope_hash`, and `hop` (`0` or `1`). Authenticated with `DOME_RELAY_KEY`. The relay persists the job before the wake. Same `job_id` + same identity hash + same hop returns the existing registration and does not wake again, except the single pending-before-send recovery above. Same `job_id` with a different hash or hop returns 409. `hop` greater than 1, or any `parent_job_id`, returns 409 `hop_limit` and does not wake.
- `POST /jobs/{job_id}/claim`: executive claims before doing any work. Authenticated with `DOME_RELAY_CALLBACK_KEY`. No body. Return claimed only once. A duplicate claim, a canceled job, and an expired job are rejected. One accepted or held job per target; a busy slot is rejected. A crashed executive's claim is not automatically re-executed. The claim stands until `POST /jobs/{job_id}/cancel` or until the 30 minute expiry. Do not auto-reclaim.
- `POST /jobs/{job_id}/complete`: executive sends Result JSON. Authenticated with `DOME_RELAY_CALLBACK_KEY`. The body's `job_id` must match the path. There is no `POST /complete`. Check the job exists, it was claimed, and the deadline and state still allow completion. Identical duplicate completion returns 409 `duplicate_result` and cannot add another result. A different result after a terminal one returns 409 `conflict`. The first terminal result wins. `held` may be updated with a distinct body and later superseded by `done` or `failed`. A late (expired) callback is quarantined and does not resurrect the job. This endpoint never wakes Grok and cannot start another job.
- `POST /jobs/{job_id}/cancel`: plugin key. Stops a later claim or result and releases the target slot. Cancellation after a wake is local and best-effort. It cannot undo external work or stop a running Grok Bot routine. `done` and `failed` stay as they are.
- `GET /results?since=<opaque-cursor>&limit=100`: plugin key. `since` is a cursor this relay returned (`c` plus a decimal integer); omit it to read from the start. `limit` is 1 to 100 and defaults to 100. Returns ordered entries, each with a cursor and a Result, plus `next_cursor`. Non-destructive: fetching never deletes data or stores a consumer cursor. The plugin commits its own cursor only after saving the batch. One deployed relay key is one owner. Hermes profile isolation stays in the plugin database.
- `GET /healthz`: no key. Fly uses it. The handler also marks overdue jobs expired so a dead claim does not hold the slot forever.

`callback_url` must equal `{DOME_RELAY_PUBLIC_ORIGIN}/jobs/{job_id}/complete` exactly.

### D. Observation boundaries

These are separate observations. Do not treat one as proof of another, and do not treat a receiver's local status word as a relay state.

- **Registration.** The plugin worker calls `POST /jobs`. A 201 means the relay stored the job and attempted the wake. A 200 means the same job was already registered. This is not a claim and not a result.
- **Wake acceptance.** The relay's webhook call returned HTTP 200, and `wake_status` is `accepted`. A routine run started. It is not a result, and it is not a claim. After a successful wake, the relay job `state` stays `registered` while the job is still unclaimed, unless it has already advanced or terminated (`finish_wake()` leaves a state that already moved forward, for example if the executive claims before the wake request finishes). The plugin records wake acceptance as plugin `state=accepted` together with `wake_status=accepted`. That plugin `state` value is not evidence of a claim. Wake acceptance alone does not prove a claim.
- **Routine-local status.** Whatever the receiving routine reports about its own run is local to that routine. It is not a relay state and must not be recorded as one.
- **Executive claim.** `POST /jobs/{job_id}/claim` succeeds with the callback key. The relay then sets its job `state` to `accepted` and returns `"status": "claimed"`. Only a claimed job may store a result.
- **Relay result state.** `POST /jobs/{job_id}/complete` stores a `done`, `held`, or `failed` result on the relay. That stored record is the relay's result state. It is not a host notification and it does not resume the originating task or the executive routine.
- **Parent summary.** Any condensed handoff from a receiving routine to a parent or executor is a separate step. It is not the stored envelope and not the stored result.
- **Native notification.** A host or messaging notification about a finished process is distinct from the stored relay result and from `dome_relay_status`. The supported way to read a result is status retrieval in the origin conversation, or a direct pull of stored results.

## 4. JSON contracts

These are JSON Schema draft 2020-12 contracts. All timestamps are RFC3339 UTC on the wire; human UI displays HST. Validate timestamp parsing, HTTPS hosts and deadline ordering in code as well as schema.

### Envelope

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["job_id", "from", "target", "brief", "priority", "deadline", "callback_url", "artifacts", "created_at", "attempt"],
  "properties": {
    "job_id": {"type": "string", "format": "uuid"},
    "from": {"type": "string", "minLength": 1, "maxLength": 128},
    "target": {"type": "string", "pattern": "^[a-z0-9_-]{1,64}$"},
    "brief": {"type": "string", "minLength": 1, "maxLength": 16000},
    "priority": {"enum": ["normal", "high", "urgent"]},
    "deadline": {"type": "string", "format": "date-time"},
    "callback_url": {"type": "string", "format": "uri", "maxLength": 2048},
    "artifacts": {"type": "array", "maxItems": 10, "items": {"type": "object", "additionalProperties": false, "required": ["label", "url"], "properties": {"label": {"type": "string", "maxLength": 128}, "url": {"type": "string", "format": "uri", "maxLength": 2048}}}},
    "created_at": {"type": "string", "format": "date-time"},
    "attempt": {"type": "integer", "minimum": 1, "maximum": 3}
  }
}
```

Identity hash: lowercase hex SHA-256 of the canonical JSON of the envelope with `attempt` removed. Canonical JSON sorts object keys, uses separators `,` and `:` with no extra spaces, and is UTF-8 with non-ASCII left as characters (Python `json.dumps(..., sort_keys=True, separators=(",", ":"), ensure_ascii=False)`). Before hashing, the relay normalizes each timestamp to UTC `YYYY-MM-DDTHH:MM:SSZ` and requires `job_id` to be a lowercase UUID. `hop` is not inside the hash; it is compared on its own. Same `job_id` with the same hash and the same hop is the same registration even when `attempt` changes. Same `job_id` with a different hash or hop is a conflict. Artifacts are references, never automatically uploaded files or secrets; reject any query string or credential on those URLs and do not auto-fetch them. `callback_url` must equal `{DOME_RELAY_PUBLIC_ORIGIN}/jobs/{job_id}/complete`. `from` is an audit label; verified sender identity comes from authentication, not the JSON string.

### Result

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["job_id", "status", "result", "completed_at"],
  "properties": {
    "job_id": {"type": "string", "format": "uuid"},
    "status": {"enum": ["done", "held", "failed"]},
    "result": {"type": "string", "maxLength": 32000},
    "completed_at": {"type": "string", "format": "date-time"}
  }
}
```

`held` means an interim blocked update, not success; completed_at then means update time. A later done/failed result may supersede held. Exact held duplicates are rejected, distinct held updates receive new relay cursors. Done/failed are terminal. Local cancellation/expiry are local queue states, not extra wire Result values. Reject conflicting terminal results; quarantine late callbacks without resurrecting jobs.

The envelope `brief` limit (1 to 16,000) and the result `result` limit (at most 32,000) are service-validation limits. They are counted in characters (Python `len` on the Unicode string), not in serialized UTF-8 request bytes. They are the limits the plugin and the relay enforce on an accepted request. They are not guaranteed end-to-end delivery limits, and they do not establish how much of an accepted brief a receiving model will actually see.

## 5. Reliability and local hold

States: queued -> registered -> dispatching -> accepted -> done/failed. Side states: held_local, retry_wait, uncertain, canceled, expired. Keep delivery_pending/delivered separate from execution state. On the relay, `accepted` is the post-claim state; a successful wake leaves the relay job `registered` with `wake_status=accepted` while it is still unclaimed, unless the job has already advanced or terminated. The plugin records that wake as plugin `state=accepted`. Wake acceptance is not proof of a claim. See section 3D.

- Insert job_id under a UNIQUE constraint before side effects. Repeating identical escalate returns the existing handle; ID reuse with changed content is an error.
- The relay retries the Grok wake up to three total attempts. After a transient failure, wait 5 seconds, then 15 seconds, plus a random extra of up to 1 second. A `Retry-After` header replaces that base wait and is still not honored if the wait would pass the job deadline. Retry connect failures and HTTP 408, 429, 500, 502, 503, and 504 only. Never retry 401 or 403. Do not follow redirects. A read or write timeout is uncertain and is not retried. The plugin may retry its own registration and result pull against the relay with the same 3-attempt schedule, and must never turn that retry into a second wake.
- A repeated registration must not create a second routine run. The claim step denies duplicate work. After an accepted, rejected, or uncertain wake, do not wake again. Never report exactly-once arbitrary external effects.
- One outstanding accepted job per target. Reserve the slot transactionally on claim. Release it on a terminal result (`done` or `failed`), on cancel, or when the deadline expires. Held jobs retain the slot.
- One external escalation hop. A registration may use `hop` 0 or 1 and must not include `parent_job_id`. A higher hop or any parent is rejected and is not woken. `POST /jobs/{job_id}/complete` cannot dispatch another job.
- Local-only jobs require an explicit local readiness heartbeat/lease. Missing or stale heartbeat means held_local, not repeated wake-ups. If the entire rig is off, existing callbacks remain on the cloud relay; new local queue work resumes when the machine returns.
- Expiry is 30 minutes from server receive time. A client `deadline` may only shorten that window: it must be after `created_at`, in the future on the relay clock, and no later than 30 minutes after receipt. Register, claim, and complete all enforce it. If the plugin caller omits `deadline`, the plugin sets it to 20 minutes from now; that default is still subject to the 30-minute ceiling. For a transaction that may need more than 20 minutes, pass an explicit deadline, still within 30 minutes of relay receipt. `completed_at` is stored as a label and cannot extend the life of the job. A completion that arrives after expiry is quarantined and does not resurrect the job.
- Reconcile interrupted leases after restart. Fetch non-destructively from the last committed cursor; transactionally dedupe result records. Recover an in-flight wake (`dispatching` when the process died) as `uncertain` rather than treating it as unsent, and do not send it again. A row still `registered` with `wake_status` `pending` has not been sent; the next identical registration may send it once.
- A claim that outlives the executive is not reclaimed. It remains `accepted` until cancel or the 30 minute expiry. Do not start a second claim for that job automatically.
- Append audit metadata for transitions, attempts, accepted/uncertain/rejected wake, claims, duplicate rejection, auth failure counts and delivery. Do not put brief or result bodies, credentials, or full webhook URLs in logs or in the audit table. After a wake attempt finishes, delete the stored task text from the relay database. Results stay in SQLite on the volume until an operator deletes that database file. There is no purge tool in this version.

The demonstration can show one accepted claim and one terminal record under tested failures. Transport and host notification remain at-least-once; exactly-once execution of arbitrary assistant actions is out of scope.

## 6. Security and catalog disclosures

No secrets in envelopes, artifacts, callback URLs, exceptions, traces or audit logs. Store callback and consumer credentials separately with least privilege; a consumer key cannot submit results. Compare bearer keys in constant time, require TLS, disable redirects and validate configured hosts at startup and before sends. No caller-supplied network destinations; block loopback/private/link-local destinations in production and guard DNS resolution/rebinding. Explicit loopback exception only in local tests.

Remote briefs/results/artifacts are untrusted data. The saved executive routine defines allowed tasks and the fixed return destination. Payloads cannot change authority, run commands, install code, authorize spending or post/send externally. Missing approvals must hold/fail cleanly, not hang unattended or auto-approve. Escalation itself transmits task text and consumes executive usage: N must explicitly authorize the target and standing scope.

Before catalog submission, README and PR must disclose: outbound job text to the configured Grok Bot endpoint; result text to the chosen relay host; credential storage routes; local profile SQLite and relay durable storage; retention; the explicit worker and its polling interval; all third-party services and any billed usage; no telemetry, self-updater, shell execution or unrelated-file access in MVP. If any behavior changes, update disclosures. No secrets in the public repo. Expand .gitignore for *.db, *.db-wal, *.db-shm, runtime logs, caches and local exports during implementation.

Catalog file is a later, separately approved PR, not part of this spec task. It pins a full SHA, quoted version, truthful requires_hermes and exact capabilities. Do not copy the catalog capabilities mapping into the native manifest.

## 7. Test and demo plan

Unit tests (fake clocks/transports/temp databases): schema rejection; missing fields; deadlines; duplicate ID same/different content; registration/claim races; callback ownership; wrong/missing keys; host allowlist/redirect/SSRF cases; no secret leakage; retry bounds; concurrency slots; hop rule; canceled/expired and conflicting/late callbacks; held-to-terminal update; crash before/after cursor commit; profile isolation; stale local heartbeat and restart recovery.

Hermes integration tests: load root entrypoint, register exactly the three tools, JSON-string success/error returns, no import-time network, secret scope per profile, durable state location, original conversation routing and absent-session fallback. Run `hermes plugins doctor . --ci` and `hermes plugins validate . --install-deps` on the actual pinned Hermes release. Do not report these passed until executed.

`scripts/e2e.py` should use a fake executive by default, assert transitions and print redacted job IDs/status only. Live mode is explicit and must be approved because it wakes a real executive and spends usage. No secrets in command-line arguments.

Live demo: enqueue a harmless research job; executive webhook wakes; routine claims; executive uses its own allowed tools; completion returns; origin status shows it. Re-submit the same completion and show rejection. Take the local worker offline while another cloud result arrives; restart and show durable recovery with one accepted claim and one terminal record. Separately queue local-only work during stale heartbeat and demonstrate release after local readiness returns. Record under 90 seconds; do not claim the no-failure demo proves exactly-once arbitrary execution.

## 8. Work split (2–3 Cursor coding agents)

| Owner | Exclusive files | Deliverable |
|---|---|---|
| Agent A: plugin/local worker | root __init__.py, plugin.yaml, dome_relay/*, tests/plugin/* | Correct discovery, tools, scoped config/secrets, durable queue, adapters, reconciliation |
| Agent B: callback relay | relay/*, tests/relay/* | Durable registration/claim/completion/result API, authentication, hosting recipe |
| Agent C: contracts/integration/docs | schemas/*, scripts/e2e.py, tests/integration/*, README.md, deployment notes, .gitignore | Contract fixtures, acceptance evidence, safe demo/runbook |

The relay in section 3C is already implemented. The plugin must follow that contract and must not add its own Grok wake. Juni owns SPEC.md/interface decisions; Rhea coordinates branches, reviews integration and safety. With two agents, B also owns relay tests and A local tests; Rhea/Juni handle cross-system integration. Only one owner edits any shared file. Freeze contracts before parallel work; changes require coordinator review. This table does not authorize a launch, merge, deployment or contest submission by itself.

## 9. Open questions / gates

1. The pinned test baseline is Hermes v0.23.0 (`requires_hermes: ">=0.23.0"`). `hermes plugins doctor <plugin-dir> --ci` and `hermes plugins validate <plugin-dir>` on that release are still an operator check. This repo's pytest suite does not run them, and they must not be reported as passed until someone executes them.
2. The implemented host recipe is one always-on Fly machine with a SQLite volume (`relay/fly.toml`: `auto_stop_machines = "off"`, `min_machines_running = 1`). Who owns the Fly account, and who accepts the idle-machine cost, is still open. Relay retention itself is question 9.
3. Can the actual Grok Bot routine call claim and complete through a sanctioned authenticated HTTP/MCP interface? Prove this before feature expansion.
4. What exact standing tasks/targets does N authorize, including executive usage ceilings and escalation priority policy?
5. **Open: per-target callback keys.** The built relay has one `DOME_RELAY_CALLBACK_KEY` for every target. It is different from `DOME_RELAY_KEY` and from `DOME_RELAY_GROK_KEY`. A separate key per target, and who rotates keys, are not decided.
6. **Partly bounded.** Status retrieval in the origin conversation is the supported return path. v0.23.0 does not pass a compression-stable `conversation_id` (section 3A). Optional `inject_message` stays off and is not a supported push path. A stored result does not resume the originating task or apply the result as code.
7. **Resolved.** One outstanding accepted or held job per target, three wake attempts (5s then 15s plus up to 1s of jitter), one hop (`hop` 0 or 1, no parent), and expiry 30 minutes from server receive time. The local-worker heartbeat window is 60 seconds (`local_heartbeat_seconds`) unless that setting is changed. The spec left the number open; the plugin chose 60.
8. **Resolved.** Do not auto-reclaim a crashed executive claim. The claim stands until `POST /jobs/{job_id}/cancel` or the 30 minute expiry.
9. **Relay retention is resolved. A purge tool is still open.** The relay deletes stored task text when the wake attempt finishes. Results remain in the volume SQLite file until an operator deletes that file. There is no purge or deletion API. Who may inspect stored results, and what retention applies to the plugin's own database, are still open.
10. Publication approval: confirm exact main-branch spec/manifest edits and use of a public repository; no upstream PR or demo post is included here.

Known open items, called out so they are not treated as decided:

- Per-target callback keys. Today every target shares one `DOME_RELAY_CALLBACK_KEY`.
- A purge tool. Nothing in the relay deletes stored results except deleting the SQLite file by hand.

At this revision: earlier acknowledgement and short-text round trips completed without a human ferrying the result; what is not demonstrated is unattended completion of a dense, multi-step task end to end without owner intervention. Also not demonstrated or not established: live worker-restart integrity of an in-flight job; end-to-end capture of the wake request body; how much of an accepted brief a receiving model sees.

## 10. Receiving-side runbook requirements (not implemented in this repository)

The two rules in this section apply to receivers and their runbooks. They are not implemented by the plugin or the relay, and updating this document does not change receiving-side behavior.

### Execution handoff

An execution handoff — from a receiving routine or parent to the process that will claim and work the job — must preserve:

- the actual received task or brief, including its constraints, verbatim or through an accessible artifact; identify missing portions rather than reconstructing them
- task identity (`job_id` and target)
- exact references from the envelope (artifact URLs and other literal references)
- the deadline
- provenance (what was received, and from where)
- completeness flags (whether the received brief or envelope is complete, and character or byte length if known)

This is a requirement on receivers and runbooks, not a feature of this repository.

### Continuation exception

If callback information is missing from what a receiver can see, the default is to stop and seek owner direction or recovery of the existing delivery. Any resend is a separately authorized recovery decision, not a default and not a capability the relay currently supplies. Deriving the destination from an already-approved host plus the job id, and proceeding, is a run-only continuation exception. It requires the owner's explicit instruction for that run. It is not the default response to a malformed or incomplete job, and it is not implemented as automatic plugin or relay behavior.

## Sources (read November 7, 2026)

- [Hermes plugin developer guide](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins): native manifest/layout, register_tool, config_schema, get_secret and plugin_db APIs.
- [Catalog README and admission rules](https://github.com/NousResearch/hermes-agent/blob/main/plugin-catalog/README.md): exact pins, public surfaces, approvals, credential scope and disclosures.
- [Catalog submission guide](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission).
- [Grok Bot routines](https://cursor.com/help/grok-bot/routines): authenticated webhook JSON and 200 acceptance semantics.
