# Dome Relay technical specification

**Status:** design draft, not implemented or validated. Author: Juni, interim CTO.
**Target:** one real round trip before the November 11, 2026 noon HST contest deadline.

## 1. Goal and scope

Keep established assistants in their own workspaces. A local Hermes agent asks Celeste to escalate a job; a webhook wakes the selected Grok Bot executive; the executive uses its existing tools; an authenticated callback returns its result to the original conversation.

The MVP must also reject duplicates, hold local-only work while the worker is offline, and recover jobs and received results after process restart. Non-goals: Lyra's host adapter, email and Telegram (after the contest), arbitrary remote code execution, a new agent runtime, and moving identities into Hermes.

A real external executive and sanctioned callback mechanism must be proven. A mock executive is useful for tests but is not the live-demo acceptance criterion.

## 2. Current repository and exact Hermes integration contract

Read before this draft: README.md, plugin.yaml, dome_relay/__init__.py and .gitignore on main. Current code only builds a partial brief; send_brief raises NotImplementedError. The root manifest explicitly says its format is a placeholder. No runnable plugin is claimed.

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
  adapters/grok_webhook.py
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

The manifest and loadable root `__init__.py` must be adjacent. `entrypoint: dome_relay` is not the documented native loader mechanism; remove it. Replace placeholder `config:` with `config_schema:`. A manifest-only correction does NOT repair the currently absent root entrypoint.

In root `register(ctx)`, register exactly `dome_relay_escalate`, `dome_relay_status`, `dome_relay_cancel` using `ctx.register_tool(name=..., toolset="dome_relay", schema=..., handler=...)`. Each schema has name, description and a JSON Schema parameters object. Handlers accept `(args: dict, **kwargs) -> str`, catch errors and return JSON strings, including errors. Do not shadow existing tools. `register()` must perform no network calls or launch workers; doctor imports it in isolation.

- escalate: target, brief, priority, deadline, artifacts, optional caller-supplied job_id and execution_location cloud/local. Persist before any send; return job_id and state immediately. Derive sender and callback URL from trusted settings/context, never arbitrary model arguments.
- status: job_id; query the originating profile/conversation's job and latest result, never another user's job. This is the minimum durable return-to-origin mechanism.
- cancel: job_id; stop retries and future result injection. Cancellation after acceptance is local/best-effort; it cannot undo external work or reliably interrupt a running Grok Bot routine.

Manifest v2 uses top-level `provides_tools`, `provides_hooks`, `config_schema`, `requires_env`, `python_dependencies` and a LIST of privileged `capabilities`. Do not confuse this with a future catalog YAML's `capabilities` MAPPING, which summarizes actual tools/hooks/middleware/env names. Declare only implemented registrations. A design commit should retain `provides_tools: []` until tools actually register.

Use `ctx.get_config("relay_url", default="")` for nonsecret plugin settings, resolved under `plugins.entries.dome-relay.settings`; `ctx.set_config` changes only that namespace. Read declared secrets through `from agent.secret_scope import get_secret`, then `get_secret("DOME_RELAY_GROK_KEY")` (and the other declared names). Never read `.env`, auth.json, browser profiles or a bare process-wide environment for Hermes secrets. Resolve the active profile's values before passing narrowly scoped credentials to a worker; do not reuse one global client across profiles.

Use `plugins.plugin_storage.plugin_db("dome-relay")` for profile-scoped durable SQLite storage and `plugin_data_dir` where files are necessary. Never put runtime databases in the plugin install tree. The worker is an explicitly started plugin-owned process/CLI command, with clean shutdown, bounded network timeouts and a single-worker lease. Keep root registration cheap. Final implementation must use public lifecycle APIs and must declare any added hooks/commands honestly.

Do not invent a minimum Hermes version: pin a released test version and derive the truthful SemVer floor from tests. Current guide has contradictory prose about api_version/global API numbering; omit optional api_version and rely on documented behaviors plus validation.

## 3. Components and flow

### A. Plugin and worker

SQLite tables: jobs, attempts, audit_events, result_inbox, consumer_cursors and delivery_outbox. Jobs retain profile, original conversation_id, session-routing metadata, immutable envelope hash, local execution requirement, timestamps, lease and terminal state. Use conversation_id rather than session_id alone because compression may rotate transcript segments.

The worker runs network I/O without model-token polling. A configured interval checks pending work and pulls relay results; default 15 seconds while active, with backoff when idle or unreachable. This is process polling, not a recurring executive routine. Persist cursor and inbox entries atomically before applying results.

The MVP returns results through status retrieval in the origin conversation until reinjection is proven on the pinned Hermes build. `ctx.inject_message` is UNVERIFIED in the Hermes plugin guide; do not build against it as an established API. The documented surface is `pre_llm_call` hook injection, which is ephemeral and applies to the current turn only, not persisted delivery to the origin session. Any future automatic reinjection must first prove correct active-profile context and session recovery on the pinned build; if the origin session no longer exists, retain the result for status retrieval instead of injecting into a random new session. Any proven automatic delivery remains at-least-once with a stable result ID, not guaranteed exactly once across a crash at the host boundary.

### B. Generic target adapter

Interface (conceptual, not Hermes API): `validate_config`, `dispatch(envelope) -> accepted | rejected | uncertain`, and optional `reconcile(job_id)`. Do not assume all targets support cancel or idempotent execution.

GrokWebhookAdapter POSTs the envelope JSON to a configured HTTPS routine URL with `Authorization: Bearer <get_secret("DOME_RELAY_GROK_KEY")>`, timeouts and redirects disabled. A 200 acknowledges that a routine run started; it is NOT a result. A transport timeout after sending is uncertain, not proof the job failed. Never blindly re-wake an uncertain/accepted job.

### C. Always-on callback relay

FastAPI or equivalent. Free-tier deployment is an option, not a guarantee: verify persistence, quotas, sleep behavior, TLS and costs. SQLite requires a durable single-instance volume; an ephemeral filesystem cannot satisfy restart recovery. If the host cannot provide that, use its durable database service.

Additional registration endpoint is necessary to authenticate results for known jobs:

- `POST /jobs`: plugin registers immutable job_id, target, deadline and envelope hash before wake-up. Authenticated with an owner/plugin key. Same ID + same body returns existing registration; different body returns 409.
- `POST /complete`: executive sends Result JSON, authenticated with a separately provisioned per-target callback bearer key. Check job exists, target owns it, deadline/state permits completion, body size and schema. Secrets are headers, never JSON or URL parameters. Identical duplicate completion returns 409 duplicate_result and cannot add another result; conflicting completion returns 409 conflict. First terminal result wins.
- `GET /results?since=<opaque-cursor>&limit=100`: authenticated plugin consumer, scoped to its owner/profile. Returns ordered entries, each with a sequence cursor and Result, plus next_cursor. Non-destructive: fetching never deletes data or globally advances a cursor. The plugin commits its own cursor only after saving the batch.
- `POST /jobs/{job_id}/claim`: authenticated executive routine atomically claims before doing any work. Return claimed only once; duplicate or canceled/expired jobs are rejected. This closes the duplicate-wake gap. A crashed executive's claim is NOT automatically re-executed; operator reconciliation is required for uncertain side effects.

The relay does not need the Grok Bot sender key. The plugin owns outbound wake authentication; the executive host securely owns its callback/claim key. Fixed endpoints are preconfigured in the routine, not obeyed from incoming task text. No credential is embedded in callback_url.

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

Immutable identity hash excludes attempt. Artifacts are references, never automatically uploaded files or secrets; reject signed/token-bearing URLs and do not auto-fetch arbitrary links. The relay matches callback_url to its exact configured HTTPS origin and /complete path. `from` is an audit label; verified sender identity comes from authentication, not the JSON string.

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

## 5. Reliability and local hold

States: queued -> registered -> dispatching -> accepted -> done/failed. Side states: held_local, retry_wait, uncertain, canceled, expired. Keep delivery_pending/delivered separate from execution state.

- Insert job_id under a UNIQUE constraint before side effects. Repeating identical escalate returns the existing handle; ID reuse with changed content is an error.
- Retry safe registration/pull/complete requests up to three total attempts with delays 5 and 15 seconds plus jitter; honor Retry-After within the deadline. Retry only eligible transient failures. Never retry 401/403 until configuration is fixed.
- A webhook retry may create a second routine run. The mandatory claim step must deny duplicate work. After an accepted or ambiguous wake, reconcile claim/results; do not blindly dispatch again. Never report exactly-once arbitrary external effects.
- Default concurrency one outstanding accepted job per target. Reserve the slot transactionally; release on terminal result, explicit operator resolution or deadline. Held jobs retain their slot.
- Default maximum one external escalation hop: return results cannot themselves dispatch. Store parent lineage and hop count locally. An executive cannot create a forwarding chain through /complete.
- Local-only jobs require an explicit local readiness heartbeat/lease. Missing or stale heartbeat means held_local, not repeated wake-ups. If the entire rig is off, existing callbacks remain on the cloud relay; new local queue work resumes when the machine returns.
- Expire before each send and claim. Use server receive time to validate result timeliness; never trust a sender-supplied completed_at to bypass expiry.
- Reconcile interrupted leases after restart. Fetch non-destructively from the last committed cursor; transactionally dedupe result records. Recover an in-flight wake as uncertain rather than treating it as unsent.
- Append audit metadata for transitions, attempts, accepted/uncertain wake, claims, duplicate rejection, auth failure counts and delivery. Do not put brief/result bodies, credentials or full webhook URLs in logs.

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

Juni owns SPEC.md/interface decisions; Rhea coordinates branches, reviews integration and safety. With two agents, B also owns relay tests and A local tests; Rhea/Juni handle cross-system integration. Only one owner edits any shared file. Freeze contracts before parallel work; changes require coordinator review. This table does not authorize a launch, merge, deployment or contest submission by itself.

## 9. Open questions / gates

1. Which released Hermes version is the test baseline, and what is the minimum supported SemVer after validation?
2. Which relay host offers durable storage without surprise billing or sleep-induced loss? Who owns the account and retention policy?
3. Can the actual Grok Bot routine call claim and complete through a sanctioned authenticated HTTP/MCP interface? Prove this before feature expansion.
4. What exact standing tasks/targets does N authorize, including executive usage ceilings and escalation priority policy?
5. How will each executive's callback credential be provisioned securely, distinct from the webhook sender key? Who rotates keys?
6. The MVP uses status retrieval in the origin conversation. Can automatic reinjection be proven on the pinned Hermes build after session rotation/restart? `ctx.inject_message` is UNVERIFIED; documented `pre_llm_call` injection is ephemeral and current-turn only, not persisted origin-session delivery.
7. Is default one concurrent job/target, three attempts and a one-hop limit acceptable? What deadline and heartbeat thresholds suit the demo?
8. Who manually reconciles uncertain executive claims after a crash? Do not auto-reclaim arbitrary work.
9. Who may inspect stored briefs/results and what deletion/retention policy applies to both stores?
10. Publication approval: confirm exact main-branch spec/manifest edits and use of a public repository; no upstream PR or demo post is included here.

## Sources (read November 7, 2026)

- [Hermes plugin developer guide](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins): native manifest/layout, register_tool, config_schema, get_secret and plugin_db APIs.
- [Catalog README and admission rules](https://github.com/NousResearch/hermes-agent/blob/main/plugin-catalog/README.md): exact pins, public surfaces, approvals, credential scope and disclosures.
- [Catalog submission guide](https://hermes-agent.nousresearch.com/docs/developer-guide/plugins/catalog-submission).
- [Grok Bot routines](https://cursor.com/help/grok-bot/routines): authenticated webhook JSON and 200 acceptance semantics.
