# hermes-dome-relay

A [Hermes Agent](https://github.com/NousResearch) plugin from **Team Astrum** that passes structured escalation briefs between agents, so a cheap always-on dispatcher can hand real work to the right specialist and get results back.

> Status: the Hermes plugin and the relay both run. Target Hermes release: **v0.23.0** (`requires_hermes: ">=0.23.0"`). Hackathon deadline: Wed Nov 11, 2026, 12:00 PM HST.

## What it does

- **Escalate**: the Hermes agent saves a job, and a worker registers it with the relay (`POST /jobs`). The relay, not this plugin, wakes the Grok Bot executive.
- **Status**: the same conversation asks for the job and gets the latest result (`GET /results` on the relay, saved in the profile database).
- **Cancel**: stop retries and ask the relay to cancel. A cancel cannot undo a Grok run that already started.
- **Hold local work**: a job tagged `local` waits until this machine's worker has checked in recently.

A job is task text and optional reference links. It does not grant authority to post, send messages, spend money, or touch credentials. The executive only does what its own saved routine already allows.

## Repo layout

| File | What it is |
| --- | --- |
| `plugin.yaml` | The plugin's name, version and settings. Hermes reads this. |
| `__init__.py` | Hermes loads `register(ctx)` from this file. |
| `dome_relay/` | Tools, the worker, and the relay client. |
| `relay/` | The always-on callback service (the part that runs on Fly). |
| `SPEC.md` | The design contract. |
| `LICENSE` | MIT: anyone can use or change this, with credit. |

## New to repos? Quick guide

1. **Get a copy:** `git clone https://github.com/nerdcakes/hermes-dome-relay.git`
2. **See what changed:** `git status`
3. **Save your changes:** `git add .` then `git commit -m "what you did"`
4. **Upload them:** `git push`

## Security

Never commit URLs, keys, or tokens.

The plugin's only secret is `DOME_RELAY_KEY`. It reads that name through `agent.secret_scope.get_secret`. It does not open a dotenv file, `auth.json`, or the process environment itself. Non-secret settings (`relay_url`, `sender`, and the rest) live under `plugins.entries.dome-relay.settings`.

The relay process in `relay/` is different. It runs on Fly, and Fly hands it secrets as environment variables. Those values stay out of this repo. The plugin must not wake Grok and must not hold the Grok webhook URL or key. See [The relay](#the-relay) below.

The contract is in [SPEC.md](SPEC.md).

## Use the plugin

You need [Hermes Agent](https://hermes-agent.nousresearch.com/docs/getting-started/installation) **v0.23.0** or newer. These steps use placeholders. Put your own relay origin and key in Hermes, not in this repo.

### 1. Install and enable

Hermes loads plugins from the active profile's `plugins` folder, `$HERMES_HOME/plugins`. On a default local install that is `~/.hermes/plugins`. Hosted or custom setups may point `HERMES_HOME` elsewhere (for example `/opt/data`), so check with `hermes config path` (the config file sits in `HERMES_HOME`).

The install path exercised for this revision is a **profile-scoped, pinned native install** from the public git repository into the active Hermes profile. It is not a catalog install. A catalog entry is a separate, later change and is not required for this method. This document does not imply that a catalog entry has been admitted.

Pin a full 40-character commit SHA of the revision you intend to run. Tags, branches, and abbreviated SHAs are not accepted. Hermes checks out that commit detached, verifies that `HEAD` matches the requested SHA, and records the source, revision, and pin in the current profile (`hermes plugins list` shows it as `git pinned@<sha>`). The same pin is available in Hermes Desktop under Skills → Plugins → Install from Git, in the Pin to commit field.

```bash
hermes plugins install nerdcakes/hermes-dome-relay --ref <full-40-character-sha>
hermes plugins enable dome-relay
```

`hermes plugins update` will not move a pinned install. To change revision, install again with `--force --ref` and a new full SHA.

**Manual install.** You can also clone this repository and symlink it into the profile's `plugins` folder. Clone somewhere permanent (not a temp folder; the plugin is a symlink to the clone), then run these from inside the clone, because `$(pwd)` is the link target:

```bash
git clone https://github.com/nerdcakes/hermes-dome-relay
cd hermes-dome-relay
PLUGIN_DIR="${HERMES_HOME:-$HOME/.hermes}/plugins"
mkdir -p "$PLUGIN_DIR" && ln -s "$(pwd)" "$PLUGIN_DIR/dome-relay"
hermes plugins enable dome-relay
```

That block is Bash. In fish, from the clone: `set -l plugin_dir "$HERMES_HOME/plugins"; mkdir -p $plugin_dir; ln -s (pwd) $plugin_dir/dome-relay`. In PowerShell: `New-Item -ItemType SymbolicLink -Path "$env:HERMES_HOME\plugins\dome-relay" -Target (Get-Location)`. If `HERMES_HOME` is unset, use the default profile directory `~/.hermes` (`$HOME/.hermes` in fish). This is only another way to write the symlink. It does not add Windows support.

Hermes reads `plugin.yaml` and the `__init__.py` beside it. For either install method, the plugin directory is the profile plugins folder. Check the wiring:

```bash
PLUGIN_DIR="${HERMES_HOME:-$HOME/.hermes}/plugins"
hermes plugins doctor "$PLUGIN_DIR/dome-relay" --ci
hermes plugins validate "$PLUGIN_DIR/dome-relay"
```

`hermes plugins validate` requires that plugin-directory path. Without it, Hermes reports a missing argument and does not check the plugin.

### 2. Point it at your relay

`relay_url` is the relay's public origin: `https`, no path, and no key in the URL. It must be the same origin the relay was given as its public origin, because every job's callback is `{relay_url}/jobs/{job_id}/complete`.

```bash
hermes config set plugins.entries.dome-relay.settings.relay_url https://relay.example
hermes config set plugins.entries.dome-relay.settings.sender celeste
```

`sender` is a label in the task JSON. It is not a secret. Leave `hop` at `1` (only `0` or `1` is allowed).

### 3. Set the one secret

The plugin needs `DOME_RELAY_KEY`. That key can register a job, cancel one, and read results. It cannot claim or complete a job, and it is not the Grok wake key.

In the Hermes Desktop app, open the dome-relay plugin settings and fill in **Relay key**. Hermes stores it for you. From a terminal, add `DOME_RELAY_KEY` to the secret file Hermes already loads for the active profile, then start Hermes again. Do not paste the value into `plugin.yaml`, a chat, or this repo.

### 4. A short demo

Two terminals. No real job text that posts, pays, or reads keys.

```bash
hermes dome-relay worker
```

Leave that running. In a Hermes chat, ask the agent to escalate a harmless research question:

- tool: `dome_relay_escalate`
- target: `research`
- brief: a short public question
- priority: `normal`

The tool returns a `job_id` and `state` right away (`queued` locally, or `held_local` if the job is tagged `local` and this machine's worker heartbeat is stale). The worker then registers the job with the relay. Then ask for status:

- tool: `dome_relay_status`
- `job_id`: the id from the first call

Watch these observations separately. They are not all distinct values of the plugin's `state` field, and the same word `accepted` does not mean the same thing on the plugin and on the relay:

1. **Queued locally** — the plugin has saved the job. No relay call has been made yet.
2. **Registered** — the worker has registered the job (`POST /jobs`) and the relay has stored it.
3. **Wake accepted** — the relay's `wake_status` is `accepted` (the webhook returned HTTP 200: a routine run started, not that the work finished). After a successful wake the relay job `state` stays `registered`. The plugin records this as `state=accepted` together with `wake_status=accepted`. **Wake acceptance alone does not prove a claim.**
4. **Executive claimed** — `POST /jobs/{job_id}/claim` succeeded. The relay then sets its job `state` to `accepted` and returns `"status": "claimed"`. The plugin status tool does not receive a separate claim event, so a plugin `state` of `accepted` is not evidence that the executive claimed the job.
5. **Result stored** — the executive posted a result, and the relay has saved it (`done`, `failed`, or `held`).
6. **Result retrieved** — the worker or a status call has pulled that result (`GET /results`) into this profile, and `dome_relay_status` can show it.

A finished relay handoff (wake accepted, or a callback stored on the relay) is not the implementation finishing: a long coding job can still be underway, and `done` is the result text, not a merge. Asking again with the same `job_id` and the same brief returns the same job (`deduped: true`) and does not start a second run. If you omit `deadline` when the job is first created, the plugin sets it to 20 minutes from now. For a transaction that may need more than 20 minutes, pass an explicit deadline. The relay still caps every job at 30 minutes from the moment it received it, and a job's own deadline can only shorten that window. Omitting `deadline` on a repeat escalate is still the same job; the plugin keeps the deadline it saved the first time.

Stop the worker with Ctrl-C. A result that arrives while it is stopped stays on the relay. Start the worker again and status shows it. The profile database is outside the plugin folder, so a plugin update does not delete it.

Loading or enabling the plugin, and the plugin's `register()` function, only record the tools, hooks, and the `hermes dome-relay` command. They do not start a worker. The worker is process polling: it performs network I/O and does not consume model tokens. Asking the Hermes agent for status is a conversation turn and does involve model and tool activity. A saved result, and the job's `callback_url`, do not resume the originating Hermes task and do not resume the executive routine. Completing a job only stores the result. The supported way to read it is `dome_relay_status` in the conversation that sent the job.

`execution_location: local` waits until this machine's worker heartbeat is fresh (60 seconds, unless you change `local_heartbeat_seconds`). The spec left that window open; 60 seconds is this plugin's choice. `hermes dome-relay ready` refreshes the heartbeat without a poll.

The worker command exists only when this plugin loads in-process. That is the Hermes default (`plugins.isolation: in_process`) and the supported deployment. `plugins.isolation: host` skips `register_cli_command`, and that registration is the only worker entrypoint, so `hermes dome-relay` is not installed. Leave Hermes terminal and Docker sandbox settings as they are; this is about where the plugin process loads, not about command sandboxing.

Status reads the job that belongs to this conversation. Hermes v0.23.0 passes the physical `session_id` and does not pass a conversation id that stays put when rotating compression (`compression.in_place: false`) replaces that id. The public stable `conversation_id` was added to Hermes after v0.23.0, so this plugin does not call Hermes session internals to walk the lineage. It records an edge only from the `post_compaction` hook, when Hermes reports a new session id and a different previous id. A later status, cancel, or repeat escalate from that replacement session can see the same job. A different session in the same profile cannot. In-place compression (the Hermes default) keeps the session id, so no edge is required. If the hook did not run, a call that only has the new session id is `not_found`.

`expired` means this profile stopped waiting, and the job no longer reserves its target. A terminal relay result (`done` or `failed`) that was accepted before the deadline still replaces that state when it is retrieved late. A `held` result stays in the status body so it can be read, and after the deadline the job state is `expired` so the next job for that target can be sent. A canceled job still does not return a result.

### Service-validation limits and current limitations

The plugin and the relay accept briefs of 1 to 16,000 characters and results of at most 32,000 characters. Those are service-validation limits, counted in characters (not serialized UTF-8 request bytes). They are not guaranteed end-to-end delivery limits, and they do not establish how much of an accepted brief a receiving model will actually see.

At this revision: unattended completion has not been demonstrated; live worker-restart integrity has not been tested; the wake request body has not been captured end to end; and how much of an accepted brief a receiving model sees is not established.

### What you can turn on later

`dome_relay_status` is the supported way to read a result. Two optional switches exist and stay off unless you turn them on. They are not a supported push path. On a gateway, injection wants a routing key, and this plugin does not substitute the transcript session id or promise that the message was delivered.

- `inject_context` (default false): before a turn, `pre_llm_call` can add newly finished results for **this** conversation to that turn. It is not a saved message, and it is not sent to some other chat.
- `inject_message` (default false): the plugin may call `ctx.inject_message` only when Hermes already passed a gateway `session_key`. On a gateway that also needs `plugins.entries.dome-relay.allow_gateway_injection: true`, which is a host grant, not a plugin setting. If Hermes returns false, the plugin does not retry that note, and status still works. Leave this off.

```bash
hermes config set plugins.entries.dome-relay.settings.inject_context false
hermes config set plugins.entries.dome-relay.settings.inject_message false
```

## What a job is not allowed to do

The model can set the target, the brief, the priority, an optional deadline, optional https links, an optional job id, and `cloud` or `local`. It cannot set the sender, the callback URL, the hop, or any credential.

Links are references. The plugin does not fetch them, and it rejects a query string or a password in the URL. Nothing in the job JSON authorizes posting, sending a message, spending money, or reading credentials. If the executive is not allowed to do the task, the result should be `held` or `failed`, not a silent approval.

A completed job is a result, not an approval. If the result is code — a branch or a pull request — merging it stays a separate human decision, made outside the relay. The relay tracks the callback; it does not grant the yes.

## Disclosures

- Job text is sent to the relay you configure. The relay, not this plugin, forwards that text to the Grok Bot webhook. A successful wake spends Grok Bot usage.
- Result text is stored on the relay and copied into this profile's plugin database when the worker or a status call reads it.
- The only plugin credential is `DOME_RELAY_KEY`, via Hermes secret scope. The Grok key and the executive callback key stay on the relay and the executive.
- Local state is a SQLite file under the active Hermes profile (`plugin-data/dome-relay`). The relay keeps its own SQLite file on the Fly volume. This version has no purge tool. Delete those files yourself if you want the text gone. The relay already deletes task text after the wake attempt; results stay until the database file is deleted.
- The worker is explicit: `hermes dome-relay worker`. It polls every 15 seconds while a job is open, and waits longer when idle or the relay cannot be reached. Registration retries are 3 attempts, 5s then 15s plus up to 1s of jitter, honoring `Retry-After`. 401 and 403 are not retried.
- Third-party services: your relay host, and Grok Bot when the relay wakes a routine. An always-on Fly machine can cost money while idle. See [The relay](#the-relay).
- No telemetry, no self-updater, no shell, and no reading of unrelated files.

If any of that changes, this section has to change with it.

A catalog entry is a separate, later change. It is not part of this plugin.

## The relay

This is the always-on box between the plugin and the Grok Bot executive.

1. The plugin registers a job (`POST /jobs`) with the relay key.
2. The relay saves the job, then POSTs the task text to the Grok routine webhook. **HTTP 200 means a run started (`wake_status: accepted`). It does not mean the work is finished, and it does not mean the executive has claimed the job.** After a successful wake the relay job `state` stays `registered`.
3. The executive claims the job once (`POST /jobs/{job_id}/claim`) with a different key. A successful claim is when the relay sets its job `state` to `accepted` and returns `"status": "claimed"`. A second claim, a canceled job, or an expired job is refused.
4. The executive posts the result (`POST /jobs/{job_id}/complete`).
5. The plugin reads results (`GET /results?since=...`). Reading does not delete them. The plugin keeps its own cursor.

A job lives at most **30 minutes** from the moment the relay received it, and sooner if the job's own deadline is sooner. The relay's clock decides. The time in the result body cannot extend that. If the plugin caller omits `deadline`, the plugin sets it to 20 minutes from now; that default is still subject to the 30-minute ceiling.

Other locked rules: one accepted job at a time per target; one hop (a result cannot start another job); wake retries are 3 tries, waiting 5s then 15s plus a little jitter, only for temporary failures. A `Retry-After` header replaces the wait. **401 and 403 are never retried.** A timeout after the request may have been sent is treated as uncertain and is not sent again.

`POST /jobs/{job_id}/cancel` (relay key) stops a later claim or result. It cannot recall a Grok run that already started. If a claim succeeds and the executive then crashes, the job stays claimed until someone cancels it or the 30 minutes run out. The relay will not claim it again by itself.

### Keys (Fly secrets)

| Secret | Who uses it |
| --- | --- |
| `DOME_RELAY_KEY` | Plugin. Register, cancel, read results. |
| `DOME_RELAY_CALLBACK_KEY` | Executive. Claim and complete. Must be different from the relay key. |
| `DOME_RELAY_GROK_KEY` | Sent only as `Authorization: Bearer` to the routine webhook. |
| `DOME_RELAY_GROK_WEBHOOK_URL` | The routine's POST URL. HTTPS, public host. |
| `DOME_RELAY_PUBLIC_ORIGIN` | This relay's public origin, such as `https://<your-app-name>.fly.dev`. No path and no key. |

`DOME_RELAY_ALLOW_LOOPBACK` is for local tests only. Do not set it on Fly.

The database file path is `DOME_RELAY_DB` (already set in `relay/fly.toml` to `/data/dome-relay.db`). That one is not a secret.

This deploy is one owner. The plugin keeps Hermes profiles apart in its own database. A separate callback key per target, and a tool that deletes stored results, are still open.

### Run the tests

From the repo root, with Python 3.11 or newer:

```bash
python3 -m venv .venv
.venv/bin/pip install -r relay/requirements.txt -r relay/requirements-dev.txt
.venv/bin/python -m pytest
```

### Deploy (do this yourself; nothing here deploys for you)

Install the [Fly CLI](https://fly.io/docs/flyctl/install/), then `fly auth login`. Edit `app` and `primary_region` in `relay/fly.toml` if you need a different app name or city. The volume must use that same region. Keep the app at one machine: the SQLite file cannot be shared.

```bash
cd relay

fly apps create dome-relay

fly volumes create dome_relay_data \
  --region sjc \
  --size 1 \
  --yes

fly secrets set \
  DOME_RELAY_KEY="<long random plugin key>" \
  DOME_RELAY_CALLBACK_KEY="<different long random executive key>" \
  DOME_RELAY_GROK_KEY="<the routine webhook key>" \
  DOME_RELAY_GROK_WEBHOOK_URL="<the routine POST URL>" \
  DOME_RELAY_PUBLIC_ORIGIN="https://<your-app-name>.fly.dev"

fly deploy
fly scale count 1
fly status
```

Replace every `<...>` value in your own terminal. Do not put those values in the repo, in `fly.toml`, or in a chat log. `DOME_RELAY_PUBLIC_ORIGIN` must be the origin Fly actually serves, with no path at the end. The job's `callback_url` must be exactly that origin plus `/jobs/<job_id>/complete`.

Check it with `curl https://<your-app-name>.fly.dev/healthz`. You want `{"ok":true}`.

An always-on machine can cost money even when idle. Check Fly's current price for a `shared-cpu-1x` machine with 256 MB before you leave it running. A successful wake also spends Grok Bot usage, because a 200 starts a real routine run.

### What is stored

- Task text is forwarded to the Grok webhook, then removed from the relay database.
- Results stay in SQLite on the Fly volume until someone deletes that database. There is no automatic purge in this version.
- The audit table records event names and job ids. It does not record task text, result text, keys, or the webhook URL.

### What this service does not do

No telemetry, no self-updater, no shell, and no reading of unrelated files. It does not fetch artifact links. It does not follow webhook redirects. It does not send the Grok key anywhere except the configured webhook.

### Contract

Registration, claim, complete, the identity hash, expiry, hop, and retention are specified in [SPEC.md](SPEC.md).

### Fake round trip

`scripts/e2e.py` runs one job through the plugin and the relay with a fake executive. It prints a short job id and a status word. It does not take a key as an argument.

```bash
python3 scripts/e2e.py
```

`--live` is refused unless you also pass `--i-understand-this-wakes-a-real-executive`, because a live run wakes a real executive and spends usage. Even then, this script does not read a key from the command line.

## License

MIT, see [LICENSE](LICENSE).
