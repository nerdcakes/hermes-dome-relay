# hermes-dome-relay

A [Hermes Agent](https://github.com/NousResearch) plugin from **Team Astrum** that passes structured escalation briefs between agents, so a cheap always-on dispatcher can hand real work to the right specialist and get results back.

> Status: early scaffold for the Nous hackathon (deadline Wed Nov 11, 2026, 12:00 PM HST). Nothing here runs yet.

## What it will do

- **Send a brief**: package a task as JSON (`job_id`, `from`, `brief`, `priority`, optional `callback_url`) and POST it to another agent's inbox.
- **Receive a result**: accept the reply on a callback URL and hand it back to the original agent.
- **Hold local work**: tasks tagged `local` wait in a queue until the home rig is online instead of failing.

## Repo layout

| File | What it is |
| --- | --- |
| `plugin.yaml` | The plugin's name, version and settings. Hermes reads this. |
| `dome_relay/__init__.py` | The plugin code (a stub for now). |
| `relay/` | The always-on callback service (the part that runs on Fly). |
| `SPEC.md` | The design contract. |
| `LICENSE` | MIT: anyone can use or change this, with credit. |

## New to repos? Quick guide

1. **Get a copy:** `git clone https://github.com/nerdcakes/hermes-dome-relay.git`
2. **See what changed:** `git status`
3. **Save your changes:** `git add .` then `git commit -m "what you did"`
4. **Upload them:** `git push`

## Security

Never commit URLs, keys or tokens.

The Hermes plugin (not built yet) must read secrets only through `agent.secret_scope.get_secret(...)` (Hermes catalog rule 11). Set `relay_url` in plugin config under `plugins.entries.dome-relay.settings`. The plugin must not read `.env`, `auth.json`, or `os.environ`.

The relay process in `relay/` is different. It runs on Fly, and Fly hands it secrets as environment variables. Those values stay out of this repo. See [The relay](#the-relay) below.

The contract is in [SPEC.md](SPEC.md). The plugin still does not run. The relay service does. The plugin must not wake Grok and must not hold the Grok webhook URL or key.

## The relay

This is the always-on box between the plugin and the Grok Bot executive.

1. The plugin registers a job (`POST /jobs`) with the relay key.
2. The relay saves the job, then POSTs the task text to the Grok routine webhook. **HTTP 200 means a run started. It does not mean the work is finished.**
3. The executive claims the job once (`POST /jobs/{job_id}/claim`) with a different key. A second claim, a canceled job, or an expired job is refused.
4. The executive posts the result (`POST /jobs/{job_id}/complete`).
5. The plugin reads results (`GET /results?since=...`). Reading does not delete them. The plugin keeps its own cursor.

A job lives at most **30 minutes** from the moment the relay received it, and sooner if the job's own deadline is sooner. The relay's clock decides. The time in the result body cannot extend that.

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

### Run the tests

From the repo root, with Python 3.12:

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

Registration, claim, complete, the identity hash, expiry, hop, and retention are specified in [SPEC.md](SPEC.md). This deploy is one owner. The plugin still has to keep Hermes profiles apart in its own database. A separate callback key per target, and a tool that deletes stored results, are still open.

## License

MIT, see [LICENSE](LICENSE).
