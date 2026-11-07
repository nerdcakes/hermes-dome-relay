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
| `LICENSE` | MIT: anyone can use or change this, with credit. |

## New to repos? Quick guide

1. **Get a copy:** `git clone https://github.com/nerdcakes/hermes-dome-relay.git`
2. **See what changed:** `git status`
3. **Save your changes:** `git add .` then `git commit -m "what you did"`
4. **Upload them:** `git push`

## Security

Never commit URLs, keys or tokens. Secrets are read only through `agent.secret_scope.get_secret(...)` (Hermes catalog rule 11). Set `relay_url` in plugin config under `plugins.entries.dome-relay.settings`. Never read secrets from `.env`, `auth.json`, or `os.environ`.

The design draft is in [SPEC.md](SPEC.md). Nothing here runs yet.

## License

MIT, see [LICENSE](LICENSE).
