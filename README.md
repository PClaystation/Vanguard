# Vanguard

Discord moderation + utility bot with slash commands, reminders, incident guardrails, and vote workflows.

## License Notice

This repository is source-available, not open-source.

- Unauthorized copying, self-hosting, redistribution, and derivative/competing deployments are prohibited.
- See [LICENSE](LICENSE), [TERMS_OF_SERVICE.md](TERMS_OF_SERVICE.md), and [PRIVACY_POLICY.md](PRIVACY_POLICY.md) for legal terms.

## What It Includes

- Moderation commands: lockdown, timeout, warn, purge, undo/cases
- Community commands: reminders, poll/choose/roll, server/user info
- User install commands: personal reminders, AI chat, avatar/banner lookups, mutual server listing, and install-context detection
- Ops features: status checks, guard mode, advanced vote tracking (custom ballots, elections, runoff, quorum/threshold rules, and auto-executed action votes via `/voteaction`), AI chat relay with session memory (`/vanguard`) and reset (`/vanguardreset`)
- Identity hooks: optional Continental ID account lookup via `/continentalid`
- Policy commands: privacy and terms

## Requirements

- Python 3.12+
- A Discord bot token

## Quick Start (Authorized Operators Only)

1. Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Configure environment:

```bash
cp .env.example .env
# fill in DISCORD_BOT_TOKEN and optional vars
```

3. Run the bot:

```bash
python3 thingamabot.py
```

## Configuration

Environment variables are read from `.env` (via `python-dotenv`) or your shell.

- `DISCORD_BOT_TOKEN` (required)
- `AI_SERVER_BASE_URL` (default: derived from `AI_SERVER_URL`, usually `http://localhost:3001`)
- `AI_SERVER_URL` (legacy/default ask endpoint, default: `http://localhost:3001/ask`)
- `AI_ASK_URL` (default: `${AI_SERVER_BASE_URL}/ask`)
- `AI_CHAT_URL` (default: `${AI_SERVER_BASE_URL}/chat`)
- `AI_HEALTH_URL` (default: `${AI_SERVER_BASE_URL}/health`)
- `AI_MODELS_URL` (default: `${AI_SERVER_BASE_URL}/models`)
- `AI_SESSION_URL` (default: `${AI_SERVER_BASE_URL}/session`)
- `CONTINENTAL_ID_BASE_URL` (optional base URL for Continental ID service integration)
- `CONTINENTAL_ID_HEALTH_URL` (optional override, default: `${CONTINENTAL_ID_BASE_URL}/api/vanguard/health`)
- `CONTINENTAL_ID_RESOLVE_URL` (optional override, default: `${CONTINENTAL_ID_BASE_URL}/api/vanguard/users/resolve`)
- `AI_REQUEST_TIMEOUT_SECONDS` (default: `60`)
- `AI_CHAT_STYLE` (`concise|balanced|detailed`, default: `balanced`)
- `AI_HISTORY_MESSAGES` (default: `12`, max: `24`)
- `AI_USE_CONTEXT` (default: `true`)
- `AI_USE_CACHE` (default: `true`)
- `AI_INCLUDE_DEBUG` (default: `false`)
- `AI_MODEL` (optional model override for `/chat`)
- `AI_TEMPERATURE` (optional, range `0..2`)
- `AI_TOP_P` (optional, range `0..1`)
- `AI_NUM_PREDICT` (optional, range `1..4096`)
- `AI_REPEAT_PENALTY` (optional, range `0.8..2`)
- `FLAG_USER_URL` (default: legacy backend URL, or `${CONTINENTAL_ID_BASE_URL}/api/vanguard/users/flag` when using Continental ID)
- `UNFLAG_USER_URL` (default: legacy backend URL, or `${CONTINENTAL_ID_BASE_URL}/api/vanguard/users/unflag` when using Continental ID)
- `VANGUARD_BACKEND_API_KEY` (optional secret header value used for AI/backend requests)
- `VANGUARD_BACKEND_KEY_HEADER` (default: `X-Vanguard-Api-Key`)
- `VANGUARD_INSTANCE_ID` (optional instance identifier sent to backend/license service)
- `VANGUARD_INSTANCE_HEADER` (default: `X-Vanguard-Instance-Id`)
- `VANGUARD_ALLOWED_GUILD_IDS` (optional comma-separated guild allowlist)
- `VANGUARD_LICENSE_VERIFY_URL` (optional override, default: `${CONTINENTAL_ID_BASE_URL}/api/vanguard/license/verify` when using Continental ID)
- `VANGUARD_LICENSE_KEY` (optional bearer token for license verification)
- `VANGUARD_REQUIRE_LICENSE` (default: `false`; when `true`, commands are blocked if license check fails)
- `VANGUARD_LICENSE_RECHECK_SECONDS` (default: `900`, range: `60..86400`)
- `VANGUARD_CONTROL_CENTER_ENABLED` (default: `false`; starts the web dashboard inside the bot process)
- `VANGUARD_CONTROL_CENTER_HOST` (default: `127.0.0.1`)
- `VANGUARD_CONTROL_CENTER_PORT` (default: `8080`)
- `VANGUARD_CONTROL_CENTER_CLIENT_ID` (Discord application client ID for OAuth login; usually your app/application ID)
- `VANGUARD_CONTROL_CENTER_CLIENT_SECRET` (Discord OAuth client secret)
- `VANGUARD_CONTROL_CENTER_REDIRECT_URI` (registered Discord OAuth callback URL, for example `http://127.0.0.1:8080/control/auth/callback`)
- `VANGUARD_CONTROL_CENTER_TOKEN` (optional operator override token for full-instance access; normal users should log in with Discord)
- `VANGUARD_CONTROL_CENTER_PUBLIC_URL` (optional URL advertised by `/controlcenter`; useful when the bot is reverse-proxied)
- `VANGUARD_GUILD_JOIN_NOTIFY_USER_ID` (optional Discord user ID to DM whenever Vanguard joins a server)
- `PRIVACY_POLICY_URL` (optional)
- `TERMS_OF_SERVICE_URL` (optional)
- `VANGUARD_DATA_DIR` (default: `./data`)

## Hosted-Only Hardening

Use these controls if you want users to consume your hosted bot instead of self-hosting forks:

- Put your backend behind `VANGUARD_BACKEND_API_KEY` so cloned bots cannot call private AI/moderation services.
- Set `VANGUARD_ALLOWED_GUILD_IDS` so this bot instance only runs in your approved servers.
- Enable `VANGUARD_REQUIRE_LICENSE=true` and configure `VANGUARD_LICENSE_VERIFY_URL` for a remote kill switch.

## Data Storage

Runtime JSON state is stored in `data/`:

- `data/settings.json`
- `data/reminders.json`
- `data/modlog.json`
- `data/votes.json`

On startup, legacy root files are migrated into `data/` automatically when possible.

## User Install Support

Vanguard supports both server installs and user installs.

- User installs expose personal-safe commands in DMs and other private contexts, including `/installcontext`, `/mutualservers`, `/banner`, `/userinfo`, reminders, and AI chat.
- Server moderation, guard, voting administration, and guild configuration commands remain guild-only.
- `/help` adapts its overview so account-safe commands and server-only commands are separated clearly.

## Control Center

Vanguard now includes an embedded control center website for per-server configuration.

1. Set these values in `.env`:

```bash
VANGUARD_CONTROL_CENTER_ENABLED=true
VANGUARD_CONTROL_CENTER_HOST=127.0.0.1
VANGUARD_CONTROL_CENTER_PORT=8080
VANGUARD_CONTROL_CENTER_CLIENT_ID=your-discord-application-id
VANGUARD_CONTROL_CENTER_CLIENT_SECRET=your-discord-client-secret
VANGUARD_CONTROL_CENTER_REDIRECT_URI=http://127.0.0.1:8080/control/auth/callback
```

2. Start the bot and open `http://127.0.0.1:8080` for the branded site.
3. Sign in with Discord.
4. The dashboard will only show Vanguard guilds where the signed-in user can already moderate/configure the bot.
5. Pick a guild and update:
   - welcome channel, welcome role, and welcome message
   - ops/log channels, lockdown role, and extra mod roles
   - guard presets plus advanced anti-raid thresholds

The public landing page is served at `/` and the authenticated control center lives at `/control/` on the same website. The optional `VANGUARD_CONTROL_CENTER_TOKEN` still works as an operator override if you need full-instance access. The `/controlcenter` command shows the configured dashboard URL to moderators. By default the site binds to localhost; if you expose it behind a reverse proxy, set `VANGUARD_CONTROL_CENTER_PUBLIC_URL`, use an HTTPS callback URL, and keep the operator token secret.

## Testing and Linting

```bash
ruff check .
pytest -q
python3 -m py_compile thingamabot.py vote.py data_paths.py
```

## CI

GitHub Actions runs compile checks, Ruff, and Pytest on push/PR.

## Website (GitHub Pages)

- Static site files are in `docs/` for GitHub Pages publishing.
- In GitHub repo settings, set Pages source to:
  - `Deploy from a branch`
  - Branch: `main` (or your default branch)
  - Folder: `/docs`
- `docs/404.html` is included for GitHub Pages fallback handling.

## Project Notes

- `thingamabot.py` is the active Python bot entrypoint.
- Legacy Node implementation was archived to `archive/legacy/thingamabot.js` to avoid split maintenance.
