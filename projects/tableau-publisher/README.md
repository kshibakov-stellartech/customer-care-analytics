# cc-tableau

Automation for Tableau datasource operations (no UI): clone a template datasource, replace Custom SQL, and publish as a new datasource.

## Security

1. Put secrets in `cc_tableau/.env` only.
2. Never commit `.env`.
3. Rotate exposed keys if they were ever shared in chat/history.

## Setup

1. Copy env template:
   - `cp cc_tableau/.env.example cc_tableau/.env`
2. Fill required variables in `cc_tableau/.env`.

## Run

Load env and start script:

```bash
set -a
source cc_tableau/.env
set +a
python3 cc_tableau/scripts/publish_datasource.py
```

The script will prompt for:
1. SQL code/path (example: `duplicates` or `cc_reports/duplicates.sql`)
2. New datasource name (example: `cc-tech-duplicates`)

## VS Code Task

Run task:
- `Tableau: Publish datasource from template`

It executes:
- `python3 cc_tableau/scripts/publish_datasource.py`

(Env vars still must be loaded in your shell/session.)
