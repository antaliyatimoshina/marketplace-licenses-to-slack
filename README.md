# Marketplace trials → Slack (free)

Polls the Atlassian **Marketplace Reporting API** for **new evaluation licenses** (trials) in a UTC date window and posts a compact summary to Slack via **Incoming Webhook**.

## Setup

1. **Create Slack Incoming Webhook**
   - In Slack: Apps → “Incoming Webhooks” → Add to Workspace → pick channel → copy URL.

2. **Create an Atlassian API token**
   - https://id.atlassian.com/manage-profile/security/api-tokens  
   - Use your Atlassian account email + this token for Basic Auth.

3. **Find your Vendor ID**
   - In Marketplace Vendor Console URL or profile (a number like `1227491`).

4. **Fork this repo** and add GitHub **Secrets**:
   - `MP_USER` → your Atlassian account email
   - `MP_API_TOKEN` → API token
   - `VENDOR_ID` → your numeric vendor id
   - `SLACK_WEBHOOK` → the Slack incoming webhook URL

5. (Optional) Add `APPS` secret like `Mria CRM: CRM for Jira Teams` to limit to specific app(s).  
   Use comma-separated names for multiple apps.  
   Add `LOOKBACK_DAYS` (e.g., `1`) if you want to cover the last N days.

6. **Enable the workflow**
   - It runs every 30 minutes (UTC). You can also run it manually from the Actions tab.

## What it does

- Calls: `GET /rest/2/vendors/{vendorId}/reporting/licenses?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD`
- Filters to `evaluationLicense == true` with `evaluationStartDate` within the window.
- Posts a single message summarizing new trials.

## Notes

- The Marketplace API is eventually consistent; using a small **LOOKBACK_DAYS** (0–1) helps avoid misses.
- No Slack app/bot token required — Incoming Webhooks are sufficient.
- Keep the repo private if you store any customization; **secrets are safe** in Actions.

## Local test
```bash
export MP_USER="you@example.com"
export MP_API_TOKEN="…"
export VENDOR_ID="1227491"
export SLACK_WEBHOOK="https://hooks.slack.com/services/…"
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python src/notify_trials.py
