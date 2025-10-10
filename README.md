# Marketplace licenses → Slack (free)

Polls the Atlassian Marketplace Reporting API once per day for new licenses (trials + paid) and uninstalls/unsubscribes/disable events for a single UTC date (yesterday by default), then posts one compact message to Slack via Incoming Webhook. No state is stored.

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

6. **Enable the workflow**
   - It runs everyday at 7:00 (UTC).
     
## What it does

- Calls: `GET /rest/2/vendors/{vendorId}/reporting/licenses/export?accept=json&dateType=start&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD` (new licenses: trials + paid)
- Calls: `GET /rest/2/vendors/{vendorId}/reporting/feedback/details/export?accept=json&type=uninstall&type=unsubscribe&type=disable&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD` (uninstalls/churn)
- Parses company/contact, license type, and (when present) user count from the `tier`
- Posts **one Slack message per day**, grouped by app: **New licenses** first, then **Uninstalls / Unsubscribes**
- Uses a **single UTC day** (yesterday by default); set `DAY=YYYY-MM-DD` to backfill a specific date
- No state stored; runs on GitHub Actions and posts via Slack Incoming Webhook

## Notes

- The Marketplace API is eventually consistent;
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
