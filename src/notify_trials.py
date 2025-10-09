#!/usr/bin/env python3
import os
import sys
import json
import datetime as dt
from dateutil import tz
import requests

# Required env vars (set as GitHub Secrets in Actions):
# MP_USER           -> your Atlassian account email
# MP_API_TOKEN      -> your Atlassian API token
# VENDOR_ID         -> numeric vendor id
# SLACK_WEBHOOK     -> Slack Incoming Webhook URL
#
# Optional:
# APPS              -> comma-separated app names to include (defaults to all)
# LOOKBACK_DAYS     -> integer; check N days back instead of only "today" (default 0)

def env(name, default=None, required=False):
    v = os.getenv(name, default)
    if required and not v:
        print(f"Missing required env var: {name}", file=sys.stderr)
        sys.exit(2)
    return v

MP_USER       = env("MP_USER", required=True)
MP_API_TOKEN  = env("MP_API_TOKEN", required=True)
VENDOR_ID     = env("VENDOR_ID", required=True)
SLACK_WEBHOOK = env("SLACK_WEBHOOK", required=True)

APPS_FILTER   = set([a.strip() for a in os.getenv("APPS","").split(",") if a.strip()])
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))

# Date window (UTC)
today_utc = dt.datetime.utcnow().date()
start_date = today_utc - dt.timedelta(days=LOOKBACK_DAYS)
end_date   = today_utc  # inclusive day; API uses start/end as dates

def fetch_licenses(vendor_id: str, start: dt.date, end: dt.date):
    """Pull licenses for date window. Returns list of dicts."""
    url = f"https://marketplace.atlassian.com/rest/2/vendors/{vendor_id}/reporting/licenses"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat()
    }
    auth = (MP_USER, MP_API_TOKEN)

    # The API may paginate; handle 'next' if present
    out = []
    while True:
        r = requests.get(url, params=params, auth=auth, timeout=60)
        r.raise_for_status()
        data = r.json()

        # Different accounts sometimes return top-level or nested "licenses"
        items = data.get("licenses", data)
        if isinstance(items, dict) and "licenses" in items:
            items = items["licenses"]

        if not isinstance(items, list):
            items = []

        out.extend(items)

        next_link = None
        # Safely find link headers in body if exposed that way
        for k in ("_links", "links", "page", "paging"):
            if isinstance(data.get(k), dict):
                next_link = data[k].get("next") or data[k].get("nextPage")
                if next_link:
                    break

        if not next_link:
            break
        # Absolute or relative next link
        if next_link.startswith("http"):
            url = next_link
            params = {}
        else:
            url = f"https://marketplace.atlassian.com{next_link}"
            params = {}
    return out

def pick_new_evaluations(items, date_from: dt.date, date_to: dt.date):
    """Filter to evaluations that started within [date_from, date_to]."""
    wanted = []
    for lic in items:
        # Normalize fields safely
        eval_flag = lic.get("evaluationLicense") or lic.get("isEvaluation") or False
        if not eval_flag:
            continue

        # Dates are ISO strings (UTC); some payloads use evaluationStartDate
        start = lic.get("evaluationStartDate") or lic.get("startDate")
        if not isinstance(start, str):
            continue
        # Keep only those that start within the window (string startswith is fine for YYYY-MM-DD)
        if not (start.startswith(date_from.isoformat()) or
                (date_from != date_to and date_from.isoformat() <= start[:10] <= date_to.isoformat())):
            continue

        app_name = (
            lic.get("appName")
            or (lic.get("app") or {}).get("name")
            or lic.get("addonName")
            or "Unknown app"
        )
        if APPS_FILTER and app_name not in APPS_FILTER:
            continue

        customer = (
            (lic.get("customer") or {}).get("name")
            or lic.get("customerName")
            or "Unknown customer"
        )
        license_id = lic.get("licenseId") or (lic.get("license") or {}).get("licenseId") or "N/A"
        end_date = lic.get("evaluationEndDate") or lic.get("endDate") or "N/A"
        hosting  = lic.get("hosting") or (lic.get("deployment") or "").upper() or "N/A"
        plan     = lic.get("edition") or lic.get("plan") or ""

        wanted.append({
            "app": app_name,
            "customer": customer,
            "licenseId": license_id,
            "end": end_date,
            "hosting": hosting,
            "plan": plan
        })
    return wanted

def post_to_slack(webhook, items, start: dt.date, end: dt.date):
    if not items:
        print("No new trials for window:", start, "→", end)
        return

    date_label = start.isoformat() if start == end else f"{start.isoformat()}–{end.isoformat()}"
    lines = []
    for e in items:
        suffix = f", {e['plan']}" if e['plan'] else ""
        lines.append(
            f"• *{e['app']}* — {e['customer']} "
            f"(ID `{e['licenseId']}`) · {e['hosting']}{suffix} · ends {e['end']}"
        )

    text = f":tada: *New Marketplace trial(s)* ({date_label}, UTC)\n" + "\n".join(lines)
    r = requests.post(webhook, json={"text": text}, timeout=30)
    r.raise_for_status()
    print(f"Posted {len(items)} item(s) to Slack.")

def main():
    items = fetch_licenses(VENDOR_ID, start_date, end_date)
    new_evals = pick_new_evaluations(items, start_date, end_date)
    post_to_slack(SLACK_WEBHOOK, new_evals, start_date, end_date)

if __name__ == "__main__":
    main()
