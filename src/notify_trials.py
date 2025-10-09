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
    """
    Fetch licenses via the EXPORT endpoint (JSON) for a UTC date window.
    Robust to payload being either a list or an object wrapper.
    """
    base = "https://marketplace.atlassian.com"
    url = f"{base}/rest/2/vendors/{vendor_id}/reporting/licenses/export"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "dateType": "start",        # filter by license start date
        "accept": "json",           # export API returns JSON when accept=json
        "withDataInsights": "true", # include evaluation/customer fields
    }
    auth = (MP_USER, MP_API_TOKEN)
    headers = {"Accept": "application/json"}

    r = requests.get(url, params=params, auth=auth, headers=headers, timeout=120)
    r.raise_for_status()
    payload = r.json()

    def extract_items(p):
        # If the API returns a bare array
        if isinstance(p, list):
            return p
        # If it returns an object wrapper
        if isinstance(p, dict):
            for key in ("licenses", "items", "data", "results", "values"):
                v = p.get(key)
                if isinstance(v, list):
                    return v
            # nested containers some responses use
            for key in ("content", "page", "paging", "_embedded"):
                v = p.get(key)
                if isinstance(v, dict):
                    for k2 in ("licenses", "items", "data", "results", "values"):
                        v2 = v.get(k2)
                        if isinstance(v2, list):
                            return v2
            # single-record fallback
            if any(k in p for k in ("licenseId", "appName", "customer", "evaluationStartDate")):
                return [p]
        return []

    return extract_items(payload)

def pick_new_evaluations(items, date_from: dt.date, date_to: dt.date):
    """
    Keep only evaluation licenses and extract fields robustly.
    We do NOT re-filter by date here (dateType=start already did it).
    """

    def pick_first(d, *paths):
        # paths can be "a.b.c" or ("a","b","c"); returns first non-empty value
        for path in paths:
            keys = path.split(".") if isinstance(path, str) else path
            cur = d
            ok = True
            for k in keys:
                if isinstance(cur, dict) and k in cur:
                    cur = cur[k]
                else:
                    ok = False
                    break
            if ok and cur not in (None, "", []):
                return cur
        return None

    wanted = []
    for lic in items:
        # evaluation if explicit flag OR eval date present
        is_eval = bool(lic.get("evaluationLicense")) or ("evaluationStartDate" in lic)
        if not is_eval:
            continue

        app_name = (
            pick_first(lic, "appName", "app.name", "addonName")
            or "Unknown app"
        )

        # customer names appear under several shapes; try them in order
        customer = (
            pick_first(
                lic,
                "customer.name", "customerName",
                "dataInsights.customerName", "insights.customerName",
                "organization.name", "endUser.name",
                "purchaser.name", "account.name", "company.name",
                "license.customer.name"
            ) or
            # fall back to an id if we have one
            pick_first(lic, "customer.id", "account.id", "organization.id") or
            "Unknown customer"
        )

        # license / entitlement number variants
        license_id = (
            pick_first(
                lic,
                "licenseId", "license.licenseId",
                "entitlementNumber", "license.entitlementNumber",
                "supportEntitlementNumber", "sen",
                "entitlement.id", "entitlement.number"
            ) or "N/A"
        )

        end_date = (
            pick_first(lic, "evaluationEndDate", "endDate", "license.maintenanceEndDate")
            or "N/A"
        )

        hosting = (
            pick_first(lic, "hosting", "deployment", "license.hosting") or "N/A"
        )
        hosting = str(hosting).upper().replace("_", " ")

        plan = pick_first(lic, "edition", "plan", "license.edition") or ""

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
