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
    Collect ANY new licenses (evaluation or commercial) that started in the window.
    Output fields for Slack rows:
      • {customer} · {contactName} ({contactEmail}) · {LICENSE_TYPE} [· {N users}]
    """
    import re

    def g(d, path):
        cur = d
        for k in path.split("."):
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return None
        return cur

    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    def users_from_tier(tier):
        if isinstance(tier, str):
            m = re.search(r"(\d+)\s*Users?", tier, re.I)
            if m:
                return int(m.group(1))
        return None

    wanted = []
    for lic in items:
        # We include everything: trials and paid. (dateType=start already filtered by start date.)
        app_name = first(lic.get("addonName"), g(lic, "app.name"), lic.get("appName"), "Unknown app")

        company     = g(lic, "contactDetails.company")
        site        = lic.get("cloudSiteHostname")
        tech_name   = g(lic, "contactDetails.technicalContact.name")
        bill_name   = g(lic, "contactDetails.billingContact.name")
        tech_email  = g(lic, "contactDetails.technicalContact.email")
        bill_email  = g(lic, "contactDetails.billingContact.email")

        def email_domain(email):
            return email.split("@", 1)[1] if isinstance(email, str) and "@" in email else None

        customer = first(
            company,
            site,
            email_domain(tech_email),
            email_domain(bill_email),
            tech_name,
            bill_name,
            "Unknown customer"
        )

        contact_name  = first(tech_name, bill_name)
        contact_email = first(tech_email, bill_email)

        # License type label (e.g., EVALUATION, COMMERCIAL, SUBSCRIPTION), with fallback to tier
        license_type = (lic.get("licenseType") or lic.get("tier") or "LICENSE").upper()

        users = users_from_tier(lic.get("tier"))

        wanted.append({
            "app": app_name,
            "customer": customer,
            "contactName": contact_name,
            "contactEmail": contact_email,
            "licenseType": license_type,
            "users": users,
        })
    return wanted

def post_to_slack(webhook, items, start: dt.date, end: dt.date):
    if not items:
        print("No new licenses for window:", start, "→", end)
        return

    by_app = {}
    for e in items:
        by_app.setdefault(e["app"], []).append(e)

    date_label = start.isoformat() if start == end else f"{start.isoformat()}–{end.isoformat()}"

    blocks = []
    for app, rows in by_app.items():
        header = f":tada: *New Marketplace licenses for {app}* ({date_label}, UTC)"
        lines = []
        for e in rows:
            if e.get("contactName") and e.get("contactEmail"):
                contact = f"{e['contactName']} ({e['contactEmail']})"
            else:
                contact = e.get("contactName") or e.get("contactEmail") or "—"
            user_part = f" · {e['users']} users" if e.get("users") else ""
            lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{user_part}")
        blocks.append(header + "\n" + "\n".join(lines))

    text = "\n\n".join(blocks)
    r = requests.post(webhook, json={"text": text}, timeout=30)
    r.raise_for_status()
    print(f"Posted {sum(len(v) for v in by_app.values())} item(s) to Slack.")

def main():
    items = fetch_licenses(VENDOR_ID, start_date, end_date)
    new_evals = pick_new_evaluations(items, start_date, end_date)
    post_to_slack(SLACK_WEBHOOK, new_evals, start_date, end_date)

if __name__ == "__main__":
    main()
