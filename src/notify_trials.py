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
    Keep only evaluation licenses and extract fields based on export payload.
    We do NOT re-filter by date (dateType=start already did it).
    """

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

    def email_domain(email: str | None):
        if isinstance(email, str) and "@" in email:
            return email.split("@", 1)[1]
        return None

    wanted = []
    for lic in items:
        # Treat as evaluation if licenseType/tier says so or eval markers exist
        is_eval = (
            (lic.get("licenseType") == "EVALUATION") or
            (lic.get("tier") == "Evaluation") or
            ("latestEvaluationStartDate" in lic) or
            ("evaluationStartDate" in lic) or
            bool(lic.get("evaluationLicense"))
        )
        if not is_eval:
            continue

        app_name = first(lic.get("addonName"), g(lic, "app.name"), lic.get("appName"), "Unknown app")

        # CUSTOMER: prefer company; then contact names; then site hostname; then email domain
        company = g(lic, "contactDetails.company")
        tech_name = g(lic, "contactDetails.technicalContact.name")
        bill_name = g(lic, "contactDetails.billingContact.name")
        site = lic.get("cloudSiteHostname")
        tech_email = g(lic, "contactDetails.technicalContact.email")
        bill_email = g(lic, "contactDetails.billingContact.email")

        customer = first(
            company,
            tech_name,
            bill_name,
            site,
            email_domain(tech_email),
            email_domain(bill_email),
            "Unknown customer"
        )

        # LICENSE / ENTITLEMENT NUMBER
        license_id = first(
            lic.get("appEntitlementNumber"),
            lic.get("hostEntitlementNumber"),
            lic.get("appEntitlementId"),
            lic.get("hostEntitlementId"),
            lic.get("licenseId"),
            "N/A"
        )

        end_date = first(
            lic.get("evaluationEndDate"),
            lic.get("maintenanceEndDate"),
            g(lic, "license.maintenanceEndDate"),
            "N/A"
        )

        hosting = first(lic.get("hosting"), g(lic, "license.hosting"), "N/A")
        hosting = str(hosting).upper().replace("_", " ")

        plan = first(lic.get("tier"), lic.get("licenseType"), g(lic, "license.edition"), "")

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
    print("DEBUG sample item keys:", list(items[0].keys()))
    print("DEBUG sample item:", json.dumps(items[0], indent=2)[:4000])
    new_evals = pick_new_evaluations(items, start_date, end_date)
    post_to_slack(SLACK_WEBHOOK, new_evals, start_date, end_date)

if __name__ == "__main__":
    main()
