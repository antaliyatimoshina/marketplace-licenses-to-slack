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
DRY_RUN       = env("DRY_RUN") == "1"

APPS_FILTER   = set([a.strip() for a in os.getenv("APPS","").split(",") if a.strip()])
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))

# Date window (UTC)
today_utc = dt.datetime.utcnow().date()

def day_window_utc():
    """
    Returns (start_date, end_date) as the same YYYY-MM-DD date in UTC.
    If env DAY=YYYY-MM-DD is set, uses that date; else defaults to yesterday (UTC).
    """
    d = os.getenv("DAY")
    if d:
        s = e = dt.date.fromisoformat(d)
    else:
        e = dt.datetime.utcnow().date() - dt.timedelta(days=1)
        s = e
    return s, e

# Run once/day and post *yesterday* only (single-day window)
# This avoids re-posts and eventual-consistency hiccups.
if os.getenv("MODE_YESTERDAY", "1") == "1":
    start_date = today_utc - dt.timedelta(days=1)
    end_date = start_date
else:
    # fallback to original behavior if you ever need it
    LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "0"))
    start_date = today_utc - dt.timedelta(days=LOOKBACK_DAYS)
    end_date = today_utc

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
    Collect ANY new licenses (evaluation or commercial) and extract fields for Slack:
      • {customer} · {contactName} ({contactEmail}) · {LICENSE_TYPE} [· {N users}]
    Also include a stable 'licenseId' for de-dup (prefers E-… entitlement number).
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

    def email_domain(email):
        return email.split("@", 1)[1] if isinstance(email, str) and "@" in email else None

    def dedup_id(lic: dict) -> str | None:
        # Prefer the visible entitlement number (E-…)
        return first(
            lic.get("appEntitlementNumber"),
            lic.get("hostEntitlementNumber"),
            lic.get("appEntitlementId"),
            lic.get("hostEntitlementId"),
            # last-resort composite ID (addonKey + cloudId)
            f"{lic.get('addonKey')}::{lic.get('cloudId')}" if lic.get("addonKey") and lic.get("cloudId") else None,
        )

    rows = []
    for lic in items:
        app_name = first(lic.get("addonName"), g(lic, "app.name"), lic.get("appName"), "Unknown app")

        company     = g(lic, "contactDetails.company")
        site        = lic.get("cloudSiteHostname")
        tech_name   = g(lic, "contactDetails.technicalContact.name")
        bill_name   = g(lic, "contactDetails.billingContact.name")
        tech_email  = g(lic, "contactDetails.technicalContact.email")
        bill_email  = g(lic, "contactDetails.billingContact.email")

        customer = first(company, site, email_domain(tech_email), email_domain(bill_email), tech_name, bill_name, "Unknown customer")
        contact_name  = first(tech_name, bill_name)
        contact_email = first(tech_email, bill_email)

        license_type = (lic.get("licenseType") or lic.get("tier") or "LICENSE").upper()
        users = users_from_tier(lic.get("tier"))

        lid = dedup_id(lic)

        rows.append({
            "app": app_name,
            "customer": customer,
            "contactName": contact_name,
            "contactEmail": contact_email,
            "licenseType": license_type,
            "users": users,
            "licenseId": lid,   # <-- critical for de-dup
        })
    return rows

def fetch_uninstalls(vendor_id: str, start: dt.date, end: dt.date):
    """
    Fetch churn feedback (uninstall/unsubscribe/disable) for a UTC date window.
    Uses Feedback Details EXPORT with accept=json for richer fields.
    """
    base = "https://marketplace.atlassian.com"
    url = f"{base}/rest/2/vendors/{vendor_id}/reporting/feedback/details/export"
    params = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "accept": "json",
        # churn actions to include:
        "type": ["uninstall", "unsubscribe", "disable"],
    }
    auth = (MP_USER, MP_API_TOKEN)
    headers = {"Accept": "application/json"}
    r = requests.get(url, params=params, auth=auth, headers=headers, timeout=120)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("feedback", []) or data.get("items", []) or []
    return []

def pick_uninstalls(items):
    """
    Map feedback items to rows for Slack lines:
      • {customerOrSite} · {contactName} ({email}) · {TYPE}
    """
    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    rows = []
    for f in items:
        app = first(f.get("addonName"), f.get("addonKey"), "Unknown app")
        ftype = (f.get("feedbackType") or "").upper()  # UNINSTALL / UNSUBSCRIBE / DISABLE
        email = f.get("email")
        name = f.get("fullName")
        site = first(f.get("cloudSiteHostname"), f.get("cloudId"))
        cust = first(
            site,
            (email.split("@",1)[1] if isinstance(email, str) and "@" in email else None),
            "Unknown"
        )

        rows.append({
            "app": app,
            "customer": cust,
            "contactName": name,
            "contactEmail": email,
            "licenseType": ftype,  # we’ll print this as the action label
            "users": None,         # not present in feedback
        })
    return rows


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
        header = f":airplane: *New Marketplace licenses for {app}* ({date_label}, UTC)"
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
    if DRY_RUN:
        print("DRY_RUN=1 → would post:\n" + "\n".join(lines if isinstance(lines, list) else [text]))
    return
    r = requests.post(webhook, json={"text": text}, timeout=30)
    r.raise_for_status()
    print(f"Posted {sum(len(v) for v in by_app.values())} item(s) to Slack.")

def post_combined_to_slack(webhook, licenses_rows, uninstall_rows, start: dt.date, end: dt.date):
    if not licenses_rows and not uninstall_rows:
        print("Nothing to post (no new licenses or uninstalls).")
        return

    date_label = start.isoformat()  # single-day summary
    by_app_lic = {}
    for e in licenses_rows:
        by_app_lic.setdefault(e["app"], []).append(e)
    by_app_un  = {}
    for e in uninstall_rows:
        by_app_un.setdefault(e["app"], []).append(e)

    parts = []
    # Licenses section (first)
    for app, rows in by_app_lic.items():
        header = f":tada: *New Marketplace licenses for {app}* ({date_label}, UTC)"
        lines = []
        for e in rows:
            if e.get("contactName") and e.get("contactEmail"):
                contact = f"{e['contactName']} ({e['contactEmail']})"
            else:
                contact = e.get("contactName") or e.get("contactEmail") or "—"
            user_part = f" · {e['users']} users" if e.get("users") else ""
            lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{user_part}")
        parts.append(header + "\n" + "\n".join(lines))

    # Uninstalls section (second)
    for app, rows in by_app_un.items():
        header = f":no_entry: *Uninstalls / Unsubscribes for {app}* ({date_label}, UTC)"
        lines = []
        for e in rows:
            if e.get("contactName") and e.get("contactEmail"):
                contact = f"{e['contactName']} ({e['contactEmail']})"
            else:
                contact = e.get("contactName") or e.get("contactEmail") or "—"
            lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}")
        parts.append(header + "\n" + "\n".join(lines))

    text = "\n\n".join(parts)
    r = requests.post(webhook, json={"text": text}, timeout=30)
    r.raise_for_status()
    print(f"Posted combined message: {sum(len(v) for v in by_app_lic.values())} licenses, {sum(len(v) for v in by_app_un.values())} uninstalls.")


def main():
    # fixed single-day window (yesterday or DAY=YYYY-MM-DD)
    start_date, end_date = day_window_utc()

    lic_items = fetch_licenses(VENDOR_ID, start_date, end_date)   # you already have this
    lic_rows  = pick_new_evaluations(lic_items, start_date, end_date)

    un_items  = fetch_uninstalls(VENDOR_ID, start_date, end_date)
    un_rows   = pick_uninstalls(un_items)

    post_combined_to_slack(SLACK_WEBHOOK, lic_rows, un_rows, start_date, end_date)

    if not lic_rows and not un_rows:
        requests.post(SLACK_WEBHOOK, json={"text": f"ℹ️ No new licenses or uninstalls for {start_date} (UTC)."})
    return

