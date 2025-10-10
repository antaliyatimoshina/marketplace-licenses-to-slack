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

# Pick the reporting day (yesterday by default, or DAY=YYYY-MM-DD for backfill)
start_date, end_date = day_window_utc()

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
            "appKey": app_key,
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
            "appKey": app_key,
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
    """
    One message per appKey:
      {Pretty App Name} Marketplace Events (YYYY-MM-DD, UTC)
      ✈️ New licenses
      • customer · Name (email) · TYPE [· N users]
      
      ➖ Uninstalls / Unsubscribes
      • customer/site · Name (email) · TYPE
    """
    import re

    if not licenses_rows and not uninstall_rows:
        requests.post(webhook, json={"text": f"ℹ️ No new licenses or uninstalls for {start.isoformat()} (UTC)."})
        print("Nothing to post.")
        return

    def prettiest_name(candidates):
        # Prefer names that have spaces/colon (human labels) over bare keys
        cand = sorted(candidates, key=lambda s: (":" not in s and " " not in s, len(s)))
        return cand[0] if cand else "Unknown app"

    # Group by canonical key
    groups = {}
    for r in (licenses_rows or []):
        k = r.get("appKey") or r.get("app")
        g = groups.setdefault(k, {"names": set(), "lic": [], "un": []})
        g["names"].add(r.get("app") or "")
        g["lic"].append(r)
    for r in (uninstall_rows or []):
        k = r.get("appKey") or r.get("app")
        g = groups.setdefault(k, {"names": set(), "lic": [], "un": []})
        g["names"].add(r.get("app") or "")
        g["un"].append(r)

    date_label = start.isoformat()
    parts = []
    for k in sorted(groups.keys() or []):
        g = groups[k]
        app_title = prettiest_name(g["names"])
        section_lines = []

        if g["lic"]:
            lines = []
            for e in g["lic"]:
                if e.get("contactName") and e.get("contactEmail"):
                    contact = f"{e['contactName']} ({e['contactEmail']})"
                else:
                    contact = e.get("contactName") or e.get("contactEmail") or "—"
                users_part = f" · {e['users']} users" if e.get("users") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{users_part}")
            section_lines.append(":airplane: New licenses\n" + "\n".join(lines))

        if g["un"]:
            lines = []
            for e in g["un"]:
                if e.get("contactName") and e.get("contactEmail"):
                    contact = f"{e['contactName']} ({e['contactEmail']})"
                else:
                    contact = e.get("contactName") or e.get("contactEmail") or "—"
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}")
            section_lines.append(":heavy_minus_sign: Uninstalls / Unsubscribes\n" + "\n".join(lines))

        parts.append(f"{app_title} Marketplace Events ({date_label}, UTC)\n\n" + "\n\n".join(section_lines))

    text = "\n\n".join(parts)
    r = requests.post(webhook, json={"text": text}, timeout=30)
    r.raise_for_status()
    print("Posted combined message (merged by appKey).")


def main():
    start_date, end_date = day_window_utc()
    print(f"[INFO] Daily window (UTC): {start_date}")

    try:
        lic_items = fetch_licenses(VENDOR_ID, start_date, end_date)
        lic_rows  = pick_new_evaluations(lic_items, start_date, end_date)
        print(f"[INFO] Licenses: raw={len(lic_items)} mapped={len(lic_rows)}")

        un_items  = fetch_uninstalls(VENDOR_ID, start_date, end_date)
        un_rows   = pick_uninstalls(un_items)
        print(f"[INFO] Uninstalls: raw={len(un_items)} mapped={len(un_rows)}")
    except Exception as e:
        print(f"[ERROR] Exception during fetch/map: {e}")
        # Optional: post the error to Slack (comment out if you prefer silent failures)
        requests.post(SLACK_WEBHOOK, json={"text": f"❗ Script error: {e}"})
        raise

    if not lic_rows and not un_rows:
        msg = {"text": f"ℹ️ No new licenses or uninstalls for {start_date} (UTC)."}
        requests.post(SLACK_WEBHOOK, json=msg)
        print("[INFO] No items; posted 'no changes' message to Slack.")
        return

    post_combined_to_slack(SLACK_WEBHOOK, lic_rows, un_rows, start_date, end_date)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # make failures visible in logs (and optionally in Slack)
        import traceback, os, requests
        traceback.print_exc()
        hook = os.getenv("SLACK_WEBHOOK")
        if hook:
            requests.post(hook, json={"text": f"❗ notify_licenses.py crashed: {e}"})
        raise

