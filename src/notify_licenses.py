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
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

def slack_post(payload: dict):
    """Post to Slack unless DRY_RUN=1, in which case just log."""
    if DRY_RUN:
        print("[DRY_RUN] Would post to Slack:\n" + payload.get("text","")[:2000])
        return
    r = requests.post(SLACK_WEBHOOK, json=payload, timeout=30)
    r.raise_for_status()


APPS_FILTER   = set([a.strip() for a in os.getenv("APPS","").split(",") if a.strip()])

# Date window (UTC)
today_utc = dt.datetime.utcnow().date()

def _extract_license_id(lic: dict):
    """Prefer the visible E-… entitlement; fall back to other ids/composite."""
    def _first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None
    return _first(
        lic.get("appEntitlementNumber"),
        lic.get("hostEntitlementNumber"),
        lic.get("appEntitlementId"),
        lic.get("hostEntitlementId"),
        (f"{lic.get('addonKey')}::{lic.get('cloudId')}"
         if lic.get("addonKey") and lic.get("cloudId") else None),
    )

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
    Map ANY new licenses (trial/paid) to rows for Slack and include:
      - app      : pretty name
      - appKey   : canonical key for grouping with uninstall rows
      - customer : company/site/email-domain fallback
      - contactName/contactEmail
      - licenseType : e.g., EVALUATION/COMMERCIAL (uppercased)
      - users    : parsed from 'tier' when present
      - licenseId: visible entitlement number if available
    """
    import re

    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    def domain(email):
        return email.split("@", 1)[1] if isinstance(email, str) and "@" in email else None

    rows = []
    for lic in (items or []):
        # Names/keys
        app_name = first(
            lic.get("addonName"),
            (lic.get("app") or {}).get("name"),
            lic.get("appName"),
            "Unknown app",
        )
        # compute the key inline (no temporary variable)
        app_key_expr = first(
            lic.get("addonKey"),
            (lic.get("app") or {}).get("key"),
            app_name,  # last-resort fallback to keep grouping stable
        )

        # Contact/customer
        cd   = lic.get("contactDetails") or {}
        tech = cd.get("technicalContact") or {}
        bill = cd.get("billingContact") or {}
        site = lic.get("cloudSiteHostname")

        customer = first(
            cd.get("company"),
            site,
            domain(tech.get("email")),
            domain(bill.get("email")),
            tech.get("name"),
            bill.get("name"),
            "Unknown customer",
        )
        contact_name  = first(tech.get("name"),  bill.get("name"))
        contact_email = first(tech.get("email"), bill.get("email"))

        # Type & users
        license_id = _extract_license_id(lic)
        license_type = (lic.get("licenseType") or lic.get("tier") or "LICENSE").upper()
        users = None
        if isinstance(lic.get("tier"), str):
            m = re.search(r"(\d+)\s*Users?", lic["tier"], re.I)
            if m:
                users = int(m.group(1))

        # Best visible ID
        license_id = first(
            lic.get("appEntitlementNumber"),
            lic.get("hostEntitlementNumber"),
            lic.get("appEntitlementId"),
            lic.get("hostEntitlementId"),
            (f"{lic.get('addonKey')}::{lic.get('cloudId')}"
             if lic.get("addonKey") and lic.get("cloudId") else None),
        )

        rows.append({
            "app": app_name,
            "appKey": app_key_expr,          # <— computed inline; no NameError possible
            "customer": customer,
            "contactName": contact_name,
            "contactEmail": contact_email,
            "licenseType": license_type,
            "users": users,
            "licenseId": license_id,
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

def pick_uninstalls(items, name_map=None):
    """
    Map feedback items (uninstall/unsubscribe/disable) to rows for Slack:
      • {customerOrSiteOrId} · {contactName} ({email}) · {TYPE}
    Includes:
      - app     : pretty name (addonName → name_map[addonKey] → addonKey)
      - appKey  : canonical key for grouping with license rows
      - licenseId: best available entitlement/license id (E-… preferred)
    """
    import uuid

    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    def is_uuidlike(s):
        try:
            uuid.UUID(str(s))
            return True
        except Exception:
            return False

    def domain(email):
        return email.split("@", 1)[1] if isinstance(email, str) and "@" in email else None

    rows = []
    for f in (items or []):
        key = first(f.get("addonKey"), (f.get("app") or {}).get("key"))
        app_name = first(f.get("addonName"), (name_map or {}).get(key), key, "Unknown app")
        app_key_expr = key or app_name

        ftype = (f.get("feedbackType") or "").upper()  # UNINSTALL / UNSUBSCRIBE / DISABLE
        email = f.get("email")
        name  = f.get("fullName")
        site  = first(f.get("cloudSiteHostname"), (None if is_uuidlike(f.get("cloudId")) else f.get("cloudId")))

        # Best visible ID(s)
        license_id = first(
            f.get("appEntitlementNumber"),
            f.get("licenseId"),
            f.get("hostEntitlementNumber"),
            f.get("appEntitlementId"),
            f.get("hostEntitlementId"),
        )

        # Customer fallback chain: hostname → email domain → entitlement → short cloudId → Unknown
        cust = first(
            site,
            domain(email),
            license_id,
            (str(f.get("cloudId"))[:8] + "…" if f.get("cloudId") else None),
            "Unknown",
        )

        rows.append({
            "app": app_name,
            "appKey": app_key_expr,
            "customer": cust,
            "contactName": name,
            "contactEmail": email,
            "licenseType": ftype,
            "users": None,
            "licenseId": license_id,
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
    • customer · Name (email) · TYPE [· N users] [· E-...]
    
    ➖ Uninstalls / Unsubscribes
    • customer/site · Name (email) · TYPE [· E-...]
    """
    # Group by canonical key
    groups = {}
    for r in (licenses_rows or []):
        k = r.get("appKey") or r.get("app") or "unknown"
        g = groups.setdefault(k, {"names": set(), "lic": [], "un": []})
        if r.get("app"):
            g["names"].add(r["app"])
        g["lic"].append(r)

    for r in (uninstall_rows or []):
        k = r.get("appKey") or r.get("app") or "unknown"
        g = groups.setdefault(k, {"names": set(), "lic": [], "un": []})
        if r.get("app"):
            g["names"].add(r["app"])
        g["un"].append(r)

    if not groups:
        slack_post({"text": f"ℹ️ No new licenses or uninstalls for {start.isoformat()} (UTC)."})
        print("Nothing to post.")
        return

    def prettiest_name(names: set[str]) -> str:
        if not names:
            return "Unknown app"
        # prefer human-looking names (with spaces/colon)
        return sorted(names, key=lambda s: (":" not in s and " " not in s, len(s)))[0]

    date_label = start.isoformat()
    parts: list[str] = []

    for k in sorted(groups.keys()):
        g = groups[k]
        app_title = prettiest_name(g["names"])

        section_chunks: list[str] = []

        # New licenses section
        if g["lic"]:
            lines = []
            for e in g["lic"]:
                if e.get("contactName") and e.get("contactEmail"):
                    contact = f"{e['contactName']} ({e['contactEmail']})"
                else:
                    contact = e.get("contactName") or e.get("contactEmail") or "—"
                users_part = f" · {e['users']} users" if e.get("users") else ""
                id_part    = f" · {e['licenseId']}"   if e.get("licenseId") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{users_part}{id_part}")
            section_chunks.append(":airplane: New licenses\n" + "\n".join(lines))

        # Uninstalls section
        if g["un"]:
            lines = []
            for e in g["un"]:
                if e.get("contactName") and e.get("contactEmail"):
                    contact = f"{e['contactName']} ({e['contactEmail']})"
                else:
                    contact = e.get("contactName") or e.get("contactEmail") or "—"
                id_part = f" · {e['licenseId']}" if e.get("licenseId") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{id_part}")
            section_chunks.append(":heavy_minus_sign: Uninstalls / Unsubscribes\n" + "\n".join(lines))

        parts.append(f"{app_title} Marketplace Events ({date_label}, UTC)\n\n" + "\n\n".join(section_chunks))

    text = "\n\n".join(parts)
    slack_post({"text": text})
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
        # requests.post(SLACK_WEBHOOK, json={"text": f"❗ Script error: {e}"})
        raise

    if not lic_rows and not un_rows:
        msg = {"text": f"ℹ️ No new licenses or uninstalls for {start_date} (UTC)."}
        slack_post({"text": f"ℹ️ No new licenses or uninstalls for {start_date} (UTC)."})
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

