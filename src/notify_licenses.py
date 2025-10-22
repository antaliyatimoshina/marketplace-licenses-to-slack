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

CONVERSION_LOOKBACK_DAYS = int(os.getenv("CONVERSION_LOOKBACK_DAYS", "60"))

def _iso10(s):
    return (s or "")[:10] if isinstance(s, str) else None

def infer_conversions_from_licenses(lic_items, target: dt.date):
    """
    Heuristic conversion finder for day=target:
      - license is COMMERCIAL/PAID (not evaluation)
      - had a trial (latestEvaluationStartDate present)
      - license row was updated on the target date (lastUpdated == target)
    Returns a filtered subset of lic_items (raw dicts).
    """
    want = []
    tgt = target.isoformat()
    for lic in lic_items or []:
        lt = (lic.get("licenseType") or lic.get("tier") or "").upper()
        if lt not in ("COMMERCIAL", "PAID"):
            continue
        if not _iso10(lic.get("latestEvaluationStartDate")):
            continue
        if _iso10(lic.get("lastUpdated")) != tgt:
            continue
        want.append(lic)
    return want

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

def fetch_transactions(vendor_id: str, start: dt.date, end: dt.date):
    """
    Transactions async export:
      1) POST initiate
      2) poll status
      3) download JSON
    Returns a list of transaction dicts (or []).
    """
    import time, urllib.parse

    base = "https://marketplace.atlassian.com"
    # Try v2 then v4 (tenants differ)
    init_urls = [
        f"{base}/rest/2/vendors/{vendor_id}/reporting/transactions/async/export",
        f"{base}/rest/4/vendors/{vendor_id}/reporting/transactions/async/export",
    ]
    qparams = {
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "accept": "json",
        # UI often adds this; harmless if ignored:
        "include": "zeroTransactions",
    }
    headers = {"Accept": "application/json"}

    status_url = None
    last_err = None

    # 1) Initiate
    for init in init_urls:
        try:
            r = requests.post(init, params=qparams, headers=headers,
                              auth=(MP_USER, MP_API_TOKEN), timeout=60)
            if r.status_code == 404:
                last_err = f"404 on {r.url}"
                continue
            r.raise_for_status()
            data = r.json() if r.content else {}
            export_id = (
                data.get("exportId")
                or data.get("id")
                or (data.get("links") or {}).get("self", "").split("/")[-1]
            )
            status_url = (
                data.get("statusUrl")
                or (data.get("links") or {}).get("status")
                or (f"{init}/{urllib.parse.quote(str(export_id))}/status" if export_id else None)
            )
            if status_url:
                break
            last_err = f"unexpected initiate response on {init}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e} on {init}"
            continue

    if not status_url:
        print(f"[WARN] transactions initiate failed: {last_err}")
        return []

    # 2) Poll status (up to ~60s)
    deadline = time.time() + 60
    download_url = None
    while time.time() < deadline:
        rs = requests.get(status_url, headers=headers, auth=(MP_USER, MP_API_TOKEN), timeout=60)
        if rs.status_code == 404:
            time.sleep(2)
            continue
        rs.raise_for_status()
        sdata = rs.json() if rs.content else {}
        state = (sdata.get("state") or sdata.get("status") or "").lower()
        download_url = sdata.get("downloadUrl") or sdata.get("resultUrl")
        if state in ("completed", "complete", "done") and download_url:
            break
        if state in ("failed", "error"):
            print(f"[WARN] transactions export failed: {sdata}")
            return []
        time.sleep(2)

    if not download_url:
        print("[WARN] transactions export timed out without downloadUrl")
        return []

    # 3) Download JSON
    rd = requests.get(download_url, headers=headers, auth=(MP_USER, MP_API_TOKEN), timeout=120)
    rd.raise_for_status()
    try:
        payload = rd.json()
    except Exception:
        print("[WARN] transactions export is not JSON; first 200 chars:")
        print(rd.text[:200])
        return []

    # Normalize list
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("transactions"), list):
        return payload["transactions"]
    return []

def debug_dump_transactions(items, prefix="[TX]"):
    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    print(f"{prefix} total: {len(items)}")
    for i, t in enumerate(items[:50], 1):
        when = first(t.get("transactionDate"), t.get("date"), t.get("created"))
        if isinstance(when, str): when = when[:19]
        ent  = first(t.get("appEntitlementNumber"), t.get("entitlementNumber"))
        typ  = (first(t.get("transactionType"), t.get("eventType"), t.get("type")) or "").upper()
        lic  = (first(t.get("licenseType"), t.get("license")) or "").title()
        app  = first(t.get("addonName"), (t.get("app") or {}).get("name"), "Unknown app")
        cust = first((t.get("contactDetails") or {}).get("company"), t.get("customer"), t.get("accountName"), "—")
        users= first(t.get("users"), t.get("quantity"), t.get("seats"))
        amt  = first(t.get("amount"), t.get("price")); cur = first(t.get("currency"), t.get("currencyCode"))
        amt_s = f" · {amt} {cur}" if amt and cur else ""
        users_s = f" · {users} users" if users else ""
        print(f"{prefix} {i:02d} • {when} • {app} • {typ}/{lic}{users_s} • {cust} • {ent}{amt_s}")


def fetch_cloud_conversions(vendor_id: str, start: dt.date, end: dt.date):
    """
    Transactions for a date window (UTC).
    Tries export/base endpoints, first with include=zeroTransactions, then without.
    Normalizes to a list.
    """
    base = "https://marketplace.atlassian.com"
    endpoints = [
        f"{base}/rest/2/vendors/{vendor_id}/reporting/transactions/export",
        f"{base}/rest/2/vendors/{vendor_id}/reporting/transactions",
    ]
    # try with and without the include=zeroTransactions switch the UI uses
    param_variants = [
        {"startDate": start.isoformat(), "endDate": end.isoformat(), "accept": "json", "include": "zeroTransactions"},
        {"startDate": start.isoformat(), "endDate": end.isoformat(), "accept": "json"},
    ]
    headers = {"Accept": "application/json"}
    last_err = None

    for url in endpoints:
        for params in param_variants:
            try:
                r = requests.get(url, params=params, headers=headers,
                                 auth=(MP_USER, MP_API_TOKEN), timeout=60)
                # Some tenants return 404 on one variant but not the other
                if r.status_code == 404:
                    last_err = f"404 on {r.url}"
                    continue
                # 204/empty bodies → keep trying next variant
                if r.status_code == 204 or not r.content:
                    last_err = f"{r.status_code} no content on {r.url}"
                    continue

                r.raise_for_status()
                data = r.json()
                # API sometimes returns a list or {"transactions":[...]}
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    items = data.get("transactions")
                    if isinstance(items, list):
                        return items
                # fall through to try next variant
                last_err = f"unexpected JSON on {r.url}"
            except Exception as e:
                last_err = f"{type(e).__name__}: {e} on {url}"
                continue

    print(f"[WARN] fetch_transactions failed on all attempts: {last_err}")
    return []

def debug_dump_conversions(items, prefix="[CONV]"):
    """
    Print compact lines for cloud conversions so you can see what the API returns.
    """
    def first(*vals):
        for v in vals:
            if isinstance(v, str) and v.strip():
                return v.strip()
            if v not in (None, "", [], {}):
                return v
        return None

    print(f"{prefix} total: {len(items)}")
    for i, c in enumerate(items[:100], 1):
        when = first(c.get("conversionDate"), c.get("date"))
        if isinstance(when, str):
            when = when[:19]
        ent  = first(c.get("appEntitlementNumber"), c.get("entitlementNumber"))
        app  = first(c.get("addonName"), (c.get("app") or {}).get("name"), "Unknown app")
        key  = first(c.get("addonKey"), (c.get("app") or {}).get("key"))
        cust = first((c.get("contactDetails") or {}).get("company"),
                     c.get("customer"), c.get("accountName"),
                     c.get("cloudSiteHostname"), "—")
        users = first(c.get("users"), c.get("seats"), c.get("quantity"))
        users_s = f" · {users} users" if users else ""
        print(f"{prefix} {i:02d} • {when} • {app} • {cust} • {ent}{users_s} • key={key}")


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

CONVERSION_LOOKBACK_DAYS = int(os.getenv("CONVERSION_LOOKBACK_DAYS", "45"))

def _parse_date(s: str | None):
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        return None

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

        start_dt = _parse_date(
            lic.get("maintenanceStartDate")
            or lic.get("latestMaintenanceStartDate")
            or lic.get("evaluationStartDate")
        )
        trial_dt = _parse_date(lic.get("latestEvaluationStartDate"))
        
        is_paid = license_type not in ("EVALUATION", "EVAL", "TRIAL")
        is_conversion = (
            is_paid and trial_dt and start_dt and
            (start_dt - trial_dt).days >= 0 and
            (start_dt - trial_dt).days <= CONVERSION_LOOKBACK_DAYS
        )

        rows.append({
            "app": app_name,
            "appKey": app_key_expr,
            "customer": customer,
            "contactName": contact_name,
            "contactEmail": contact_email,
            "licenseType": license_type,
            "users": users,
            "licenseId": license_id,
            "isConversion": bool(is_conversion),
            "trialStarted": trial_dt.isoformat() if trial_dt else None,
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
    
        # app-scoped rows
        lic_rows = g["lic"]
        un_rows  = g["un"]
    
        # 1) split licenses into conversions vs non-conversions
        paid_conversions = [e for e in lic_rows if e.get("isConversion")]
        new_nonconversion = [e for e in lic_rows if not e.get("isConversion")]
    
        # (optional) same-day reinstall marker
        reinstalled_ids = {e["licenseId"] for e in lic_rows if e.get("licenseId")}
    
        # Conversions
        if paid_conversions:
            lines = []
            for e in paid_conversions:
                contact = (
                    f"{e['contactName']} ({e['contactEmail']})"
                    if e.get("contactName") and e.get("contactEmail")
                    else (e.get("contactName") or e.get("contactEmail") or "—")
                )
                users_part = f" · {e['users']} users" if e.get("users") else ""
                id_part    = f" · {e['licenseId']}" if e.get("licenseId") else ""
                trial_part = f" (trial started {e['trialStarted']})" if e.get("trialStarted") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{users_part}{id_part}{trial_part}")
            section_chunks.append(":moneybag: Conversions (trial → paid)\n" + "\n".join(lines))
    
        # New licenses (non-conversions)
        if new_nonconversion:
            lines = []
            for e in new_nonconversion:
                contact = (
                    f"{e['contactName']} ({e['contactEmail']})"
                    if e.get("contactName") and e.get("contactEmail")
                    else (e.get("contactName") or e.get("contactEmail") or "—")
                )
                users_part = f" · {e['users']} users" if e.get("users") else ""
                id_part    = f" · {e['licenseId']}" if e.get("licenseId") else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{users_part}{id_part}")
            section_chunks.append(":airplane: New licenses\n" + "\n".join(lines))
    
        # Uninstalls / Unsubscribes (keep your existing loop, but you can add same-day reinstall flag)
        if un_rows:
            lines = []
            for e in un_rows:
                contact = (
                    f"{e['contactName']} ({e['contactEmail']})"
                    if e.get("contactName") and e.get("contactEmail")
                    else (e.get("contactName") or e.get("contactEmail") or "—")
                )
                id_part = f" · {e['licenseId']}" if e.get("licenseId") else ""
                reinst_part = " (same-day reinstall)" if e.get("licenseId") in reinstalled_ids else ""
                lines.append(f"• {e['customer']} · {contact} · {e['licenseType']}{id_part}{reinst_part}")
            section_chunks.append(":heavy_minus_sign: Uninstalls / Unsubscribes\n" + "\n".join(lines))

    parts.append(f"{app_title} Marketplace Events ({date_label}, UTC)\n\n" + "\n\n".join(section_chunks))

    text = "\n\n".join(parts)
    slack_post({"text": text})
    print("Posted combined message (merged by appKey).")

def main():
    start_date, end_date = day_window_utc()
    print(f"[INFO] Daily window (UTC): {start_date}")

    # 2a) Wide fetch for conversion inference (uses lastUpdated on the target date)
    wide_start = start_date - dt.timedelta(days=CONVERSION_LOOKBACK_DAYS)
    lic_items_wide = fetch_licenses(VENDOR_ID, wide_start, end_date)   # existing function

    inferred_raw = infer_conversions_from_licenses(lic_items_wide, start_date)
    conv_rows = pick_new_evaluations(inferred_raw, start_date, end_date)  # reuse your mapper
    # mark as conversions + carry trial start date if present
    # build a quick index by licenseId so we can annotate trialStarted:
    raw_by_ent = {}
    for lic in inferred_raw:
        ent = lic.get("appEntitlementNumber") or lic.get("hostEntitlementNumber")
        if ent:
            raw_by_ent[ent] = lic
    for r in conv_rows:
        r["isConversion"] = True
        ent = r.get("licenseId")
        trial_dt = _iso10(raw_by_ent.get(ent, {}).get("latestEvaluationStartDate")) if ent else None
        if trial_dt:
            r["trialStarted"] = trial_dt

    # 2b) Normal single-day license rows (new starts etc.)
    lic_items = fetch_licenses(VENDOR_ID, start_date, end_date)
    lic_rows  = pick_new_evaluations(lic_items, start_date, end_date)

    # 2c) Uninstalls (your existing path)
    un_items = fetch_uninstalls(VENDOR_ID, start_date, end_date)
    name_map = build_app_name_map(lic_items, un_items)
    un_rows  = pick_uninstalls(un_items, name_map=name_map)

    # 2d) Merge conversions + de-dupe by licenseId so they don’t also appear under New licenses
    seen_ids = {r.get("licenseId") for r in conv_rows if r.get("licenseId")}
    lic_rows_filtered = [r for r in lic_rows if r.get("licenseId") not in seen_ids]
    lic_rows_final = conv_rows + lic_rows_filtered

    print(f"[INFO] Licenses mapped: {len(lic_rows_final)} | Conversions inferred: {len(conv_rows)} | Uninstalls mapped: {len(un_rows)}")

    if not lic_rows_final and not un_rows:
        slack_post({"text": f"ℹ️ No new licenses or uninstalls for {start_date} (UTC)."})
        print("[INFO] No items; posted 'no changes' message to Slack.")
        return

    post_combined_to_slack(SLACK_WEBHOOK, lic_rows_final, un_rows, start_date, end_date)

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

